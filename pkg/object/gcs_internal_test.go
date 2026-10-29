package object

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestGCSStorePrefixIgnoresSurroundingSlashes(t *testing.T) {
	for _, prefix := range []string{"t4", "t4/", "/t4", "/t4/", "t4//"} {
		s := &GCSStore{prefix: strings.Trim(prefix, "/")}
		if got := s.key("manifest/latest"); got != "t4/manifest/latest" {
			t.Errorf("prefix %q: key = %q, want %q", prefix, got, "t4/manifest/latest")
		}
	}
	for _, prefix := range []string{"", "/", "//"} {
		s := &GCSStore{prefix: strings.Trim(prefix, "/")}
		if got := s.key("manifest/latest"); got != "manifest/latest" {
			t.Errorf("prefix %q: key = %q, want %q", prefix, got, "manifest/latest")
		}
	}
}

func TestGCSChunkSize(t *testing.T) {
	for _, tc := range []struct {
		size int64
		want int
	}{
		{-1, gcsMaxChunkSize},
		{0, 1},
		{300, 301},
		{gcsMaxChunkSize - 1, gcsMaxChunkSize},
		{gcsMaxChunkSize, gcsMaxChunkSize},
		{50 << 20, gcsMaxChunkSize},
	} {
		if got := gcsChunkSize(tc.size); got != tc.want {
			t.Errorf("gcsChunkSize(%d) = %d, want %d", tc.size, got, tc.want)
		}
	}
}

func TestGCSTransportBoundsResponseWait(t *testing.T) {
	tr := gcsTransport()
	if tr.ResponseHeaderTimeout <= 0 || tr.ResponseHeaderTimeout > 15*time.Second {
		t.Fatalf("ResponseHeaderTimeout = %v, want at most 15s", tr.ResponseHeaderTimeout)
	}
	if tr.HTTP2 == nil || tr.HTTP2.SendPingTimeout <= 0 || tr.HTTP2.SendPingTimeout+tr.HTTP2.PingTimeout > 15*time.Second {
		t.Fatalf("HTTP/2 health check %+v, want a dead connection closed within 15s", tr.HTTP2)
	}
	if tr.Proxy == nil {
		t.Fatal("transport ignores proxy settings")
	}
}

func TestGCSConfigRequiresBucket(t *testing.T) {
	if _, err := NewGCSStoreFromConfig(t.Context(), GCSConfig{}); err == nil || !strings.Contains(err.Error(), "bucket") {
		t.Fatalf("expected missing bucket error, got %v", err)
	}
}

func TestGCSConfigReportsMissingDefaultCredentials(t *testing.T) {
	t.Setenv("STORAGE_EMULATOR_HOST", "")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "missing.json"))
	_, err := NewGCSStoreFromConfig(t.Context(), GCSConfig{Bucket: "b"})
	if err == nil || !strings.Contains(err.Error(), "T4_GCS_CREDENTIALS_FILE") {
		t.Fatalf("expected an application default credentials error, got %v", err)
	}
}

// writeServiceAccountFile writes a service account credentials file with a
// fresh key. Building a client from it makes no network calls.
func writeServiceAccountFile(t *testing.T, credType string) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(map[string]string{
		"type":           credType,
		"project_id":     "t4-test",
		"private_key_id": "0123",
		"private_key":    string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		"client_email":   "t4@t4-test.iam.gserviceaccount.com",
		"client_id":      "1",
		"token_uri":      "https://oauth2.googleapis.com/token",
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "creds.json")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestGCSConfigCredentialsFile(t *testing.T) {
	t.Setenv("STORAGE_EMULATOR_HOST", "")
	t.Run("ServiceAccount", func(t *testing.T) {
		s, err := NewGCSStoreFromConfig(t.Context(), GCSConfig{Bucket: "b", Prefix: "/p/", CredentialsFile: writeServiceAccountFile(t, "service_account")})
		if err != nil {
			t.Fatalf("NewGCSStoreFromConfig: %v", err)
		}
		defer func() { _ = s.Close() }()
		if got := s.key("leader-lock"); got != "p/leader-lock" {
			t.Fatalf("key = %q", got)
		}
	})
	t.Run("UnsupportedType", func(t *testing.T) {
		_, err := NewGCSStoreFromConfig(t.Context(), GCSConfig{Bucket: "b", CredentialsFile: writeServiceAccountFile(t, "gdch_service_account")})
		if err == nil || !strings.Contains(err.Error(), `type "gdch_service_account"`) {
			t.Fatalf("expected unsupported type error, got %v", err)
		}
	})
	t.Run("NotJSON", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "creds.json")
		if err := os.WriteFile(path, []byte("not json"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := NewGCSStoreFromConfig(t.Context(), GCSConfig{Bucket: "b", CredentialsFile: path}); err == nil || !strings.Contains(err.Error(), "not JSON") {
			t.Fatalf("expected parse error, got %v", err)
		}
	})
	t.Run("Missing", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "missing.json")
		if _, err := NewGCSStoreFromConfig(t.Context(), GCSConfig{Bucket: "b", CredentialsFile: path}); err == nil || !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected not-exist error, got %v", err)
		}
	})
}

// fakeGCS is a minimal GCS JSON API for one bucket: single-request
// (multipart) uploads with ifGenerationMatch, object metadata reads, and
// object downloads. Hooks inject the faults the tests need.
type fakeGCS struct {
	mu      sync.Mutex
	objects map[string]fakeGCSObject
	gen     int64
	uploads int

	// dropUploads is how many upload responses to drop after the write was
	// applied, as if the connection broke before the response arrived.
	dropUploads int
	// afterDrop, if set, runs (with mu held) after a dropped upload.
	afterDrop func(f *fakeGCS)
	// stallUploads is how many uploads to leave unanswered, without
	// applying them.
	stallUploads int
	stall        chan struct{}
	// attrsStatus, if set, is the status of every metadata read.
	attrsStatus int
}

type fakeGCSObject struct {
	data     []byte
	gen      int64
	metadata map[string]string
}

func newFakeGCS(t *testing.T) (*fakeGCS, *GCSStore) {
	t.Helper()
	f := &fakeGCS{objects: map[string]fakeGCSObject{}, stall: make(chan struct{})}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(f.stall) })
	t.Setenv("STORAGE_EMULATOR_HOST", strings.TrimPrefix(srv.URL, "http://"))
	s, err := NewGCSStoreFromConfig(t.Context(), GCSConfig{Bucket: "bucket"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return f, s
}

// data returns the content of name and the number of uploads so far.
func (f *fakeGCS) data(name string) (string, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return string(f.objects[name].data), f.uploads
}

// put stores data as a write by another client.
func (f *fakeGCS) put(name string, data []byte) {
	f.gen++
	f.objects[name] = fakeGCSObject{data: data, gen: f.gen}
}

func (f *fakeGCS) resource(name string, o fakeGCSObject) map[string]any {
	crc := make([]byte, 4)
	binary.BigEndian.PutUint32(crc, crc32.Checksum(o.data, crc32.MakeTable(crc32.Castagnoli)))
	return map[string]any{
		"kind":       "storage#object",
		"bucket":     "bucket",
		"name":       name,
		"generation": strconv.FormatInt(o.gen, 10),
		"size":       strconv.Itoa(len(o.data)),
		"crc32c":     base64.StdEncoding.EncodeToString(crc),
		"metadata":   o.metadata,
	}
}

func writeGCSError(w http.ResponseWriter, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = fmt.Fprintf(w, `{"error":{"code":%d,"message":%q}}`, code, http.StatusText(code))
}

func (f *fakeGCS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/upload/storage/v1/b/bucket/o":
		f.upload(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/storage/v1/b/bucket/o/"):
		f.attrs(w, strings.TrimPrefix(r.URL.Path, "/storage/v1/b/bucket/o/"))
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/bucket/"):
		f.download(w, strings.TrimPrefix(r.URL.Path, "/bucket/"))
	default:
		writeGCSError(w, http.StatusNotImplemented)
	}
}

func (f *fakeGCS) upload(w http.ResponseWriter, r *http.Request) {
	_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || r.URL.Query().Get("uploadType") != "multipart" {
		writeGCSError(w, http.StatusBadRequest)
		return
	}
	mr := multipart.NewReader(r.Body, params["boundary"])
	var meta struct {
		Name     string            `json:"name"`
		Metadata map[string]string `json:"metadata"`
	}
	part, err := mr.NextPart()
	if err == nil {
		err = json.NewDecoder(part).Decode(&meta)
	}
	var data []byte
	if err == nil {
		if part, err = mr.NextPart(); err == nil {
			data, err = io.ReadAll(part)
		}
	}
	if err != nil {
		writeGCSError(w, http.StatusBadRequest)
		return
	}

	f.mu.Lock()
	f.uploads++
	if f.stallUploads > 0 {
		f.stallUploads--
		f.mu.Unlock()
		<-f.stall
		return
	}
	cur := f.objects[meta.Name]
	if m := r.URL.Query().Get("ifGenerationMatch"); m != "" {
		if want, _ := strconv.ParseInt(m, 10, 64); want != cur.gen {
			f.mu.Unlock()
			writeGCSError(w, http.StatusPreconditionFailed)
			return
		}
	}
	f.gen++
	obj := fakeGCSObject{data: data, gen: f.gen, metadata: meta.Metadata}
	f.objects[meta.Name] = obj
	if f.dropUploads > 0 {
		f.dropUploads--
		if f.afterDrop != nil {
			f.afterDrop(f)
		}
		f.mu.Unlock()
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
		return
	}
	res := f.resource(meta.Name, obj)
	f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(res)
}

func (f *fakeGCS) attrs(w http.ResponseWriter, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.attrsStatus != 0 {
		writeGCSError(w, f.attrsStatus)
		return
	}
	o, ok := f.objects[name]
	if !ok {
		writeGCSError(w, http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(f.resource(name, o))
}

func (f *fakeGCS) download(w http.ResponseWriter, name string) {
	f.mu.Lock()
	o, ok := f.objects[name]
	f.mu.Unlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("X-Goog-Generation", strconv.FormatInt(o.gen, 10))
	w.Header().Set("Content-Length", strconv.Itoa(len(o.data)))
	_, _ = w.Write(o.data)
}

// TestGCSConditionalWriteWhoseResponseWasLost: the client retries a
// conditional write whose first attempt landed but whose response was lost.
// The retry fails with 412; the write must still be reported as successful,
// or leader election would conclude that it lost an election it won.
func TestGCSConditionalWriteWhoseResponseWasLost(t *testing.T) {
	t.Run("PutIfAbsent", func(t *testing.T) {
		f, s := newFakeGCS(t)
		f.dropUploads = 1
		if err := s.PutIfAbsent(t.Context(), "leader-lock", strings.NewReader("mine")); err != nil {
			t.Fatalf("PutIfAbsent: %v", err)
		}
		if got, uploads := f.data("leader-lock"); got != "mine" || uploads != 2 {
			t.Fatalf("object holds %q after %d uploads, want %q after the dropped one and its retry", got, uploads, "mine")
		}
	})
	t.Run("PutIfMatch", func(t *testing.T) {
		f, s := newFakeGCS(t)
		f.put("leader-lock", []byte("old"))
		gen := strconv.FormatInt(f.gen, 10)
		f.dropUploads = 1
		if err := s.PutIfMatch(t.Context(), "leader-lock", strings.NewReader("mine"), gen); err != nil {
			t.Fatalf("PutIfMatch: %v", err)
		}
		if got, uploads := f.data("leader-lock"); got != "mine" || uploads != 2 {
			t.Fatalf("object holds %q after %d uploads, want %q after the dropped one and its retry", got, uploads, "mine")
		}
	})
	t.Run("OverwrittenBeforeRetry", func(t *testing.T) {
		// The write landed, then another node overwrote it before the
		// retry: by now the write has lost, which is what the caller's
		// next read would show.
		f, s := newFakeGCS(t)
		f.dropUploads = 1
		f.afterDrop = func(f *fakeGCS) { f.put("leader-lock", []byte("theirs")) }
		if err := s.PutIfAbsent(t.Context(), "leader-lock", strings.NewReader("mine")); err != ErrPreconditionFailed {
			t.Fatalf("PutIfAbsent: got %v, want ErrPreconditionFailed", err)
		}
	})
}

func TestGCSConditionalWriteConflict(t *testing.T) {
	t.Run("PutIfAbsent", func(t *testing.T) {
		f, s := newFakeGCS(t)
		f.put("leader-lock", []byte("theirs"))
		if err := s.PutIfAbsent(t.Context(), "leader-lock", strings.NewReader("mine")); err != ErrPreconditionFailed {
			t.Fatalf("got %v, want ErrPreconditionFailed", err)
		}
		if got, _ := f.data("leader-lock"); got != "theirs" {
			t.Fatalf("object holds %q", got)
		}
	})
	t.Run("PutIfMatchStale", func(t *testing.T) {
		f, s := newFakeGCS(t)
		f.put("leader-lock", []byte("v1"))
		stale := strconv.FormatInt(f.gen, 10)
		f.put("leader-lock", []byte("v2"))
		if err := s.PutIfMatch(t.Context(), "leader-lock", strings.NewReader("mine"), stale); err != ErrPreconditionFailed {
			t.Fatalf("got %v, want ErrPreconditionFailed", err)
		}
	})
	t.Run("PutIfMatchInvalidETag", func(t *testing.T) {
		f, s := newFakeGCS(t)
		for _, etag := range []string{"", "0", "-1", `"0123abcd"`} {
			if err := s.PutIfMatch(t.Context(), "leader-lock", strings.NewReader("mine"), etag); err != ErrPreconditionFailed {
				t.Fatalf("PutIfMatch(%q): got %v, want ErrPreconditionFailed", etag, err)
			}
		}
		if _, uploads := f.data("leader-lock"); uploads != 0 {
			t.Fatalf("%d uploads for invalid ETags, want none", uploads)
		}
	})
	t.Run("OutcomeUnknown", func(t *testing.T) {
		// 412, and whether this write landed cannot be checked: the caller
		// must not conclude it lost.
		f, s := newFakeGCS(t)
		f.put("leader-lock", []byte("theirs"))
		f.attrsStatus = http.StatusForbidden
		err := s.PutIfAbsent(t.Context(), "leader-lock", strings.NewReader("mine"))
		if err == nil || errors.Is(err, ErrPreconditionFailed) {
			t.Fatalf("got %v, want an error other than ErrPreconditionFailed", err)
		}
	})
}

// TestGCSRetriesRequestWithNoAnswer checks that a write GCS never answers is
// retried once gcsResponseHeaderTimeout passes, rather than waiting longer.
func TestGCSRetriesRequestWithNoAnswer(t *testing.T) {
	defer func(d time.Duration) { gcsResponseHeaderTimeout = d }(gcsResponseHeaderTimeout)
	gcsResponseHeaderTimeout = 200 * time.Millisecond

	for name, put := range map[string]func(*GCSStore) error{
		"Put": func(s *GCSStore) error { return s.Put(t.Context(), "key", strings.NewReader("value")) },
		"PutIfAbsent": func(s *GCSStore) error {
			return s.PutIfAbsent(t.Context(), "key", strings.NewReader("value"))
		},
	} {
		t.Run(name, func(t *testing.T) {
			f, s := newFakeGCS(t)
			f.stallUploads = 1
			start := time.Now()
			if err := put(s); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if took := time.Since(start); took > 10*time.Second {
				t.Fatalf("%s took %v", name, took)
			}
			if got, _ := f.data("key"); got != "value" {
				t.Fatalf("object holds %q", got)
			}
		})
	}
}

func TestGCSGetETagUsesGeneration(t *testing.T) {
	f, s := newFakeGCS(t)
	if _, err := s.GetETag(t.Context(), "leader-lock"); err != ErrNotFound {
		t.Fatalf("GetETag of an absent key: got %v, want ErrNotFound", err)
	}
	f.put("leader-lock", []byte("v1"))
	res, err := s.GetETag(t.Context(), "leader-lock")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	if want := strconv.FormatInt(f.gen, 10); res.ETag != want {
		t.Fatalf("ETag = %q, want generation %q", res.ETag, want)
	}
}
