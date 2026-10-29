package object

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// The tests in this file run against a GCS backend:
//
//   - T4_GCS_TEST_BUCKET=<bucket>: an existing bucket, with Application Default
//     Credentials (or the emulator, if STORAGE_EMULATOR_HOST is also set).
//     Each test works under its own prefix and deletes it afterwards.
//   - STORAGE_EMULATOR_HOST=<host:port> alone: an emulator such as
//     fake-gcs-server; each test creates its own bucket.
//
// Without either they are skipped. TestGCSBackendEnforcesGenerationPreconditions
// checks the backend itself: the remaining tests are only meaningful against
// a backend that passes it.

type gcsTestBackend struct {
	client *storage.Client
	bucket string
	prefix string
}

// newGCSTestBackend returns a client and an empty bucket/prefix to test in,
// or skips the test when no GCS backend is configured.
func newGCSTestBackend(t *testing.T) *gcsTestBackend {
	t.Helper()
	bucket := os.Getenv("T4_GCS_TEST_BUCKET")
	emulator := os.Getenv("STORAGE_EMULATOR_HOST")
	if bucket == "" && emulator == "" {
		t.Skip("set T4_GCS_TEST_BUCKET (real bucket, ADC) or STORAGE_EMULATOR_HOST (emulator) to run GCS tests")
	}
	ctx := context.Background()
	opts, err := gcsClientOptions(GCSConfig{CredentialsFile: os.Getenv("T4_GCS_TEST_CREDENTIALS_FILE")})
	if err != nil {
		t.Fatal(err)
	}
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	b := &gcsTestBackend{client: client, bucket: bucket}
	if bucket == "" {
		b.bucket = fmt.Sprintf("t4-test-%d", time.Now().UnixNano())
		if err := client.Bucket(b.bucket).Create(ctx, "t4-test", &storage.BucketAttrs{VersioningEnabled: true}); err != nil {
			t.Fatalf("create bucket: %v", err)
		}
	} else {
		b.prefix = fmt.Sprintf("t4-test/%s-%d", strings.ReplaceAll(t.Name(), "/", "-"), time.Now().UnixNano())
	}
	t.Cleanup(func() { b.deleteAll(t) })
	return b
}

// store returns a GCSStore over the test bucket and prefix.
func (b *gcsTestBackend) store() *GCSStore {
	return NewGCSStore(b.client, b.bucket, b.prefix)
}

func (b *gcsTestBackend) deleteAll(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	q := &storage.Query{Prefix: b.prefix, Versions: true}
	it := b.client.Bucket(b.bucket).Objects(ctx, q)
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			t.Logf("cleanup: list: %v", err)
			return
		}
		_ = b.client.Bucket(b.bucket).Object(attrs.Name).Generation(attrs.Generation).Delete(ctx)
	}
	if b.prefix == "" {
		_ = b.client.Bucket(b.bucket).Delete(ctx)
	}
}

func (b *gcsTestBackend) object(key string) *storage.ObjectHandle {
	if b.prefix != "" {
		key = b.prefix + "/" + key
	}
	return b.client.Bucket(b.bucket).Object(key)
}

// rawWrite writes data with the plain client under cond (nil: none) and
// returns the new generation.
func (b *gcsTestBackend) rawWrite(ctx context.Context, key string, data []byte, cond *storage.Conditions) (int64, error) {
	o := b.object(key)
	if cond != nil {
		o = o.If(*cond)
	}
	w := o.NewWriter(ctx)
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return 0, err
	}
	if err := w.Close(); err != nil {
		return 0, err
	}
	return w.Attrs().Generation, nil
}

// TestGCSBackendEnforcesGenerationPreconditions proves that the backend the
// GCS tests run against enforces generation preconditions on writes, using
// the plain client. GCS's S3-compatible endpoint ignores If-None-Match and
// If-Match on PUT; an emulator that ignored ifGenerationMatch would make the
// precondition and race tests below pass or fail for the wrong reason.
func TestGCSBackendEnforcesGenerationPreconditions(t *testing.T) {
	b := newGCSTestBackend(t)
	ctx := context.Background()

	gen1, err := b.rawWrite(ctx, "k", []byte("v1"), &storage.Conditions{DoesNotExist: true})
	if err != nil {
		t.Fatalf("create absent object with DoesNotExist: %v", err)
	}
	if _, err := b.rawWrite(ctx, "k", []byte("v2"), &storage.Conditions{DoesNotExist: true}); !isGCSPreconditionFailed(err) {
		t.Fatalf("DoesNotExist on an existing object: got %v, want 412", err)
	}
	gen2, err := b.rawWrite(ctx, "k", []byte("v2"), &storage.Conditions{GenerationMatch: gen1})
	if err != nil {
		t.Fatalf("GenerationMatch on the current generation: %v", err)
	}
	if gen2 == gen1 {
		t.Fatalf("overwrite kept generation %d", gen1)
	}
	if _, err := b.rawWrite(ctx, "k", []byte("v3"), &storage.Conditions{GenerationMatch: gen1}); !isGCSPreconditionFailed(err) {
		t.Fatalf("GenerationMatch on a stale generation: got %v, want 412", err)
	}
	if _, err := b.rawWrite(ctx, "missing", []byte("v"), &storage.Conditions{GenerationMatch: gen2}); !isGCSPreconditionFailed(err) {
		t.Fatalf("GenerationMatch on an absent object: got %v, want 412", err)
	}
	r, err := b.object("k").NewReader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	if got, _ := io.ReadAll(r); string(got) != "v2" {
		t.Fatalf("object holds %q after rejected writes, want %q", got, "v2")
	}
	if r.Attrs.Generation != gen2 {
		t.Fatalf("read generation %d, want %d", r.Attrs.Generation, gen2)
	}

	// A resumable upload carries its precondition from the request that
	// starts it, but the object is created only when the upload is
	// finalized. The precondition must be checked then: here the key is
	// created after the upload's first chunk was accepted.
	const chunk = 256 << 10
	uploadCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	w := b.object("resumable").If(storage.Conditions{DoesNotExist: true}).NewWriter(uploadCtx)
	w.ChunkSize = chunk
	accepted := make(chan struct{})
	var once sync.Once
	w.ProgressFunc = func(n int64) {
		if n >= chunk {
			once.Do(func() { close(accepted) })
		}
	}
	if _, err := w.Write(make([]byte, chunk)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-accepted:
	case <-time.After(time.Minute):
		t.Fatal("first chunk of the resumable upload was never accepted")
	}
	if _, err := b.rawWrite(ctx, "resumable", []byte("winner"), &storage.Conditions{DoesNotExist: true}); err != nil {
		t.Fatalf("create the key while the upload is in flight: %v", err)
	}
	if _, err := w.Write([]byte("tail")); err != nil && !isGCSPreconditionFailed(err) {
		t.Fatal(err)
	}
	if err := w.Close(); !isGCSPreconditionFailed(err) {
		t.Fatalf("finalizing a DoesNotExist upload after the key was created: got %v, want 412", err)
	}
}

func readAllAndClose(t *testing.T, rc io.ReadCloser) string {
	t.Helper()
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(b)
}

func mustGet(t *testing.T, s Store, key string) string {
	t.Helper()
	rc, err := s.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get %q: %v", key, err)
	}
	return readAllAndClose(t, rc)
}

func mustGetETag(t *testing.T, s ConditionalStore, key string) (string, string) {
	t.Helper()
	res, err := s.GetETag(context.Background(), key)
	if err != nil {
		t.Fatalf("GetETag %q: %v", key, err)
	}
	return readAllAndClose(t, res.Body), res.ETag
}

// TestGCSStoreMethods covers every Store, ConditionalStore and VersionedStore
// method.
func TestGCSStoreMethods(t *testing.T) {
	b := newGCSTestBackend(t)
	s := b.store()
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		run  func(t *testing.T)
	}{
		{"PutGet", func(t *testing.T) {
			if err := s.Put(ctx, "put/a", strings.NewReader("hello")); err != nil {
				t.Fatal(err)
			}
			if got := mustGet(t, s, "put/a"); got != "hello" {
				t.Fatalf("got %q", got)
			}
		}},
		{"PutOverwrites", func(t *testing.T) {
			for _, v := range []string{"old", "new"} {
				if err := s.Put(ctx, "put/b", strings.NewReader(v)); err != nil {
					t.Fatal(err)
				}
			}
			if got := mustGet(t, s, "put/b"); got != "new" {
				t.Fatalf("got %q", got)
			}
		}},
		{"PutEmpty", func(t *testing.T) {
			if err := s.Put(ctx, "put/empty", bytes.NewReader(nil)); err != nil {
				t.Fatal(err)
			}
			if got := mustGet(t, s, "put/empty"); got != "" {
				t.Fatalf("got %q", got)
			}
		}},
		{"PutUnknownSize", func(t *testing.T) {
			// A reader of unknown size, like the encrypted store's pipe.
			pr, pw := io.Pipe()
			go func() { _, _ = io.WriteString(pw, "streamed"); _ = pw.Close() }()
			if err := s.Put(ctx, "put/stream", pr); err != nil {
				t.Fatal(err)
			}
			if got := mustGet(t, s, "put/stream"); got != "streamed" {
				t.Fatalf("got %q", got)
			}
		}},
		{"PutReaderErrorLeavesNoObject", func(t *testing.T) {
			pr, pw := io.Pipe()
			boom := errors.New("boom")
			go func() { _, _ = io.WriteString(pw, "partial"); _ = pw.CloseWithError(boom) }()
			if err := s.Put(ctx, "put/broken", pr); !errors.Is(err, boom) {
				t.Fatalf("Put: got %v, want %v", err, boom)
			}
			if _, err := s.Get(ctx, "put/broken"); err != ErrNotFound {
				t.Fatalf("Get after failed Put: got %v, want ErrNotFound", err)
			}
		}},
		{"PutLarge", func(t *testing.T) {
			// Larger than one chunk: a resumable upload.
			data := make([]byte, gcsMaxChunkSize+3<<20)
			_, _ = rand.Read(data)
			if err := s.Put(ctx, "put/large", bytes.NewReader(data)); err != nil {
				t.Fatal(err)
			}
			if got := mustGet(t, s, "put/large"); got != string(data) {
				t.Fatalf("large object differs (%d bytes read, %d written)", len(got), len(data))
			}
		}},
		{"PutFromFile", func(t *testing.T) {
			f, err := os.CreateTemp(t.TempDir(), "seg")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = f.Close() }()
			if _, err := f.WriteString("header+segment"); err != nil {
				t.Fatal(err)
			}
			if _, err := f.Seek(7, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			if err := s.Put(ctx, "put/file", f); err != nil {
				t.Fatal(err)
			}
			if got := mustGet(t, s, "put/file"); got != "segment" {
				t.Fatalf("got %q", got)
			}
		}},
		{"GetMissing", func(t *testing.T) {
			if _, err := s.Get(ctx, "missing"); err != ErrNotFound {
				t.Fatalf("got %v, want ErrNotFound", err)
			}
		}},
		{"Delete", func(t *testing.T) {
			if err := s.Put(ctx, "del/a", strings.NewReader("x")); err != nil {
				t.Fatal(err)
			}
			if err := s.Delete(ctx, "del/a"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Get(ctx, "del/a"); err != ErrNotFound {
				t.Fatalf("Get after Delete: got %v, want ErrNotFound", err)
			}
			if err := s.Delete(ctx, "del/a"); err != nil {
				t.Fatalf("Delete of an absent key: %v", err)
			}
		}},
		{"DeleteMany", func(t *testing.T) {
			var keys []string
			for i := range 40 {
				k := fmt.Sprintf("many/%02d", i)
				if err := s.Put(ctx, k, strings.NewReader(k)); err != nil {
					t.Fatal(err)
				}
				keys = append(keys, k)
			}
			keys = append(keys, "many/absent")
			if err := s.DeleteMany(ctx, keys); err != nil {
				t.Fatal(err)
			}
			if got, err := s.List(ctx, "many/"); err != nil || len(got) != 0 {
				t.Fatalf("List after DeleteMany: %v, %v", got, err)
			}
			if err := s.DeleteMany(ctx, nil); err != nil {
				t.Fatalf("DeleteMany(nil): %v", err)
			}
		}},
		{"List", func(t *testing.T) {
			want := []string{"list/a", "list/b/c", "list/b/d", "list/z"}
			for _, k := range []string{"list/z", "list/b/d", "list/a", "list/b/c", "listx", "other/list/a"} {
				if err := s.Put(ctx, k, strings.NewReader(k)); err != nil {
					t.Fatal(err)
				}
			}
			got, err := s.List(ctx, "list/")
			if err != nil {
				t.Fatal(err)
			}
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("List(list/) = %v, want %v", got, want)
			}
			if got, err := s.List(ctx, "nothing/"); err != nil || len(got) != 0 {
				t.Fatalf("List of an empty prefix = %v, %v", got, err)
			}
		}},
		{"ListPaginates", func(t *testing.T) {
			// More keys than one page of results (1000).
			const n = 1100
			sem := make(chan struct{}, 32)
			var wg sync.WaitGroup
			errs := make(chan error, n)
			for i := range n {
				sem <- struct{}{}
				wg.Add(1)
				go func() {
					defer func() { <-sem; wg.Done() }()
					errs <- s.Put(ctx, fmt.Sprintf("page/%04d", i), strings.NewReader("x"))
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}
			got, err := s.List(ctx, "page/")
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != n || got[0] != "page/0000" || got[n-1] != fmt.Sprintf("page/%04d", n-1) {
				t.Fatalf("List returned %d keys (first %q), want %d", len(got), got[0], n)
			}
		}},
		{"GetETagIsGeneration", func(t *testing.T) {
			if err := s.Put(ctx, "etag/a", strings.NewReader("v1")); err != nil {
				t.Fatal(err)
			}
			body, etag := mustGetETag(t, s, "etag/a")
			attrs, err := b.object("etag/a").Attrs(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if body != "v1" || etag != strconv.FormatInt(attrs.Generation, 10) {
				t.Fatalf("GetETag = %q, %q; want %q, %d", body, etag, "v1", attrs.Generation)
			}
			if _, err := s.GetETag(ctx, "etag/missing"); err != ErrNotFound {
				t.Fatalf("GetETag of an absent key: got %v, want ErrNotFound", err)
			}
		}},
		{"GetVersioned", func(t *testing.T) {
			// roles/storage.objectUser cannot read bucket metadata; without
			// it, noncurrent generations are not checked.
			attrs, err := b.client.Bucket(b.bucket).Attrs(ctx)
			versioned := err == nil && attrs.VersioningEnabled
			if err != nil {
				t.Logf("bucket attributes: %v", err)
			}
			if err := s.Put(ctx, "ver/a", strings.NewReader("v1")); err != nil {
				t.Fatal(err)
			}
			_, gen1 := mustGetETag(t, s, "ver/a")
			if err := s.Put(ctx, "ver/a", strings.NewReader("v2")); err != nil {
				t.Fatal(err)
			}
			_, gen2 := mustGetETag(t, s, "ver/a")
			rc, err := s.GetVersioned(ctx, "ver/a", gen2)
			if err != nil {
				t.Fatal(err)
			}
			if got := readAllAndClose(t, rc); got != "v2" {
				t.Fatalf("live generation holds %q", got)
			}
			if versioned {
				rc, err := s.GetVersioned(ctx, "ver/a", gen1)
				if err != nil {
					t.Fatalf("noncurrent generation: %v", err)
				}
				if got := readAllAndClose(t, rc); got != "v1" {
					t.Fatalf("noncurrent generation holds %q", got)
				}
			} else {
				t.Log("bucket has no Object Versioning (or it cannot be read); noncurrent generation not checked")
			}
			for _, v := range []string{"", "0", "-1", "not-a-generation", "999"} {
				if _, err := s.GetVersioned(ctx, "ver/a", v); err != ErrNotFound {
					t.Fatalf("GetVersioned(%q): got %v, want ErrNotFound", v, err)
				}
			}
		}},
	} {
		t.Run(tc.name, tc.run)
	}
}

// TestGCSStorePrefix checks that keys are stored under the prefix and that
// List strips it.
func TestGCSStorePrefix(t *testing.T) {
	b := newGCSTestBackend(t)
	ctx := context.Background()
	prefix := strings.TrimPrefix(b.prefix+"/nested/p", "/")
	s := NewGCSStore(b.client, b.bucket, "/"+prefix+"/")
	if err := s.Put(ctx, "wal/1", strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.client.Bucket(b.bucket).Object(prefix + "/wal/1").Attrs(ctx); err != nil {
		t.Fatalf("object not stored under the prefix: %v", err)
	}
	got, err := s.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "wal/1" {
		t.Fatalf("List = %v, want [wal/1]", got)
	}
}

// TestGCSStorePreconditions covers the conditional-write cases leader election
// depends on.
func TestGCSStorePreconditions(t *testing.T) {
	b := newGCSTestBackend(t)
	s := b.store()
	ctx := context.Background()

	t.Run("CreateIfAbsentOnExisting", func(t *testing.T) {
		if err := s.PutIfAbsent(ctx, "lock-a", strings.NewReader("first")); err != nil {
			t.Fatalf("PutIfAbsent on an absent key: %v", err)
		}
		if err := s.PutIfAbsent(ctx, "lock-a", strings.NewReader("second")); err != ErrPreconditionFailed {
			t.Fatalf("PutIfAbsent on an existing key: got %v, want ErrPreconditionFailed", err)
		}
		if got := mustGet(t, s, "lock-a"); got != "first" {
			t.Fatalf("object holds %q", got)
		}
	})
	t.Run("UpdateWithStaleGeneration", func(t *testing.T) {
		if err := s.Put(ctx, "lock-b", strings.NewReader("v1")); err != nil {
			t.Fatal(err)
		}
		_, stale := mustGetETag(t, s, "lock-b")
		if err := s.Put(ctx, "lock-b", strings.NewReader("v2")); err != nil {
			t.Fatal(err)
		}
		if err := s.PutIfMatch(ctx, "lock-b", strings.NewReader("v3"), stale); err != ErrPreconditionFailed {
			t.Fatalf("PutIfMatch with a stale generation: got %v, want ErrPreconditionFailed", err)
		}
		if got := mustGet(t, s, "lock-b"); got != "v2" {
			t.Fatalf("object holds %q", got)
		}
	})
	t.Run("UpdateWithCurrentGeneration", func(t *testing.T) {
		if err := s.PutIfAbsent(ctx, "lock-c", strings.NewReader("v1")); err != nil {
			t.Fatal(err)
		}
		_, cur := mustGetETag(t, s, "lock-c")
		if err := s.PutIfMatch(ctx, "lock-c", strings.NewReader("v2"), cur); err != nil {
			t.Fatalf("PutIfMatch with the current generation: %v", err)
		}
		body, next := mustGetETag(t, s, "lock-c")
		if body != "v2" || next == cur || next == "" {
			t.Fatalf("after PutIfMatch: body %q, generation %q (was %q)", body, next, cur)
		}
		// The new generation is usable for the next update.
		if err := s.PutIfMatch(ctx, "lock-c", strings.NewReader("v3"), next); err != nil {
			t.Fatalf("PutIfMatch with the new generation: %v", err)
		}
	})
	t.Run("UpdateAbsent", func(t *testing.T) {
		if err := s.Put(ctx, "lock-d", strings.NewReader("v1")); err != nil {
			t.Fatal(err)
		}
		_, gen := mustGetETag(t, s, "lock-d")
		if err := s.Delete(ctx, "lock-d"); err != nil {
			t.Fatal(err)
		}
		if err := s.PutIfMatch(ctx, "lock-d", strings.NewReader("v2"), gen); err != ErrPreconditionFailed {
			t.Fatalf("PutIfMatch on a deleted key: got %v, want ErrPreconditionFailed", err)
		}
	})
	t.Run("InvalidETag", func(t *testing.T) {
		if err := s.Put(ctx, "lock-e", strings.NewReader("v1")); err != nil {
			t.Fatal(err)
		}
		for _, etag := range []string{"", "0", "-5", `"abc"`, "d41d8cd98f00b204e9800998ecf8427e"} {
			if err := s.PutIfMatch(ctx, "lock-e", strings.NewReader("v2"), etag); err != ErrPreconditionFailed {
				t.Fatalf("PutIfMatch(%q): got %v, want ErrPreconditionFailed", etag, err)
			}
		}
		if got := mustGet(t, s, "lock-e"); got != "v1" {
			t.Fatalf("object holds %q", got)
		}
	})
	t.Run("ResumableUploadChecksAtFinalize", func(t *testing.T) {
		// A conditional upload larger than one chunk is resumable. The
		// object is created only when the upload is finalized, so the
		// precondition must hold then: another writer that creates the key
		// while the upload is in flight wins, and the upload fails.
		gate := &gatedReader{reached: make(chan struct{}), release: make(chan struct{}), r: strings.NewReader("tail")}
		body := io.MultiReader(bytes.NewReader(make([]byte, gcsMaxChunkSize)), gate)
		done := make(chan error, 1)
		go func() { done <- s.PutIfAbsent(ctx, "lock-f", body) }()
		// The first chunk is buffered once the upload reads past it; give
		// it time to be sent.
		<-gate.reached
		time.Sleep(time.Second)
		if err := s.PutIfAbsent(ctx, "lock-f", strings.NewReader("winner")); err != nil {
			t.Fatalf("competing PutIfAbsent: %v", err)
		}
		close(gate.release)
		if err := <-done; err != ErrPreconditionFailed {
			t.Fatalf("resumable PutIfAbsent finalized after the key was created: got %v, want ErrPreconditionFailed", err)
		}
		if got := mustGet(t, s, "lock-f"); got != "winner" {
			t.Fatalf("object holds %d bytes, want the competing write", len(got))
		}
	})
}

// gatedReader closes reached on its first Read and blocks until release is
// closed.
type gatedReader struct {
	once    sync.Once
	reached chan struct{}
	release chan struct{}
	r       io.Reader
}

func (g *gatedReader) Read(p []byte) (int, error) {
	g.once.Do(func() { close(g.reached) })
	<-g.release
	return g.r.Read(p)
}

// TestGCSStoreConditionalRace runs N concurrent conditional writers on one key,
// repeatedly: exactly one must win each round, and the object must hold its
// write.
func TestGCSStoreConditionalRace(t *testing.T) {
	const (
		writers = 24
		rounds  = 10
	)
	b := newGCSTestBackend(t)
	s := b.store()
	ctx := context.Background()

	race := func(t *testing.T, key string, write func(i int) error) {
		t.Helper()
		var (
			wg      sync.WaitGroup
			start   = make(chan struct{})
			results = make([]error, writers)
		)
		for i := range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				results[i] = write(i)
			}()
		}
		close(start)
		wg.Wait()
		winner := -1
		for i, err := range results {
			switch {
			case err == nil:
				if winner >= 0 {
					t.Fatalf("writers %d and %d both won", winner, i)
				}
				winner = i
			case err != ErrPreconditionFailed:
				t.Fatalf("writer %d: %v", i, err)
			}
		}
		if winner < 0 {
			t.Fatal("no writer won")
		}
		if got := mustGet(t, s, key); got != fmt.Sprintf("writer-%d", winner) {
			t.Fatalf("object holds %q, winner was writer-%d", got, winner)
		}
	}

	t.Run("PutIfAbsent", func(t *testing.T) {
		for round := range rounds {
			key := fmt.Sprintf("race/absent-%d", round)
			race(t, key, func(i int) error {
				return s.PutIfAbsent(ctx, key, strings.NewReader(fmt.Sprintf("writer-%d", i)))
			})
		}
	})
	t.Run("PutIfMatch", func(t *testing.T) {
		key := "race/match"
		if err := s.Put(ctx, key, strings.NewReader("initial")); err != nil {
			t.Fatal(err)
		}
		for range rounds {
			_, etag := mustGetETag(t, s, key)
			race(t, key, func(i int) error {
				return s.PutIfMatch(ctx, key, strings.NewReader(fmt.Sprintf("writer-%d", i)), etag)
			})
		}
	})
	t.Run("EncryptedPutIfAbsent", func(t *testing.T) {
		kp, err := NewStaticKeyProvider(bytes.Repeat([]byte{7}, 32))
		if err != nil {
			t.Fatal(err)
		}
		enc, ok := NewEncryptedStore(s, kp).(ConditionalStore)
		if !ok {
			t.Fatal("encrypted GCSStore is not a ConditionalStore")
		}
		key := "race/encrypted"
		var mu sync.Mutex
		wins := 0
		var wg sync.WaitGroup
		for i := range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := enc.PutIfAbsent(ctx, key, strings.NewReader(fmt.Sprintf("writer-%d", i)))
				if err != nil && err != ErrPreconditionFailed {
					t.Errorf("writer %d: %v", i, err)
				}
				if err == nil {
					mu.Lock()
					wins++
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		if wins != 1 {
			t.Fatalf("%d encrypted writers won, want 1", wins)
		}
	})
}
