package object

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials"
	"cloud.google.com/go/auth/httptransport"
	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	// gcsMaxChunkSize is the largest buffer a GCS upload holds, and the size
	// of each request of a resumable upload. It is the client's default.
	gcsMaxChunkSize = 16 << 20

	// gcsWriteIDKey is the custom metadata key carrying the random ID of a
	// conditional write, so that a write whose response was lost can be
	// recognized when its retry fails with 412.
	gcsWriteIDKey = "t4-write-id"

	// gcsDeleteConcurrency bounds the parallel deletes DeleteMany issues: the
	// Go client has no multi-object delete.
	gcsDeleteConcurrency = 16
)

// gcsResponseHeaderTimeout bounds how long a GCS request waits for a response
// once it is sent, for the same reason as s3ResponseHeaderTimeout: a request
// on a connection that silently stopped carrying traffic is retried instead
// of stalling lock renewals and synchronous WAL uploads.
var gcsResponseHeaderTimeout = 15 * time.Second

// GCS runs over HTTP/2, where every request shares one connection. The
// client's default health check closes a dead connection only after 31 s
// without frames plus a 15 s ping timeout; these close it after 10 + 5 s.
const (
	gcsHTTP2SendPingTimeout = 10 * time.Second
	gcsHTTP2PingTimeout     = 5 * time.Second
)

// gcsCredentialTypes are the credential file types GCSConfig.CredentialsFile
// accepts. The client library deprecated loading a credentials file without
// naming the type it is expected to hold.
var gcsCredentialTypes = []credentials.CredType{
	credentials.ServiceAccount,
	credentials.AuthorizedUser,
	credentials.ExternalAccount,
	credentials.ImpersonatedServiceAccount,
}

// GCSStore implements Store, ConditionalStore and VersionedStore backed by a
// Google Cloud Storage bucket through the GCS JSON API.
//
// Conditional writes use generation preconditions, which GCS enforces:
// PutIfAbsent writes with ifGenerationMatch=0 and PutIfMatch with
// ifGenerationMatch=<generation>. ETags are object generations as decimal
// strings. Do not point an S3Store at GCS's S3-compatible endpoint for a
// multi-node cluster: that endpoint ignores If-None-Match and If-Match on
// PUT, so leader election is not safe there.
type GCSStore struct {
	client *storage.Client
	// bucket retries what the client deems idempotent: reads, lists and
	// conditional writes. always also retries unconditional writes and
	// deletes, as S3Store does.
	bucket *storage.BucketHandle
	always *storage.BucketHandle
	prefix string // optional key prefix, without leading or trailing slashes
	owned  bool   // client created by NewGCSStoreFromConfig
}

// NewGCSStore creates a Store backed by the given GCS bucket.
// prefix is prepended to every key (may be empty). Leading and trailing
// slashes are ignored, so "t4", "t4/" and "/t4/" all address the same keys.
func NewGCSStore(client *storage.Client, bucket, prefix string) *GCSStore {
	return &GCSStore{
		client: client,
		bucket: client.Bucket(bucket).Retryer(storage.WithErrorFunc(gcsShouldRetry)),
		always: client.Bucket(bucket).Retryer(storage.WithErrorFunc(gcsShouldRetry), storage.WithPolicy(storage.RetryAlways)),
		prefix: strings.Trim(prefix, "/"),
	}
}

// GCSConfig holds the parameters for NewGCSStoreFromConfig.
type GCSConfig struct {
	// Bucket is the GCS bucket name. Required.
	Bucket string

	// Prefix is an optional key prefix. Leading and trailing slashes are
	// ignored.
	Prefix string

	// CredentialsFile is an optional credentials JSON file used instead of
	// Application Default Credentials. Its type must be service_account,
	// authorized_user, external_account or impersonated_service_account.
	CredentialsFile string
}

// NewGCSStoreFromConfig creates a GCSStore from explicit t4 GCS settings.
//
// Credential resolution order:
//   - STORAGE_EMULATOR_HOST set: no credentials; requests go to the emulator.
//   - CredentialsFile: credentials from that file.
//   - otherwise: Application Default Credentials (GOOGLE_APPLICATION_CREDENTIALS,
//     the gcloud user credentials file, or the metadata server, which covers
//     GKE Workload Identity).
//
// Call Close to release the client. Use NewGCSStore directly when you already
// have a preconfigured *storage.Client.
func NewGCSStoreFromConfig(ctx context.Context, cfg GCSConfig) (*GCSStore, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("object/gcs: bucket not configured")
	}
	opts, err := gcsClientOptions(cfg)
	if err != nil {
		return nil, err
	}
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("object/gcs: storage client: %w", err)
	}
	s := NewGCSStore(client, cfg.Bucket, cfg.Prefix)
	s.owned = true
	return s, nil
}

// gcsClientOptions builds the options the constructor passes to
// storage.NewClient: an HTTP client over gcsTransport that authenticates
// with the configured credentials.
func gcsClientOptions(cfg GCSConfig) ([]option.ClientOption, error) {
	tr := gcsTransport()
	if os.Getenv("STORAGE_EMULATOR_HOST") != "" {
		hc, err := httptransport.NewClient(&httptransport.Options{BaseRoundTripper: tr, DisableAuthentication: true})
		if err != nil {
			return nil, fmt.Errorf("object/gcs: http client: %w", err)
		}
		return []option.ClientOption{option.WithHTTPClient(hc)}, nil
	}
	creds, err := gcsCredentials(cfg.CredentialsFile)
	if err != nil {
		return nil, err
	}
	hc, err := httptransport.NewClient(&httptransport.Options{BaseRoundTripper: tr, Credentials: creds})
	if err != nil {
		return nil, fmt.Errorf("object/gcs: http client: %w", err)
	}
	// The credentials are passed as well so that the client does not look
	// for Application Default Credentials again.
	return []option.ClientOption{option.WithHTTPClient(hc), option.WithAuthCredentials(creds)}, nil
}

func gcsCredentials(credentialsFile string) (*auth.Credentials, error) {
	opts := &credentials.DetectOptions{Scopes: []string{storage.ScopeReadWrite}}
	if credentialsFile == "" {
		creds, err := credentials.DetectDefault(opts)
		if err != nil {
			return nil, fmt.Errorf("object/gcs: application default credentials: %w; set T4_GCS_CREDENTIALS_FILE or GOOGLE_APPLICATION_CREDENTIALS, or run on GCP with a service account", err)
		}
		return creds, nil
	}
	credType, err := gcsCredentialsFileType(credentialsFile)
	if err != nil {
		return nil, err
	}
	creds, err := credentials.NewCredentialsFromFile(credType, credentialsFile, opts)
	if err != nil {
		return nil, fmt.Errorf("object/gcs: credentials file %q: %w", credentialsFile, err)
	}
	return creds, nil
}

// gcsCredentialsFileType returns the type recorded in a credentials file if it
// is one of gcsCredentialTypes.
func gcsCredentialsFileType(path string) (credentials.CredType, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("object/gcs: read credentials file: %w", err)
	}
	var f struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(b, &f); err != nil {
		return "", fmt.Errorf("object/gcs: credentials file %q is not JSON: %w", path, err)
	}
	t := credentials.CredType(f.Type)
	if !slices.Contains(gcsCredentialTypes, t) {
		return "", fmt.Errorf("object/gcs: credentials file %q has type %q; want one of %v", path, f.Type, gcsCredentialTypes)
	}
	return t, nil
}

// gcsTransport returns the base transport of GCS clients: Go's default
// transport with the response wait bounded by gcsResponseHeaderTimeout, a
// faster HTTP/2 health check, and the client library's idle connection limit.
func gcsTransport() *http.Transport {
	tr, ok := http.DefaultTransport.(*http.Transport)
	if ok {
		tr = tr.Clone()
	} else {
		tr = &http.Transport{Proxy: http.ProxyFromEnvironment, ForceAttemptHTTP2: true}
	}
	tr.MaxIdleConnsPerHost = 100
	tr.ResponseHeaderTimeout = gcsResponseHeaderTimeout
	tr.HTTP2 = &http.HTTP2Config{
		SendPingTimeout: gcsHTTP2SendPingTimeout,
		PingTimeout:     gcsHTTP2PingTimeout,
	}
	return tr
}

// gcsShouldRetry is the client's own retry predicate plus network timeouts,
// such as a response that did not start within gcsResponseHeaderTimeout. The
// client stops retrying once the caller's context is done.
func gcsShouldRetry(err error) bool {
	if storage.ShouldRetry(err) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// Close releases the client NewGCSStoreFromConfig created. It does nothing
// for a store built with NewGCSStore: the caller owns that client.
func (s *GCSStore) Close() error {
	if !s.owned {
		return nil
	}
	return s.client.Close()
}

func (s *GCSStore) key(k string) string {
	if s.prefix == "" {
		return k
	}
	return s.prefix + "/" + k
}

func (s *GCSStore) Put(ctx context.Context, key string, r io.Reader) error {
	if err := gcsWrite(ctx, s.always.Object(s.key(key)), r, nil); err != nil {
		return fmt.Errorf("object/gcs: put %q: %w", key, err)
	}
	return nil
}

func (s *GCSStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	r, err := s.bucket.Object(s.key(key)).NewReader(ctx)
	if err != nil {
		if isGCSNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("object/gcs: get %q: %w", key, err)
	}
	return r, nil
}

// GetETag returns the object body and its generation as the ETag. Both come
// from the same response, so they always describe the same version of the
// object; a read that reconnects stays on that generation.
func (s *GCSStore) GetETag(ctx context.Context, key string) (*GetWithETag, error) {
	r, err := s.bucket.Object(s.key(key)).NewReader(ctx)
	if err != nil {
		if isGCSNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("object/gcs: get-etag %q: %w", key, err)
	}
	if r.Attrs.Generation <= 0 {
		_ = r.Close()
		return nil, fmt.Errorf("object/gcs: get-etag %q: response carries no object generation", key)
	}
	return &GetWithETag{Body: r, ETag: strconv.FormatInt(r.Attrs.Generation, 10)}, nil
}

// PutIfAbsent writes to key only if it does not exist (ifGenerationMatch=0).
// Returns ErrPreconditionFailed if the key already exists.
func (s *GCSStore) PutIfAbsent(ctx context.Context, key string, r io.Reader) error {
	return s.putConditional(ctx, "put-if-absent", key, r, storage.Conditions{DoesNotExist: true})
}

// PutIfMatch writes to key only if its current generation equals matchETag
// (ifGenerationMatch=<matchETag>). Returns ErrPreconditionFailed if the object
// changed or no longer exists.
func (s *GCSStore) PutIfMatch(ctx context.Context, key string, r io.Reader, matchETag string) error {
	gen, err := strconv.ParseInt(matchETag, 10, 64)
	if err != nil || gen <= 0 {
		// No live object has such a generation. ifGenerationMatch=0 would
		// mean "absent", the opposite of If-Match, so it is not sent.
		return ErrPreconditionFailed
	}
	return s.putConditional(ctx, "put-if-match", key, r, storage.Conditions{GenerationMatch: gen})
}

// putConditional writes r to key under cond, tagged with a random write ID.
//
// The client retries a conditional write that failed with a transient error.
// If the first attempt landed and only its response was lost, the retry fails
// with 412 although this write is the live object. So on 412 the object's
// metadata is read back: if the live generation carries this write's ID, the
// write landed. If this write landed and was already overwritten, the result
// is ErrPreconditionFailed, which is what the caller's next read would tell it
// anyway. If the metadata cannot be read, the outcome is unknown and the error
// returned is not ErrPreconditionFailed, so that the caller retries rather
// than concluding it lost. See docs/design/gcs-object-store.md.
func (s *GCSStore) putConditional(ctx context.Context, op, key string, r io.Reader, cond storage.Conditions) error {
	id, err := newGCSWriteID()
	if err != nil {
		return fmt.Errorf("object/gcs: %s %q: %w", op, key, err)
	}
	o := s.bucket.Object(s.key(key))
	err = gcsWrite(ctx, o.If(cond), r, map[string]string{gcsWriteIDKey: id})
	if err == nil {
		return nil
	}
	if !isGCSPreconditionFailed(err) {
		return fmt.Errorf("object/gcs: %s %q: %w", op, key, err)
	}
	attrs, aerr := o.Attrs(ctx)
	switch {
	case aerr == nil && attrs.Metadata[gcsWriteIDKey] == id:
		return nil
	case aerr == nil || isGCSNotFound(aerr):
		return ErrPreconditionFailed
	default:
		return fmt.Errorf("object/gcs: %s %q: precondition failed, and checking whether an earlier attempt of this write landed failed: %w", op, key, aerr)
	}
}

// gcsWrite uploads r to o. The upload is a single request when r's size is
// known and below gcsMaxChunkSize, and a resumable upload otherwise.
func gcsWrite(ctx context.Context, o *storage.ObjectHandle, r io.Reader, metadata map[string]string) error {
	// Cancelling the context before Close abandons the upload, so a reader
	// error never leaves a truncated object behind.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w := o.NewWriter(ctx)
	w.ContentType = "application/octet-stream"
	w.ChunkSize = gcsChunkSize(sniffReaderSize(r))
	w.Metadata = metadata
	if _, err := io.Copy(w, r); err != nil {
		cancel()
		_ = w.Close()
		return err
	}
	return w.Close()
}

// gcsChunkSize returns the Writer chunk size for a body of size bytes (-1 if
// unknown). A body smaller than the chunk size goes in one request, buffered
// in a chunk-sized buffer, so a small body gets a chunk just above its size
// rather than the client's 16 MiB default: the leader lock is rewritten every
// few seconds. The client rounds the chunk size up to a multiple of 256 KiB.
func gcsChunkSize(size int64) int {
	if size < 0 || size >= gcsMaxChunkSize {
		return gcsMaxChunkSize
	}
	return int(size) + 1
}

func newGCSWriteID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("write id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// GetVersioned retrieves a specific generation of key. versionID is a
// generation, as returned by GetETag. Generations other than the live one
// require Object Versioning on the bucket.
func (s *GCSStore) GetVersioned(ctx context.Context, key, versionID string) (io.ReadCloser, error) {
	gen, err := strconv.ParseInt(versionID, 10, 64)
	if err != nil || gen <= 0 {
		return nil, ErrNotFound
	}
	r, err := s.bucket.Object(s.key(key)).Generation(gen).NewReader(ctx)
	if err != nil {
		if isGCSNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("object/gcs: get versioned %q: %w", key, err)
	}
	return r, nil
}

func (s *GCSStore) Delete(ctx context.Context, key string) error {
	if err := s.always.Object(s.key(key)).Delete(ctx); err != nil && !isGCSNotFound(err) {
		return fmt.Errorf("object/gcs: delete %q: %w", key, err)
	}
	return nil
}

// DeleteMany deletes keys gcsDeleteConcurrency at a time: the Go client has
// no multi-object delete.
func (s *GCSStore) DeleteMany(ctx context.Context, keys []string) error {
	var (
		mu   sync.Mutex
		errs []error
		wg   sync.WaitGroup
		sem  = make(chan struct{}, gcsDeleteConcurrency)
	)
	for _, k := range keys {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()
			if err := s.always.Object(s.key(k)).Delete(ctx); err != nil && !isGCSNotFound(err) {
				mu.Lock()
				errs = append(errs, fmt.Errorf("object/gcs: delete-many %q: %w", k, err))
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

// List returns every key under prefix, across all result pages. Like S3Store
// it lists recursively (no delimiter).
func (s *GCSStore) List(ctx context.Context, prefix string) ([]string, error) {
	q := &storage.Query{Prefix: s.key(prefix)}
	if err := q.SetAttrSelection([]string{"Name"}); err != nil {
		return nil, fmt.Errorf("object/gcs: list %q: %w", prefix, err)
	}
	var keys []string
	it := s.bucket.Objects(ctx, q)
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("object/gcs: list %q: %w", prefix, err)
		}
		k := attrs.Name
		if s.prefix != "" {
			k = k[len(s.prefix)+1:]
		}
		keys = append(keys, k)
	}
	// GCS lists in lexicographic order already; sorting guarantees the
	// Store contract whatever serves the API (emulators included).
	slices.Sort(keys)
	return keys, nil
}

func isGCSNotFound(err error) bool {
	if errors.Is(err, storage.ErrObjectNotExist) {
		return true
	}
	var e *googleapi.Error
	return errors.As(err, &e) && e.Code == http.StatusNotFound
}

func isGCSPreconditionFailed(err error) bool {
	var e *googleapi.Error
	return errors.As(err, &e) && e.Code == http.StatusPreconditionFailed
}
