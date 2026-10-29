# Native Google Cloud Storage object store

## Why

Leader election and the WAL uploader rely on atomic conditional writes on the object store (`If-None-Match: *` and
`If-Match: <etag>` on S3). GCS's S3-compatible XML API (`storage.googleapis.com` with HMAC keys) accepts both headers on
PUT and ignores them: two candidates can both "win" the leader lock, so a multi-node cluster on GCS through `S3Store`
can split-brain. GCS's own generation preconditions (`ifGenerationMatch`, with generation `0` meaning "must not exist")
are enforced. `GCSStore` talks to the GCS JSON API through `cloud.google.com/go/storage` and expresses every conditional
write as a generation precondition.

## Interface mapping

`GCSStore` implements `object.Store`, `object.ConditionalStore` and `object.VersionedStore`, the same set as `S3Store`.
The interface has no conditional delete, so none is added. `o` below is `client.Bucket(bucket).Object(prefix + "/" +
key)`; the prefix is trimmed of slashes exactly like `S3Store`'s.

| Method | GCS call | Notes |
|---|---|---|
| `Put` | `o.NewWriter` → `objects.insert` | Unconditional. Retried on transient errors (`RetryAlways`). |
| `Get` | `o.NewReader` → object GET | `ErrObjectNotExist` → `ErrNotFound`. A reader that reconnects mid-read stays pinned to the generation it started on. |
| `Delete` | `o.Delete` → `objects.delete` | Not found is success. Retried on transient errors (`RetryAlways`). |
| `DeleteMany` | `o.Delete` per key, 16 at a time | The Go client has no multi-object delete. Errors are joined, as in `S3Store`. |
| `List` | `bucket.Objects(Query{Prefix})` → `objects.list`, every page | No delimiter, so the listing is recursive like `S3Store`'s. Only `name` is requested. Keys are returned without the store prefix, in lexicographic order. |
| `GetETag` | `o.NewReader` → object GET | The ETag is the generation of the body that was read, as a decimal string, from the same response. |
| `PutIfAbsent` | `o.If(Conditions{DoesNotExist: true}).NewWriter` → `objects.insert?ifGenerationMatch=0` | 412 → `ErrPreconditionFailed` (see below). |
| `PutIfMatch` | `o.If(Conditions{GenerationMatch: gen}).NewWriter` → `objects.insert?ifGenerationMatch=gen` | An ETag that is not a positive integer → `ErrPreconditionFailed`: no live object has generation ≤ 0, and `ifGenerationMatch=0` would mean "absent", the opposite of `If-Match`. |
| `GetVersioned` | `o.Generation(gen).NewReader` → object GET `?generation=gen` | `versionID` is a generation (the ETag `GetETag` returned). Malformed or unknown → `ErrNotFound`, as `S3Store` maps an invalid version ID. Older generations need Object Versioning on the bucket. |

### Version token

Wherever `S3Store` returns or accepts an ETag, `GCSStore` uses the object generation. Callers treat ETags as opaque:
`internal/election` and `internal/checkpoint` only pass the value from `GetETag` back to `PutIfMatch`, and nothing
parses it, strips quotes, or compares it with a content MD5. `Mem` uses an MD5 hex string, `S3Store` the S3 ETag.
Nothing needed flagging.

### Errors

`GCSStore` returns the same sentinels as `S3Store`, unwrapped where `S3Store` returns them unwrapped (some callers
compare with `==`):

- 404 on reads → `ErrNotFound`; 404 on delete → success.
- 412 on a conditional write → `ErrPreconditionFailed`, unless the write turns out to have landed (below).
- Anything else is wrapped as `object/gcs: <op> "<key>": <cause>`.

## Retried conditional writes

The client retries a conditional write on a transient error: `DoesNotExist` and `GenerationMatch` make it idempotent in
the client's eyes. If the first attempt landed and only its response was lost, the retry fails with 412 although our
write is the live object.

`S3Store` does not handle this. minio-go retries a conditional PUT the same way and `S3Store` maps the 412 to
`ErrPreconditionFailed`. Callers cope unevenly. The WAL uploader byte-compares the existing segment and accepts its own
copy. Leader election re-reads the lock, finds its own record, and reports the election as lost: `electAndStart` then
follows its own address until a takeover repairs it. That is a liveness bug, not a safety bug. It is outside this change
and left for a follow-up.

`GCSStore` resolves the ambiguity in the store, so callers never see it:

1. Every conditional write carries a random 128-bit write ID in custom metadata (`x-goog-meta-t4-write-id`).
2. On 412 the store reads the object's metadata (`objects.get`, no body). If the live generation carries this write's
   ID, the write landed and the call succeeds. Otherwise it returns `ErrPreconditionFailed`.
3. If that metadata read fails, the store returns an error that is not `ErrPreconditionFailed`. The outcome is unknown,
   and every caller retries a plain error rather than concluding it lost.

The check runs on every 412, not only after a detected retry. Retries happen inside the client and transport where the
store cannot reliably observe them. A genuine conflict never carries our ID, so the answer is the same, and it costs one
metadata read per conflict, which is rare. An ID is used instead of comparing content because it identifies this write
rather than equal bytes. It also needs no hashing of streamed bodies (WAL segments are uploaded straight from the file).
If our write landed and someone overwrote it before the check, the store reports `ErrPreconditionFailed`. That is what
the caller would learn from its next read anyway, and every caller re-reads on that error. The metadata does not change
any T4 object format: bodies and keys are untouched.

## Uploads

- `ContentType` is `application/octet-stream`, so the client does not sniff the body.
- When the body's size is known (`Len()` readers, `*os.File`, as in `S3Store.sniffReaderSize`) and below 16 MiB, the
  chunk size is set just above it. The upload is then a single request with a buffer of the object's size, not the
  client's default 16 MiB. That matters for the leader lock, which is rewritten every 2–20 s. Larger or unknown-size
  bodies stream as resumable uploads in 16 MiB chunks, each retried on its own. No multipart emulation is needed.
- A resumable upload carries its precondition from the initiating request, and GCS creates the object only when the
  upload is finalized. `TestGCSBackendEnforcesGenerationPreconditions` checks that the precondition holds at that point:
  it creates the key after the upload's first chunk was accepted and expects 412 on finalize. WAL segments, the only
  large conditional writes, depend on this.

## Transport and retries

`S3Store` bounds how long a request waits for a response (15 s), so that a request on a connection that silently
stopped carrying traffic is retried instead of stalling lock renewals and synchronous WAL uploads. GCS runs over HTTP/2,
where every request shares one connection. The client's default health check notices a dead connection only after
31 s + 15 s. `GCSStore` builds its own HTTP client:

- base transport cloned from `http.DefaultTransport`, `ResponseHeaderTimeout` 15 s (as `S3Store`), HTTP/2 health check
  after 10 s without frames with a 5 s ping timeout, `MaxIdleConnsPerHost` 100 (the client library's default);
- the retry predicate is `storage.ShouldRetry` plus network timeouts, so a response-header timeout is retried like
  minio-go does;
- unconditional `Put` and `Delete` use `RetryAlways`. The client does not retry them by default, but minio-go retries
  every request, and T4's unconditional writes are idempotent: content-addressed SSTs, checkpoint files under a unique
  term/revision, the single-writer manifest, branch registry entries.

## Credentials

- Default: Application Default Credentials (`GOOGLE_APPLICATION_CREDENTIALS`, the gcloud user file, or the metadata
  server, which covers GKE Workload Identity), scope `devstorage.read_write`.
- `CredentialsFile` overrides ADC. The file's `type` must be `service_account`, `authorized_user`, `external_account`
  (Workload Identity Federation) or `impersonated_service_account`. The client library deprecated loading credential
  files without naming the expected type.
- `STORAGE_EMULATOR_HOST` set: no authentication, and requests go to the emulator (client library behaviour).

## Configuration

Flags and environment variables mirror the S3 ones: `--gcs-bucket`, `--gcs-prefix`, `--gcs-credentials-file`, read from
`T4_GCS_BUCKET`, `T4_GCS_PREFIX`, `T4_GCS_CREDENTIALS_FILE` when the flag is unset. They exist on every command that has
`--s3-bucket`: `run`, `branch fork|unfork`, `restore list|checkpoint`, `gc`, `status`. A `gs://bucket/prefix` URL was
not chosen: S3 has no URL form, so it would add a second configuration style. Separate flags also keep the generated
flag and environment reference uniform.

Setting both `--s3-bucket` and `--gcs-bucket` (by flag or environment) is a startup error, and so is any `--s3-*` flag
next to `--gcs-bucket` or any `--gcs-*` flag next to `--s3-bucket`. Otherwise a prefix or credentials flag would be
silently ignored. Commands that required `--s3-bucket` now require exactly one of the two buckets. `--branch-prefix`
reads the source from whichever bucket is configured. `t4 run` logs a warning when `--s3-endpoint` is
`storage.googleapis.com`. It does not refuse to start, because existing configurations keep working in v1.x.

Embedded use mirrors `S3Store`:

```go
store, err := object.NewGCSStoreFromConfig(ctx, object.GCSConfig{Bucket: "my-bucket", Prefix: "t4/prod"})
// or, with a preconfigured *storage.Client:
store := object.NewGCSStore(client, "my-bucket", "t4/prod")
```

`GCSStore.Close` releases a client that `NewGCSStoreFromConfig` created.

Kine's `t4://` driver is out of scope. It builds its object store from its own `s3-*` arguments. Adding GCS there means
mapping a few `gcs-*` arguments to `object.GCSConfig` and calling `NewGCSStoreFromConfig`. Nothing above is specific to
the T4 CLI.

## Operational notes

- GCS limits mutations of a single object to about one per second. T4 writes `leader-lock` at most every 2 s in fast
  mode and `manifest/latest` once per checkpoint. 429 responses are retried with backoff, so checkpoints more frequent
  than that are slowed down, not broken.
- Soft delete (on by default for new buckets, 7 days) keeps every overwritten lock generation and every garbage-collected
  WAL segment, checkpoint and SST for its retention period. Consider disabling it on a bucket used only by T4. Enable
  Object Versioning only for point-in-time restore.

## Tests

- Unit tests (always run, every OS): configuration and credential-file validation, prefix handling, chunk sizing,
  transport bounds, and an in-process fake of the JSON API for retries. The fake applies a conditional write, drops its
  response, and answers the client's retry with 412: the store must report success. Variants cover a write overwritten
  before the retry (`ErrPreconditionFailed`), a 412 whose check fails (an error that is not `ErrPreconditionFailed`),
  and an upload that is never answered (retried after the response timeout).
- Backend tests run with `STORAGE_EMULATOR_HOST` (an emulator; each test creates a bucket) or `T4_GCS_TEST_BUCKET` (an
  existing bucket, using ADC or `T4_GCS_TEST_CREDENTIALS_FILE`). Without either they are skipped.
  - `TestGCSBackendEnforcesGenerationPreconditions` runs first and uses the plain client. It proves the backend
    rejects `DoesNotExist` on an existing object, `GenerationMatch` with a stale generation or on an absent object, and
    a resumable `DoesNotExist` upload whose key appeared mid-upload.
  - fake-gcs-server 1.56.1 with `-backend memory` passes, so CI runs every GCS test against it (job `gcs-emulator`). Its
    memory backend checks the precondition and creates the object under one lock.
  - Then: every store method (`TestGCSStoreMethods`, including pagination past 1000 keys, resumable uploads and noncurrent
    generations). The precondition cases (`TestGCSStorePreconditions`). 24 concurrent writers × 10 rounds of
    `PutIfAbsent`, `PutIfMatch` and encrypted `PutIfAbsent`, each round with exactly one winner
    (`TestGCSStoreConditionalRace`). A mutation that drops the preconditions fails all of them.
  - Cluster tests over one shared GCS store: `TestGCSLeaderFailover` (three nodes, leader cut off from peers and storage,
    exactly one survivor takes over and `t4_elections_total{outcome="won"}` rises by one), and
    `TestLeaderStepsDownWithoutObjectStorage` and `TestTakeoverWaitsForNodeWithAcknowledgedWrites` reused on GCS.
  - The binary (`tests/e2e`): a single node restored from the bucket alone, `status`, `restore`, `branch`, a branch node
    and `gc` with `--gcs-bucket`. Also a three-process cluster whose leader gets SIGKILL: exactly one survivor reports
    `t4_role{role="leader"} 1` and `t4_elections_total{outcome="won"} 1` in its own metrics.

Against a real bucket (the identity needs `roles/storage.objectUser` on it; tests work under `t4-test/` and
`t4-e2e/` and delete what they write):

```bash
export T4_GCS_TEST_BUCKET=my-test-bucket   # uses Application Default Credentials
go test -count=1 -run GCS ./pkg/object
go test -count=1 -run '^TestGCS' . ./tests/e2e
```
