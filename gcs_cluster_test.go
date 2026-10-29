package t4

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/t4db/t4/internal/metrics"
	"github.com/t4db/t4/pkg/object"
)

// Cluster tests over Google Cloud Storage. Like the pkg/object GCS tests they
// run with T4_GCS_TEST_BUCKET (an existing bucket, with Application Default
// Credentials or T4_GCS_TEST_CREDENTIALS_FILE) or STORAGE_EMULATOR_HOST (an
// emulator; each test creates a bucket), and are skipped otherwise.

// newGCSClusterStore returns a GCSStore under a prefix of its own.
func newGCSClusterStore(t *testing.T) *object.GCSStore {
	t.Helper()
	bucket := os.Getenv("T4_GCS_TEST_BUCKET")
	if bucket == "" && os.Getenv("STORAGE_EMULATOR_HOST") == "" {
		t.Skip("set T4_GCS_TEST_BUCKET (real bucket, ADC) or STORAGE_EMULATOR_HOST (emulator) to run GCS cluster tests")
	}
	ctx := context.Background()
	if bucket == "" {
		client, err := storage.NewClient(ctx)
		if err != nil {
			t.Fatal(err)
		}
		bucket = fmt.Sprintf("t4-cluster-%d", time.Now().UnixNano())
		if err := client.Bucket(bucket).Create(ctx, "t4-test", nil); err != nil {
			t.Fatalf("create bucket: %v", err)
		}
		t.Cleanup(func() {
			_ = client.Bucket(bucket).Delete(context.Background())
			_ = client.Close()
		})
	}
	s, err := object.NewGCSStoreFromConfig(ctx, object.GCSConfig{
		Bucket:          bucket,
		Prefix:          fmt.Sprintf("t4-test/%s-%d", t.Name(), time.Now().UnixNano()),
		CredentialsFile: os.Getenv("T4_GCS_TEST_CREDENTIALS_FILE"),
	})
	if err != nil {
		t.Fatal(err)
	}
	// Registered after the bucket cleanup, so it runs first.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if keys, err := s.List(ctx, ""); err == nil {
			_ = s.DeleteMany(ctx, keys)
		}
		_ = s.Close()
	})
	return s
}

// gcsClusterConfig keeps a test cluster within GCS's limit of about one
// mutation per second on an object: the failover cluster's 300 ms checkpoints
// would rewrite manifest/latest faster than that.
func gcsClusterConfig(cfg *Config) {
	cfg.CheckpointInterval = 5 * time.Second
	cfg.SegmentMaxAge = time.Second
}

// TestGCSLeaderFailover kills the leader of a three-node cluster on GCS: its
// peers and its object storage go silent at once, as in a crash. Exactly one
// survivor must take over, with every acknowledged write.
func TestGCSLeaderFailover(t *testing.T) {
	cluster := newFailoverClusterOn(t, 3, newGCSClusterStore(t), gcsClusterConfig)
	leader := cluster.leader(t, 30*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var last int64
	for i := range 20 {
		rev, err := leader.Put(ctx, fmt.Sprintf("/before/%d", i), []byte("v"), 0)
		if err != nil {
			t.Fatal(err)
		}
		last = rev
	}
	survivors := cluster.survivors(leader)
	for _, n := range survivors {
		if err := n.WaitForRevision(ctx, last); err != nil {
			t.Fatal(err)
		}
	}

	won := testutil.ToFloat64(metrics.ElectionsTotal.WithLabelValues("won"))
	i := cluster.indexOf(leader)
	cluster.proxies[i].block()
	cluster.stores[i].block()
	killed := time.Now()

	newLeader := waitForLeaderNodeLocal(t, survivors, 90*time.Second)
	t.Logf("%s leads %v after the leader was killed", newLeader.cfg.NodeID, time.Since(killed))
	// Exactly one survivor leads, and keeps leading.
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		for _, n := range survivors {
			if n != newLeader && n.IsLeader() {
				t.Fatalf("%s and %s both lead", newLeader.cfg.NodeID, n.cfg.NodeID)
			}
		}
		if !newLeader.IsLeader() {
			t.Fatalf("%s stopped leading", newLeader.cfg.NodeID)
		}
	}
	if got := testutil.ToFloat64(metrics.ElectionsTotal.WithLabelValues("won")) - won; got != 1 {
		t.Fatalf("t4_elections_total{outcome=\"won\"} rose by %v after the leader was killed, want 1", got)
	}
	for i := range 20 {
		if kv, err := newLeader.Get(fmt.Sprintf("/before/%d", i)); err != nil || kv == nil {
			t.Fatalf("new leader lost /before/%d: %v, %v", i, kv, err)
		}
	}
	if _, err := newLeader.Put(ctx, "/after", []byte("v"), 0); err != nil {
		t.Fatalf("write after failover: %v", err)
	}
}

// TestGCSLeaderStepsDownWithoutObjectStorage is
// TestLeaderStepsDownWithoutObjectStorage on GCS.
func TestGCSLeaderStepsDownWithoutObjectStorage(t *testing.T) {
	testLeaderStepsDownWithoutObjectStorage(t, newFailoverClusterOn(t, 2, newGCSClusterStore(t), gcsClusterConfig))
}

// TestGCSTakeoverWaitsForNodeWithAcknowledgedWrites is
// TestTakeoverWaitsForNodeWithAcknowledgedWrites on GCS.
func TestGCSTakeoverWaitsForNodeWithAcknowledgedWrites(t *testing.T) {
	testTakeoverWaitsForNodeWithAcknowledgedWrites(t, newGCSClusterStore(t))
}
