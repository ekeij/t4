package t4

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestHashKVReplicasAgree checks that every node of a cluster hashes the same
// history to the same value once it has applied it, compaction included.
func TestHashKVReplicasAgree(t *testing.T) {
	cluster := newFailoverCluster(t, 3)
	leader := cluster.leader(t, 15*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := range 20 {
		if _, err := leader.Put(ctx, fmt.Sprintf("/k%d", i%5), []byte(fmt.Sprint(i)), 0); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if _, err := leader.Delete(ctx, "/k0"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := leader.Compact(ctx, leader.CurrentRevision()-3); err != nil {
		t.Fatalf("compact: %v", err)
	}
	rev, err := leader.Put(ctx, "/k1", []byte("last"), 0)
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	want, err := leader.HashKV(rev)
	if err != nil {
		t.Fatalf("leader HashKV: %v", err)
	}
	if want.CompactRevision == 0 {
		t.Fatalf("leader hash %+v does not reflect the compaction", want)
	}
	for _, n := range cluster.survivors(leader) {
		if err := n.WaitForRevision(ctx, rev); err != nil {
			t.Fatalf("follower WaitForRevision(%d): %v", rev, err)
		}
		got, err := n.HashKV(rev)
		if err != nil {
			t.Fatalf("follower HashKV: %v", err)
		}
		if got.Hash != want.Hash || got.CompactRevision != want.CompactRevision {
			t.Fatalf("follower hash %+v, leader %+v", got, want)
		}
	}
}
