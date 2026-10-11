package t4

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestLeaderLinearizableReadCoversFollowers checks that a linearizable read
// on the leader is never behind one a follower already served. A follower
// applies a batch once it has acknowledged it, which can be before the
// leader, waiting for that acknowledgement, applies it.
func TestLeaderLinearizableReadCoversFollowers(t *testing.T) {
	cluster := newFailoverCluster(t, 3)
	leader := cluster.leader(t, 15*time.Second)
	followers := cluster.survivors(leader)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wctx, wcancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; wctx.Err() == nil; i++ {
			_, _ = leader.Put(wctx, fmt.Sprintf("/k%d", i%10), []byte("v"), 0)
		}
	}()
	defer func() { wcancel(); <-done }()

	for i := range 2000 {
		seen, err := followers[i%len(followers)].LinearizableRevision(ctx)
		if err != nil {
			t.Fatalf("follower LinearizableRevision: %v", err)
		}
		got, err := leader.LinearizableRevision(ctx)
		if err != nil {
			t.Fatalf("leader LinearizableRevision: %v", err)
		}
		if got < seen {
			t.Fatalf("leader read at revision %d after a follower read at %d", got, seen)
		}
	}
}
