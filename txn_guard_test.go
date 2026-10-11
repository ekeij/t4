package t4

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestTxnDeleteLease checks that TxnDeleteLease deletes exactly the keys
// attached to its lease when the txn commits, on the leader and forwarded
// from a follower.
func TestTxnDeleteLease(t *testing.T) {
	cluster := newFailoverCluster(t, 2)
	leader := cluster.leader(t, 15*time.Second)
	follower := cluster.survivors(leader)[0]
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for name, n := range map[string]*Node{"leader": leader, "follower": follower} {
		t.Run(name, func(t *testing.T) {
			const lease = 7
			mustPut := func(key string, lease int64) {
				t.Helper()
				if _, err := n.Put(ctx, key, []byte("v"), lease); err != nil {
					t.Fatalf("Put(%s): %v", key, err)
				}
			}
			mustPut("/a", lease)
			mustPut("/b", lease)
			mustPut("/c", lease)
			mustPut("/c", 0) // detached again
			mustPut("/d", lease+1)

			resp, err := n.Txn(ctx, TxnRequest{Success: []TxnOp{{Type: TxnDeleteLease, Lease: lease}}})
			if err != nil {
				t.Fatalf("Txn: %v", err)
			}
			if len(resp.DeletedKeys) != 2 {
				t.Fatalf("deleted %v, want /a and /b", resp.DeletedKeys)
			}
			if err := n.WaitForRevision(ctx, resp.Revision); err != nil {
				t.Fatal(err)
			}
			for key, want := range map[string]bool{"/a": false, "/b": false, "/c": true, "/d": true} {
				kv, err := n.Get(key)
				if err != nil {
					t.Fatal(err)
				}
				if (kv != nil) != want {
					t.Errorf("%s exists=%v, want %v", key, kv != nil, want)
				}
			}
			for _, key := range []string{"/c", "/d"} {
				if _, err := n.Delete(ctx, key); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// TestTxnGuards checks that a txn whose guard fails writes nothing and
// returns ErrTxnGuard, unlike a failed condition, on the leader and forwarded
// from a follower.
func TestTxnGuards(t *testing.T) {
	cluster := newFailoverCluster(t, 2)
	leader := cluster.leader(t, 15*time.Second)
	follower := cluster.survivors(leader)[0]
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := leader.MetaPut(ctx, "guard", []byte("x")); err != nil {
		t.Fatalf("MetaPut: %v", err)
	}

	exists := func(key string) TxnCondition {
		return TxnCondition{Key: key, Target: TxnCondMetaExists, Result: TxnCondEqual, Version: 1}
	}
	for name, n := range map[string]*Node{"leader": leader, "follower": follower} {
		t.Run(name, func(t *testing.T) {
			put := []TxnOp{{Type: TxnPut, Key: "/" + name, Value: []byte("v")}}
			_, err := n.Txn(ctx, TxnRequest{Guards: []TxnCondition{exists("missing")}, Success: put, Failure: put})
			if !errors.Is(err, ErrTxnGuard) {
				t.Fatalf("Txn with a failing guard: got %v, want ErrTxnGuard", err)
			}
			if kv, err := leader.Get("/" + name); err != nil || kv != nil {
				t.Fatalf("a failed guard wrote /%s: %v, %v", name, kv, err)
			}
			resp, err := n.Txn(ctx, TxnRequest{Guards: []TxnCondition{exists("guard")}, Success: put})
			if err != nil || !resp.Succeeded || resp.Revision == 0 {
				t.Fatalf("Txn with a holding guard: %+v, %v", resp, err)
			}
		})
	}
}
