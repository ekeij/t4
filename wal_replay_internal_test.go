package t4

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	istore "github.com/t4db/t4/internal/store"
	"github.com/t4db/t4/internal/wal"
	"github.com/t4db/t4/pkg/object"
)

// pinnedStore serves every version of a key as its current content, which is
// all replayPinned needs.
type pinnedStore struct{ *object.Mem }

func (s pinnedStore) GetVersioned(ctx context.Context, key, _ string) (io.ReadCloser, error) {
	return s.Get(ctx, key)
}

// putSegment uploads entries as one WAL segment of term and returns it pinned.
func putSegment(t *testing.T, obj object.Store, term uint64, entries ...wal.Entry) PinnedObject {
	t.Helper()
	sw, err := wal.OpenSegmentWriter(t.TempDir(), term, entries[0].Sequence())
	if err != nil {
		t.Fatal(err)
	}
	for i := range entries {
		if err := sw.Append(&entries[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := sw.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(sw.Path())
	if err != nil {
		t.Fatal(err)
	}
	key := wal.ObjectKey(term, entries[0].Sequence())
	if err := obj.Put(context.Background(), key, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	return PinnedObject{Key: key, VersionID: "v1"}
}

// TestReplayPinnedSkipsSupersededTerm pins every WAL segment in the store, as
// a restore point listing wal/ does, including a deposed leader's segment
// that overlaps the term that replaced it. The replay must keep only the
// newer term's history, as replayRemote does.
func TestReplayPinnedSkipsSupersededTerm(t *testing.T) {
	obj := pinnedStore{object.NewMem()}
	// Term 1 wrote /a twice, but its second write never committed: term 2
	// took over at sequence 2 and created /b there instead.
	old := putSegment(t, obj, 1,
		wal.Entry{Revision: 1, Term: 1, Op: wal.OpCreate, Key: "/a", Value: []byte("a1"), CreateRevision: 1, Version: 1},
		wal.Entry{Revision: 2, Term: 1, Op: wal.OpUpdate, Key: "/a", Value: []byte("a2"), CreateRevision: 1, PrevRevision: 1, Version: 2},
	)
	newer := putSegment(t, obj, 2,
		wal.Entry{Revision: 2, Term: 2, Op: wal.OpCreate, Key: "/b", Value: []byte("b1"), CreateRevision: 2, Version: 1},
	)

	db, err := istore.OpenMem()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	rp := &RestorePoint{Store: obj, WALSegments: []PinnedObject{old, newer}}
	if err := replayPinned(context.Background(), db, rp, 0, NoopLogger); err != nil {
		t.Fatalf("replayPinned: %v", err)
	}

	for key, want := range map[string]string{"/a": "a1", "/b": "b1"} {
		kv, err := db.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if kv == nil || string(kv.Value) != want {
			t.Errorf("Get(%q) = %+v, want %q", key, kv, want)
		}
	}
	if got := db.CurrentRevision(); got != 2 {
		t.Errorf("CurrentRevision = %d, want 2", got)
	}
}
