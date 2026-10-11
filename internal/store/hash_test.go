package store

import (
	"errors"
	"testing"

	"github.com/t4db/t4/internal/wal"
)

func hashKV(t *testing.T, s *Store, rev int64) KVHash {
	t.Helper()
	h, err := s.HashKV(rev)
	if err != nil {
		t.Fatalf("HashKV(%d): %v", rev, err)
	}
	return h
}

func compactEntry(rev, compactAt int64) wal.Entry {
	return wal.Entry{Revision: rev, Term: 1, Op: wal.OpCompact, PrevRevision: compactAt}
}

func TestHashKVSameLogSameHash(t *testing.T) {
	entries := []wal.Entry{
		createEntry(1, "a", []byte("1")),
		createEntry(2, "b", []byte("2")),
		updateEntry(3, "a", []byte("3"), 1, 1),
		deleteEntry(4, "b", 2, 2),
	}
	a, b := openMem(t), openMem(t)
	apply(t, a, entries...)
	for _, e := range entries {
		apply(t, b, e)
	}
	if ha, hb := hashKV(t, a, 0), hashKV(t, b, 0); ha != hb {
		t.Fatalf("same log, different hashes: %+v vs %+v", ha, hb)
	}

	c := openMem(t)
	apply(t, c, entries[:3]...)
	apply(t, c, updateEntry(4, "b", []byte("x"), 2, 2))
	if ha, hc := hashKV(t, a, 0), hashKV(t, c, 0); ha.Hash == hc.Hash {
		t.Fatalf("different logs, same hash %d", ha.Hash)
	}
}

func TestHashKVPinnedRevisionIgnoresLaterWrites(t *testing.T) {
	s := openMem(t)
	apply(t, s, createEntry(1, "a", []byte("1")), createEntry(2, "b", []byte("2")))
	before := hashKV(t, s, 2)
	apply(t, s, updateEntry(3, "a", []byte("3"), 1, 1))
	after := hashKV(t, s, 2)
	if before.Hash != after.Hash || after.Revision != 2 || after.CurrentRevision != 3 {
		t.Fatalf("hash at rev 2 changed: before %+v, after %+v", before, after)
	}
	if head := hashKV(t, s, 0); head.Hash == after.Hash || head.Revision != 3 {
		t.Fatalf("hash at head %+v should cover rev 3", head)
	}
}

// TestHashKVCompactedHistory checks that, as in etcd, a record at or below the
// compact revision counts only while it is its key's live version: history
// that compaction drops, a key's version superseded after the compact
// revision, and deletions all leave the hash alone.
func TestHashKVCompactedHistory(t *testing.T) {
	build := func(firstA, compactedB string) *Store {
		s := openMem(t)
		apply(t, s,
			createEntry(1, "a", []byte(firstA)),
			updateEntry(2, "a", []byte("a2"), 1, 1),
			createEntry(3, "b", []byte(compactedB)),
			createEntry(4, "c", []byte("c")),
			deleteEntry(5, "c", 4, 4),
			compactEntry(5, 5),
			updateEntry(6, "b", []byte("b6"), 3, 3),
		)
		return s
	}
	base := build("a1", "b3")

	// a@1 was superseded before the compaction, so it is gone from history.
	if h1, h2 := hashKV(t, base, 0), hashKV(t, build("other", "b3"), 0); h1 != h2 {
		t.Fatalf("compacted-away history changed the hash: %+v vs %+v", h1, h2)
	}
	// b@3 is superseded by b@6 above the compact revision: it no longer
	// counts at rev 6, but it is still b's live version at rev 5.
	other := build("a1", "other")
	if h1, h2 := hashKV(t, base, 6), hashKV(t, other, 6); h1 != h2 {
		t.Fatalf("superseded compacted version changed the hash at rev 6: %+v vs %+v", h1, h2)
	}
	if h1, h2 := hashKV(t, base, 5), hashKV(t, other, 5); h1.Hash == h2.Hash {
		t.Fatalf("live compacted version did not count at rev 5: %+v", h1)
	}
	if h := hashKV(t, base, 0); h.CompactRevision != 5 || h.Revision != 6 {
		t.Fatalf("got %+v, want compact revision 5, revision 6", h)
	}
}

func TestHashKVRevisionBounds(t *testing.T) {
	s := openMem(t)
	apply(t, s,
		createEntry(1, "a", []byte("1")),
		updateEntry(2, "a", []byte("2"), 1, 1),
		updateEntry(3, "a", []byte("3"), 1, 2),
		compactEntry(3, 2),
	)
	if _, err := s.HashKV(1); !errors.Is(err, ErrCompacted) {
		t.Fatalf("HashKV below the compact revision: got %v, want ErrCompacted", err)
	}
	if _, err := s.HashKV(4); !errors.Is(err, ErrFutureRevision) {
		t.Fatalf("HashKV above the current revision: got %v, want ErrFutureRevision", err)
	}
	hashKV(t, s, 2)
}
