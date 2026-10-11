package store

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"

	"github.com/cockroachdb/pebble"
)

// KVHash is the result of HashKV.
type KVHash struct {
	Hash            uint32
	CompactRevision int64
	// Revision is the revision the hash covers.
	Revision int64
	// CurrentRevision is the store's revision when the hash was taken.
	CurrentRevision int64
}

// HashKV hashes the key history up to rev (0 means the current revision),
// following etcd's HashKV: every record above the compact revision, and at or
// below it only the live version each key still has at rev. Which records a
// compaction physically kept therefore does not change the hash, so two
// replicas that applied the same log agree on it.
//
// Records are hashed in a canonical form rather than as stored bytes, so
// stores whose records were written by different binaries still agree. The
// meta keyspace is not covered, as etcd's HashKV covers only the key bucket.
func (s *Store) HashKV(rev int64) (KVHash, error) {
	snap := s.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	// Both revisions come from the snapshot: a compaction that lands after
	// it was taken must not change which records the hash expects to see.
	compactRev, err := snapshotRev(snap, metaCompactKey, 0)
	if err != nil {
		return KVHash{}, err
	}
	currentRev, err := snapshotRev(snap, metaCurrentRevKey, s.CurrentRevision())
	if err != nil {
		return KVHash{}, err
	}
	if rev == 0 {
		rev = currentRev
	}
	if compactRev > 0 && rev < compactRev {
		return KVHash{}, ErrCompacted
	}
	if rev > currentRev {
		return KVHash{}, ErrFutureRevision
	}

	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	if compactRev > 0 {
		if err := hashCompacted(snap, h, compactRev, rev); err != nil {
			return KVHash{}, err
		}
	}
	if err := hashLog(snap, compactRev+1, rev, func(lrev int64, r *recordView) {
		hashRecord(h, lrev, r)
	}); err != nil {
		return KVHash{}, err
	}
	return KVHash{
		Hash:            h.Sum32(),
		CompactRevision: compactRev,
		Revision:        rev,
		CurrentRevision: currentRev,
	}, nil
}

// hashCompacted hashes the records at or below compactRev that are still each
// key's live version at rev: the newest record of a key that has none in
// (compactRev, rev], unless that record is a deletion.
func hashCompacted(snap pebble.Reader, h hash.Hash32, compactRev, rev int64) error {
	seen := make(map[string]struct{})
	if err := hashLog(snap, compactRev+1, rev, func(_ int64, r *recordView) {
		seen[string(r.key)] = struct{}{}
	}); err != nil {
		return err
	}

	type kept struct {
		rev int64
		raw []byte
	}
	var keep []kept
	iter, err := snap.NewIter(&pebble.IterOptions{LowerBound: logLower, UpperBound: logKey(compactRev + 1)})
	if err != nil {
		return fmt.Errorf("store: hash iter: %w", err)
	}
	defer func() { _ = iter.Close() }()
	// Walk backwards so the first record met for a key is its newest.
	for iter.Last(); iter.Valid(); iter.Prev() {
		r, err := decodeRecord(iter.Value())
		if err != nil {
			return fmt.Errorf("store: hash decode rev=%d: %w", decodeLogKey(iter.Key()), err)
		}
		if _, ok := seen[string(r.key)]; ok {
			continue
		}
		seen[string(r.key)] = struct{}{}
		if r.delete {
			continue
		}
		keep = append(keep, kept{rev: decodeLogKey(iter.Key()), raw: append([]byte(nil), iter.Value()...)})
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("store: hash scan: %w", err)
	}
	for i := len(keep) - 1; i >= 0; i-- {
		r, err := decodeRecord(keep[i].raw)
		if err != nil {
			return err
		}
		hashRecord(h, keep[i].rev, &r)
	}
	return nil
}

// hashLog calls fn for every log record in [fromRev, toRev], in order.
func hashLog(snap pebble.Reader, fromRev, toRev int64, fn func(rev int64, r *recordView)) error {
	if toRev < fromRev {
		return nil
	}
	iter, err := snap.NewIter(&pebble.IterOptions{LowerBound: logKey(fromRev), UpperBound: logKey(toRev + 1)})
	if err != nil {
		return fmt.Errorf("store: hash iter: %w", err)
	}
	defer func() { _ = iter.Close() }()
	for iter.First(); iter.Valid(); iter.Next() {
		r, err := decodeRecord(iter.Value())
		if err != nil {
			return fmt.Errorf("store: hash decode rev=%d: %w", decodeLogKey(iter.Key()), err)
		}
		fn(decodeLogKey(iter.Key()), &r)
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("store: hash scan: %w", err)
	}
	return nil
}

// hashRecord writes the canonical form of the record at rev to h.
func hashRecord(h hash.Hash32, rev int64, r *recordView) {
	var flags byte
	if r.create {
		flags |= flagCreate
	}
	if r.delete {
		flags |= flagDelete
	}
	var b [1 + 8*5 + 4]byte
	b[0] = flags
	binary.BigEndian.PutUint64(b[1:], uint64(rev))
	binary.BigEndian.PutUint64(b[9:], uint64(r.createRevision))
	binary.BigEndian.PutUint64(b[17:], uint64(r.prevRevision))
	binary.BigEndian.PutUint64(b[25:], uint64(logVersion(r.version, r.delete)))
	binary.BigEndian.PutUint64(b[33:], uint64(r.lease))
	binary.BigEndian.PutUint32(b[41:], uint32(len(r.key)))
	h.Write(b[:])
	h.Write(r.key)
	h.Write(r.value)
}

// snapshotRev reads a revision meta key from snap, or def when it is absent.
func snapshotRev(snap pebble.Reader, key []byte, def int64) (int64, error) {
	v, closer, err := snap.Get(key)
	if err == pebble.ErrNotFound {
		return def, nil
	}
	if err != nil {
		return 0, fmt.Errorf("store: read %q: %w", key, err)
	}
	defer func() { _ = closer.Close() }()
	return decodeRev(v[:8]), nil
}
