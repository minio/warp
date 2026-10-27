/*
 * Warp (C) 2026 MinIO, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package bench

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
)

const (
	atomicMagic      = "WAT1"
	atomicHeaderSize = 48
	atomicMetaKey    = "Warp-Atomic"

	// AtomicMinBlockSize is the smallest block that still fits a stamp.
	AtomicMinBlockSize = atomicHeaderSize
)

// Atomic violation categories, reported as the prefix of Operation.Err.
const (
	AtomicTorn         = "torn"
	AtomicWrongKey     = "wrong-key"
	AtomicCorrupt      = "corrupt"
	AtomicLength       = "length"
	AtomicMissing      = "missing"
	AtomicMetaMissing  = "meta-missing"
	AtomicMetaMismatch = "meta-mismatch"
	AtomicETagMismatch = "etag-mismatch"
	AtomicETagMD5      = "etag-md5"
	AtomicStale        = "stale"
	AtomicPhantom      = "phantom"
	AtomicListMissing  = "list-missing"
	AtomicListDeleted  = "list-deleted"
	AtomicListSize     = "list-size"
	AtomicListETag     = "list-etag"
	AtomicListStale    = "list-stale"
)

var atomicCRC = crc32.MakeTable(crc32.Castagnoli)

// AtomicID identifies one PUT: the writer that issued it and its sequence
// number on that writer.
type AtomicID struct {
	Writer, Gen uint64
}

func (a AtomicID) String() string {
	return strconv.FormatUint(a.Writer, 16) + "." + strconv.FormatUint(a.Gen, 16)
}

// ParseAtomicID parses the form produced by AtomicID.String.
func ParseAtomicID(s string) (AtomicID, bool) {
	w, g, ok := strings.Cut(s, ".")
	if !ok {
		return AtomicID{}, false
	}
	wv, err1 := strconv.ParseUint(w, 16, 64)
	gv, err2 := strconv.ParseUint(g, 16, 64)
	if err1 != nil || err2 != nil {
		return AtomicID{}, false
	}
	return AtomicID{Writer: wv, Gen: gv}, true
}

// atomicStamp is what every block of an object carries, so any block
// identifies the PUT it came from without shared state.
type atomicStamp struct {
	ID      AtomicID
	Size    int64
	KeyHash uint64
}

func atomicKeyHash(key string) uint64 {
	return uint64(crc32.Checksum([]byte(key), atomicCRC))<<32 | uint64(len(key))
}

func (s atomicStamp) fillBlock(dst []byte, idx uint64) {
	var hdr [atomicHeaderSize]byte
	copy(hdr[:4], atomicMagic)
	binary.LittleEndian.PutUint64(hdr[4:], s.ID.Writer)
	binary.LittleEndian.PutUint64(hdr[12:], s.ID.Gen)
	binary.LittleEndian.PutUint64(hdr[20:], uint64(s.Size))
	binary.LittleEndian.PutUint64(hdr[28:], idx)
	binary.LittleEndian.PutUint64(hdr[36:], s.KeyHash)
	binary.LittleEndian.PutUint32(hdr[44:], crc32.Checksum(hdr[:44], atomicCRC))
	n := copy(dst, hdr[:])
	if n == len(dst) {
		return
	}
	x := s.ID.Writer*0x9e3779b97f4a7c15 ^ s.ID.Gen*0xbf58476d1ce4e5b9 ^ idx*0x94d049bb133111eb
	rest := dst[n:]
	for len(rest) >= 8 {
		x = splitmix64(x)
		binary.LittleEndian.PutUint64(rest, x)
		rest = rest[8:]
	}
	if len(rest) > 0 {
		var tail [8]byte
		binary.LittleEndian.PutUint64(tail[:], splitmix64(x))
		copy(rest, tail[:])
	}
}

func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func parseAtomicHeader(b []byte) (st atomicStamp, idx uint64, ok bool) {
	if len(b) < atomicHeaderSize || string(b[:4]) != atomicMagic {
		return st, 0, false
	}
	if crc32.Checksum(b[:44], atomicCRC) != binary.LittleEndian.Uint32(b[44:]) {
		return st, 0, false
	}
	st.ID.Writer = binary.LittleEndian.Uint64(b[4:])
	st.ID.Gen = binary.LittleEndian.Uint64(b[12:])
	st.Size = int64(binary.LittleEndian.Uint64(b[20:]))
	st.KeyHash = binary.LittleEndian.Uint64(b[36:])
	return st, binary.LittleEndian.Uint64(b[28:]), true
}

// atomicReader streams the stamped content of one PUT.
type atomicReader struct {
	st        atomicStamp
	blockSize int64
	off       int64
	buf       []byte
	bufIdx    int64
	last      time.Time
}

func newAtomicReader(st atomicStamp, blockSize int) *atomicReader {
	return &atomicReader{st: st, blockSize: int64(blockSize), buf: make([]byte, blockSize), bufIdx: -1}
}

func (r *atomicReader) Read(p []byte) (int, error) {
	if r.off >= r.st.Size {
		return 0, io.EOF
	}
	bi := r.off / r.blockSize
	blk := r.buf[:min(r.blockSize, r.st.Size-bi*r.blockSize)]
	if bi != r.bufIdx {
		r.st.fillBlock(blk, uint64(bi))
		r.bufIdx = bi
	}
	n := copy(p, blk[r.off-bi*r.blockSize:])
	r.off += int64(n)
	r.last = time.Now()
	return n, nil
}

func (r *atomicReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += r.off
	case io.SeekEnd:
		offset += r.st.Size
	default:
		return 0, errors.New("invalid whence")
	}
	if offset < 0 {
		return 0, errors.New("negative position")
	}
	r.off = offset
	return offset, nil
}

func (r *atomicReader) LastByte() *time.Time {
	return &r.last
}

// atomicVerifier checks a streamed body against the stamp in its first
// block and records the first fault found.
type atomicVerifier struct {
	blockSize int
	buf       []byte
	expect    []byte
	n         int
	idx       uint64
	total     int64
	first     *atomicStamp
	md5       hash.Hash
	fault     string
	detail    string
}

func newAtomicVerifier(blockSize int) *atomicVerifier {
	return &atomicVerifier{
		blockSize: blockSize,
		buf:       make([]byte, blockSize),
		expect:    make([]byte, blockSize),
		md5:       md5.New(),
	}
}

func (v *atomicVerifier) Write(p []byte) (int, error) {
	v.md5.Write(p)
	written := len(p)
	for len(p) > 0 {
		c := copy(v.buf[v.n:], p)
		v.n += c
		p = p[c:]
		if v.n == v.blockSize {
			v.checkBlock(v.buf)
			v.n = 0
		}
	}
	return written, nil
}

func (v *atomicVerifier) setFault(kind, detail string) {
	if v.fault == "" {
		v.fault, v.detail = kind, detail
	}
}

func (v *atomicVerifier) checkBlock(b []byte) {
	idx := v.idx
	v.idx++
	v.total += int64(len(b))
	if idx == 0 {
		st, bi, ok := parseAtomicHeader(b)
		if !ok || bi != 0 {
			v.setFault(AtomicCorrupt, "no valid stamp in first block")
			return
		}
		v.first = &st
	}
	if v.first == nil || v.fault != "" {
		return
	}
	start := int64(idx) * int64(v.blockSize)
	if start >= v.first.Size {
		return
	}
	want := v.expect[:min(int64(len(b)), v.first.Size-start)]
	v.first.fillBlock(want, idx)
	if bytes.Equal(b[:len(want)], want) {
		return
	}
	if st, bi, ok := parseAtomicHeader(b); ok && st.ID != v.first.ID {
		v.setFault(AtomicTorn, fmt.Sprintf("block %d is block %d of %s (size %d), object starts as %s (size %d)", idx, bi, st.ID, st.Size, v.first.ID, v.first.Size))
		return
	}
	v.setFault(AtomicCorrupt, fmt.Sprintf("block %d of %s does not match its content", idx, v.first.ID))
}

// finish checks the trailing partial block and the total length. It returns
// the stamp of the object, or nil when no stamp could be read.
func (v *atomicVerifier) finish() *atomicStamp {
	if v.n > 0 {
		v.checkBlock(v.buf[:v.n])
		v.n = 0
	}
	if v.first == nil {
		if v.total == 0 {
			v.setFault(AtomicLength, "empty body")
		}
		return nil
	}
	if v.total != v.first.Size {
		v.setFault(AtomicLength, fmt.Sprintf("body of %s is %d bytes, stamp says %d", v.first.ID, v.total, v.first.Size))
	}
	return v.first
}

func (v *atomicVerifier) md5Hex() string {
	return hex.EncodeToString(v.md5.Sum(nil))
}

type atomicWrite struct {
	start, end time.Time
	acked      bool
	etag       string
	size       int64
	endpoint   string
}

type atomicKeyHistory struct {
	writes map[AtomicID]*atomicWrite
	// pruned holds, per writer, the highest generation dropped from writes.
	// A writer issues one PUT at a time, so every lower generation of that
	// writer on the key was superseded too.
	pruned map[uint64]uint64
}

// atomicHistory records the real-time interval of every PUT this process
// issued, to decide whether a read returned data that was already overwritten.
// Timestamps from other warp clients are never compared against it.
type atomicHistory struct {
	mu        sync.Mutex
	owner     uint64
	keys      map[string]*atomicKeyHistory
	reads     map[uint64]time.Time
	nextRead  uint64
	sincePrun map[string]int
}

func newAtomicHistory(owner uint64) *atomicHistory {
	return &atomicHistory{
		owner:     owner,
		keys:      make(map[string]*atomicKeyHistory),
		reads:     make(map[uint64]time.Time),
		sincePrun: make(map[string]int),
	}
}

func (h *atomicHistory) key(k string) *atomicKeyHistory {
	kh := h.keys[k]
	if kh == nil {
		kh = &atomicKeyHistory{
			writes: make(map[AtomicID]*atomicWrite),
			pruned: make(map[uint64]uint64),
		}
		h.keys[k] = kh
	}
	return kh
}

func (h *atomicHistory) begin(k string, id AtomicID, start time.Time, size int64, endpoint string) {
	h.mu.Lock()
	h.key(k).writes[id] = &atomicWrite{start: start, size: size, endpoint: endpoint}
	h.mu.Unlock()
}

func (h *atomicHistory) endpoint(k string, id AtomicID) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if w := h.key(k).writes[id]; w != nil {
		return w.endpoint
	}
	return ""
}

// finish resolves a PUT. A PUT that failed stays unresolved forever, since
// it may still have been applied.
func (h *atomicHistory) finish(k string, id AtomicID, end time.Time, acked bool, etag string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	w := h.key(k).writes[id]
	if w == nil || !acked {
		return
	}
	w.end, w.acked, w.etag = end, true, etag
	h.sincePrun[k]++
	if h.sincePrun[k] >= 32 {
		h.sincePrun[k] = 0
		h.pruneLocked(k, end)
	}
}

// readBegin registers a read and returns its ticket and start time. The start
// is taken under the lock so that no prune runs between the two.
func (h *atomicHistory) readBegin() (uint64, time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	start := time.Now()
	return h.readBeginLocked(start), start
}

func (h *atomicHistory) readBeginAt(start time.Time) uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.readBeginLocked(start)
}

func (h *atomicHistory) readBeginLocked(start time.Time) uint64 {
	h.nextRead++
	h.reads[h.nextRead] = start
	return h.nextRead
}

func (h *atomicHistory) readEnd(t uint64) {
	h.mu.Lock()
	delete(h.reads, t)
	h.mu.Unlock()
}

// supersededBefore returns the acknowledged write on kh with the latest
// start among those that finished before t, excluding skip.
func supersededBefore(kh *atomicKeyHistory, t time.Time, skip AtomicID) (latestID AtomicID, latest atomicWrite) {
	for id, a := range kh.writes {
		if id == skip || !a.acked || !a.end.Before(t) {
			continue
		}
		if a.start.After(latest.start) {
			latestID, latest = id, *a
		}
	}
	return latestID, latest
}

// pruneLocked drops writes that every current and future read must see as
// overwritten: acknowledged writes that ended before another acknowledged
// write started, where that write finished before the oldest active read.
func (h *atomicHistory) pruneLocked(k string, now time.Time) {
	cutoff := now
	for _, t := range h.reads {
		if t.Before(cutoff) {
			cutoff = t
		}
	}
	kh := h.keys[k]
	_, latest := supersededBefore(kh, cutoff, AtomicID{})
	s := latest.start
	for id, w := range kh.writes {
		if !w.acked || !w.end.Before(s) || id.Writer>>32 != h.owner {
			continue
		}
		delete(kh.writes, id)
		if id.Gen > kh.pruned[id.Writer] {
			kh.pruned[id.Writer] = id.Gen
		}
	}
}

func (h *atomicHistory) forget(k string) {
	h.mu.Lock()
	delete(h.keys, k)
	delete(h.sincePrun, k)
	h.mu.Unlock()
}

// etag returns the ETag the server acknowledged for id, if known.
func (h *atomicHistory) etag(k string, id AtomicID) (etag string, size int64, ok bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	w := h.key(k).writes[id]
	if w == nil || !w.acked {
		return "", 0, false
	}
	return w.etag, w.size, true
}

// check classifies a read of id on key k by a request that started at
// readStart. It returns an empty string when the read is admissible.
func (h *atomicHistory) check(k string, id AtomicID, readStart time.Time) (kind, detail string) {
	if id.Writer>>32 != h.owner {
		return "", ""
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	kh := h.key(k)
	w := kh.writes[id]
	if w == nil {
		if id.Gen <= kh.pruned[id.Writer] {
			if aid, a := supersededBefore(kh, readStart, id); !a.start.IsZero() {
				return AtomicStale, fmt.Sprintf("returned %s, but %s (written via %s) was acknowledged %s before the read started",
					id, aid, a.endpoint, readStart.Sub(a.end).Round(time.Microsecond))
			}
			return AtomicStale, fmt.Sprintf("returned %s, which a later acknowledged PUT replaced", id)
		}
		return AtomicPhantom, fmt.Sprintf("returned %s, which this client never wrote to %s", id, k)
	}
	if !w.acked {
		return "", ""
	}
	if aid, a := supersededBefore(kh, readStart, id); w.end.Before(a.start) {
		return AtomicStale, fmt.Sprintf("returned %s, but %s (written via %s) was acknowledged %s before the read started",
			id, aid, a.endpoint, readStart.Sub(a.end).Round(time.Microsecond))
	}
	return "", ""
}

// Atomic overwrites a small set of keys concurrently and verifies that every
// GET and STAT returns exactly one whole acknowledged PUT: body, length,
// metadata and ETag all belonging to the same write, and not a write that
// was already overwritten before the read began.
type Atomic struct {
	Common

	Keys      int
	BlockSize int
	CheckMD5  bool
	// ReadAfterWrite follows every acknowledged PUT with a GET of the same
	// key from the same thread, which lands on the next host in --host.
	ReadAfterWrite bool
	Prefix         string

	// Seed drives every thread's choice of key, operation and object size.
	// Zero picks a random seed. The seed reproduces each thread's sequence of
	// operations, not the interleaving of threads and servers.
	Seed uint64
	// SizeFn picks an object size, following the configured size flags.
	SizeFn func(rng *rand.Rand) int64

	PutWeight, GetWeight, StatWeight, ListWeight float64

	GetOpts  minio.GetObjectOptions
	StatOpts minio.StatObjectOptions

	hist     *atomicHistory
	counts   map[string]int64
	countsMu sync.Mutex
}

func (g *Atomic) keyName(i int) string {
	return g.Prefix + "atomic-" + strconv.Itoa(i)
}

func (g *Atomic) writerID(thread uint64) uint64 {
	return uint64(g.ClientIdx)<<32 | thread
}

func (g *Atomic) putOpts(id AtomicID) minio.PutObjectOptions {
	opts := g.PutOpts
	opts.DisableMultipart = true
	opts.ContentType = "application/octet-stream"
	opts.UserMetadata = map[string]string{atomicMetaKey: id.String()}
	return opts
}

func (g *Atomic) stamp(rng *rand.Rand, key string, id AtomicID) atomicStamp {
	return atomicStamp{ID: id, Size: max(g.SizeFn(rng), int64(atomicHeaderSize)), KeyHash: atomicKeyHash(key)}
}

// threadRNG returns the random source for one thread of this client.
func (g *Atomic) threadRNG(thread uint64) *rand.Rand {
	return rand.New(rand.NewSource(int64(splitmix64(g.Seed ^ splitmix64(g.writerID(thread))))))
}

// Prepare creates the bucket and writes every key once so reads never race
// the first PUT.
func (g *Atomic) Prepare(ctx context.Context) error {
	g.counts = make(map[string]int64)
	if g.Keys < 1 {
		return errors.New("at least one key is required")
	}
	if g.BlockSize < AtomicMinBlockSize {
		return fmt.Errorf("block size must be at least %d bytes", AtomicMinBlockSize)
	}
	if g.PutWeight < 0 || g.GetWeight < 0 || g.StatWeight < 0 || g.ListWeight < 0 {
		return errors.New("distributions cannot be negative")
	}
	if g.ListWeight == 0 && (g.PutWeight == 0 || g.GetWeight+g.StatWeight == 0) {
		return errors.New("need a LIST distribution, or both PUT and GET or STAT distributions")
	}
	if g.SizeFn == nil {
		return errors.New("no object size function")
	}
	for g.Seed == 0 {
		g.Seed = rand.Uint64()
	}
	g.hist = newAtomicHistory(uint64(g.ClientIdx))
	if err := g.createEmptyBucket(ctx); err != nil {
		return err
	}
	g.UpdateStatus(fmt.Sprint("Writing ", g.Keys, " keys, seed ", g.Seed))
	writer := g.writerID(1<<32 - 1)
	rng := g.threadRNG(1<<32 - 1)
	for i := range g.Keys {
		id := AtomicID{Writer: writer, Gen: uint64(i + 1)}
		key := g.keyName(i)
		st := g.stamp(rng, key, id)
		cl, done := g.Client()
		start := time.Now()
		g.hist.begin(key, id, start, st.Size, cl.EndpointURL().String())
		res, err := cl.PutObject(ctx, g.Bucket, key, newAtomicReader(st, g.BlockSize), st.Size, g.putOpts(id))
		if err == nil && i == 0 {
			err = g.probe(ctx, cl, key, st, res.ETag)
		}
		done()
		if err != nil {
			return err
		}
		end := time.Now()
		g.hist.finish(key, id, end, true, res.ETag)
		g.prepareProgress(float64(i+1) / float64(g.Keys))
	}
	return nil
}

// probe fails the run early when the server drops the metadata or uses
// ETags that are not MD5, since every read would then stop at that check
// and never reach the staleness check.
func (g *Atomic) probe(ctx context.Context, cl *minio.Client, key string, st atomicStamp, etag string) error {
	info, err := cl.StatObject(ctx, g.Bucket, key, g.StatOpts)
	if err != nil {
		return fmt.Errorf("stat after upload: %w", err)
	}
	if _, ok := ParseAtomicID(info.UserMetadata[atomicMetaKey]); !ok {
		return fmt.Errorf("server did not return the %s metadata written with the object", atomicMetaKey)
	}
	if !g.CheckMD5 || g.PutOpts.ServerSideEncryption != nil {
		return nil
	}
	h := md5.New()
	if _, err := io.Copy(h, newAtomicReader(st, g.BlockSize)); err != nil {
		return err
	}
	if sum := hex.EncodeToString(h.Sum(nil)); sum != etag {
		return fmt.Errorf("server ETag %s is not the MD5 of the body (%s), rerun with --no-etag-md5", etag, sum)
	}
	return nil
}

func (g *Atomic) count(kind string) {
	g.countsMu.Lock()
	g.counts[kind]++
	g.countsMu.Unlock()
}

func (g *Atomic) violation(op *Operation, kind, detail string) {
	g.count(kind)
	op.Err = "atomic " + kind + ": " + op.File + ": " + detail + " [read via " + op.Endpoint + ", seed " + strconv.FormatUint(g.Seed, 10) + "]"
	g.Error(op.Err)
}

// Start will execute the main benchmark.
// Operations should begin executing when the start channel is closed.
func (g *Atomic) Start(ctx context.Context, wait chan struct{}) error {
	var wg sync.WaitGroup
	wg.Add(g.Concurrency)
	c := g.Collector
	if g.AutoTermDur > 0 {
		ctx = c.AutoTerm(ctx, "", g.AutoTermScale, autoTermCheck, autoTermSamples, g.AutoTermDur)
	}
	nonTerm := context.Background()
	total := g.PutWeight + g.GetWeight + g.StatWeight + g.ListWeight
	for i := range g.Concurrency {
		go func(i int) {
			defer wg.Done()
			rcv := c.Receiver()
			rng := g.threadRNG(uint64(i))
			writer := g.writerID(uint64(i))
			var gen uint64
			done := ctx.Done()
			<-wait
			for {
				select {
				case <-done:
					return
				default:
				}
				if g.rpsLimit(ctx) != nil {
					return
				}
				key := g.keyName(rng.Intn(g.Keys))
				pick := rng.Float64() * total
				switch {
				case pick < g.PutWeight:
					gen++
					op := g.put(nonTerm, uint32(i), g.stamp(rng, key, AtomicID{Writer: writer, Gen: gen}), key)
					rcv <- op
					if g.ReadAfterWrite && op.Err == "" {
						rcv <- g.get(nonTerm, uint32(i), key, op.Endpoint)
					}
				case pick < g.PutWeight+g.GetWeight:
					rcv <- g.get(nonTerm, uint32(i), key, "")
				case pick < g.PutWeight+g.GetWeight+g.StatWeight:
					rcv <- g.stat(nonTerm, uint32(i), key)
				default:
					gen += 2
					g.listCycle(nonTerm, uint32(i), rng, AtomicID{Writer: writer, Gen: gen - 1}, rcv)
				}
			}
		}(i)
	}
	wg.Wait()
	g.finalPass()
	return nil
}

func (g *Atomic) put(ctx context.Context, thread uint32, st atomicStamp, key string) Operation {
	id := st.ID
	r := newAtomicReader(st, g.BlockSize)
	client, clDone := g.Client()
	defer clDone()
	op := Operation{
		OpType:   http.MethodPut,
		Thread:   thread,
		Size:     st.Size,
		File:     key,
		ObjPerOp: 1,
		Endpoint: client.EndpointURL().String(),
	}
	op.Start = time.Now()
	g.hist.begin(key, id, op.Start, st.Size, op.Endpoint)
	res, err := client.PutObject(ctx, g.Bucket, key, r, st.Size, g.putOpts(id))
	op.End = time.Now()
	op.LastByte = r.LastByte()
	if err != nil {
		op.Err = err.Error()
		g.Error("upload error: ", err)
	}
	g.hist.finish(key, id, op.End, err == nil, res.ETag)
	return op
}

// clientAvoiding returns a client for a host other than avoid when there
// is more than one host. Rejected clients stay checked out until it returns,
// so a selector that favors idle hosts moves off the avoided one once it
// holds more leases than any other host.
func (g *Atomic) clientAvoiding(avoid string) (*minio.Client, func()) {
	var rejected []func()
	defer func() {
		for _, done := range rejected {
			done()
		}
	}()
	for range 2*g.Concurrency + 2 {
		client, clDone := g.Client()
		if avoid == "" || client.EndpointURL().String() != avoid {
			return client, clDone
		}
		rejected = append(rejected, clDone)
	}
	return g.Client()
}

func (g *Atomic) get(ctx context.Context, thread uint32, key, avoid string) Operation {
	client, clDone := g.clientAvoiding(avoid)
	defer clDone()
	op := Operation{
		OpType:   http.MethodGet,
		Thread:   thread,
		File:     key,
		ObjPerOp: 1,
		Endpoint: client.EndpointURL().String(),
	}
	var t uint64
	t, op.Start = g.hist.readBegin()
	defer g.hist.readEnd(t)
	g.verifyGet(ctx, client, &op)
	return op
}

func (g *Atomic) verifyGet(ctx context.Context, client *minio.Client, op *Operation) {
	fbr := firstByteRecorder{}
	v := newAtomicVerifier(g.BlockSize)
	obj, err := client.GetObject(ctx, g.Bucket, op.File, g.GetOpts)
	var info minio.ObjectInfo
	if err == nil {
		fbr.r = obj
		_, err = io.Copy(v, &fbr)
		if err == nil {
			info, err = obj.Stat()
		}
		obj.Close()
	}
	op.FirstByte = fbr.t
	op.End = time.Now()
	op.Size = v.total + int64(v.n)
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			g.violation(op, AtomicMissing, "key written before the run returned NoSuchKey")
			return
		}
		if errors.Is(err, io.ErrUnexpectedEOF) && op.Size > 0 {
			v.finish()
			v.setFault(AtomicLength, "body ended early")
			g.violation(op, v.fault, v.detail+": "+err.Error())
			return
		}
		op.Err = err.Error()
		g.Error("download error: ", err)
		return
	}
	st := v.finish()
	if v.fault == "" && st.KeyHash != atomicKeyHash(op.File) {
		v.setFault(AtomicWrongKey, "body "+st.ID.String()+" was written to a different key")
	}
	if v.fault != "" {
		g.violation(op, v.fault, v.detail+" (ETag "+info.ETag+", meta "+info.UserMetadata[atomicMetaKey]+")")
		return
	}
	metaID, ok := ParseAtomicID(info.UserMetadata[atomicMetaKey])
	if !ok {
		g.violation(op, AtomicMetaMissing, "no "+atomicMetaKey+" metadata on body "+st.ID.String())
		return
	}
	if metaID != st.ID {
		g.violation(op, AtomicMetaMismatch, "metadata says "+metaID.String()+", body is "+st.ID.String()+g.writtenVia(op.File, metaID, st.ID))
		return
	}
	if etag, _, ok := g.hist.etag(op.File, st.ID); ok && etag != info.ETag {
		g.violation(op, AtomicETagMismatch, fmt.Sprintf("body is %s acked with ETag %s, GET returned ETag %s", st.ID, etag, info.ETag))
		return
	}
	if g.CheckMD5 && g.PutOpts.ServerSideEncryption == nil && v.md5Hex() != info.ETag {
		g.violation(op, AtomicETagMD5, fmt.Sprintf("ETag %s, body %s has MD5 %s", info.ETag, st.ID, v.md5Hex()))
		return
	}
	if kind, detail := g.hist.check(op.File, st.ID, op.Start); kind != "" {
		g.violation(op, kind, detail+g.writtenVia(op.File, st.ID))
	}
}

func (g *Atomic) writtenVia(key string, ids ...AtomicID) string {
	var parts []string
	for _, id := range ids {
		if ep := g.hist.endpoint(key, id); ep != "" {
			parts = append(parts, id.String()+" written via "+ep)
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return " (" + strings.Join(parts, ", ") + ")"
}

func (g *Atomic) stat(ctx context.Context, thread uint32, key string) Operation {
	client, clDone := g.Client()
	defer clDone()
	op := Operation{
		OpType:   "STAT",
		Thread:   thread,
		File:     key,
		ObjPerOp: 1,
		Endpoint: client.EndpointURL().String(),
	}
	var t uint64
	t, op.Start = g.hist.readBegin()
	defer g.hist.readEnd(t)
	info, err := client.StatObject(ctx, g.Bucket, key, g.StatOpts)
	op.End = time.Now()
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			g.violation(&op, AtomicMissing, "key written before the run returned NoSuchKey")
			return op
		}
		op.Err = err.Error()
		g.Error("stat error: ", err)
		return op
	}
	id, ok := ParseAtomicID(info.UserMetadata[atomicMetaKey])
	if !ok {
		g.violation(&op, AtomicMetaMissing, "no "+atomicMetaKey+" metadata, ETag "+info.ETag)
		return op
	}
	if etag, size, ok := g.hist.etag(key, id); ok {
		if etag != info.ETag {
			g.violation(&op, AtomicETagMismatch, fmt.Sprintf("metadata says %s acked with ETag %s, STAT returned ETag %s", id, etag, info.ETag))
			return op
		}
		if size != info.Size {
			g.violation(&op, AtomicMetaMismatch, fmt.Sprintf("metadata says %s of %d bytes, STAT returned %d", id, size, info.Size))
			return op
		}
	}
	if kind, detail := g.hist.check(key, id, op.Start); kind != "" {
		g.violation(&op, kind, detail)
	}
	return op
}

func (g *Atomic) listDir() string {
	return g.Prefix + "atomic.list/"
}

// listCycle PUTs a new key, overwrites it and deletes it, and after each
// acknowledged request lists the key from a host other than the one that
// took the request. Only this writer touches the key, so no other PUT to it
// can be in flight during a listing.
func (g *Atomic) listCycle(ctx context.Context, thread uint32, rng *rand.Rand, first AtomicID, rcv chan<- Operation) {
	dir := g.listDir() + strconv.FormatUint(first.Writer, 16) + "/"
	key := dir + strconv.FormatUint(first.Gen, 16)
	defer g.hist.forget(key)

	var prev AtomicID
	var prevETag string
	for _, id := range []AtomicID{first, {Writer: first.Writer, Gen: first.Gen + 1}} {
		put := g.put(ctx, thread, g.stamp(rng, key, id), key)
		rcv <- put
		if put.Err != "" {
			return
		}
		etag, size, _ := g.hist.etag(key, id)
		op, objs, release := g.list(ctx, thread, dir, put.Endpoint)
		if op.Err == "" {
			switch obj, ok := objs[key]; {
			case !ok:
				g.violation(&op, AtomicListMissing, fmt.Sprintf("%s acknowledged via %s is not listed", id, put.Endpoint))
			case prevETag != "" && obj.ETag == prevETag:
				g.violation(&op, AtomicListStale, fmt.Sprintf("listed %s, but %s replaced it via %s before the listing started", prev, id, put.Endpoint))
			case obj.ETag != etag:
				g.violation(&op, AtomicListETag, fmt.Sprintf("%s acknowledged via %s with ETag %s, listed with %s", id, put.Endpoint, etag, obj.ETag))
			case obj.Size != size:
				g.violation(&op, AtomicListSize, fmt.Sprintf("%s acknowledged via %s with %d bytes, listed with %d", id, put.Endpoint, size, obj.Size))
			}
		}
		release()
		rcv <- op
		prev, prevETag = id, etag
	}

	del := g.remove(ctx, thread, key)
	rcv <- del
	if del.Err != "" {
		return
	}
	op, objs, release := g.list(ctx, thread, dir, del.Endpoint)
	if _, ok := objs[key]; ok && op.Err == "" {
		g.violation(&op, AtomicListDeleted, fmt.Sprintf("%s deleted via %s is still listed", prev, del.Endpoint))
	}
	release()
	rcv <- op
}

// list returns the listing and a function that ends the read. Call it after
// every check against the listing, so no prune runs before they finish.
func (g *Atomic) list(ctx context.Context, thread uint32, prefix, avoid string) (Operation, map[string]minio.ObjectInfo, func()) {
	client, clDone := g.clientAvoiding(avoid)
	defer clDone()
	op := Operation{
		OpType:   "LIST",
		Thread:   thread,
		File:     prefix,
		Endpoint: client.EndpointURL().String(),
	}
	var t uint64
	t, op.Start = g.hist.readBegin()
	objs := make(map[string]minio.ObjectInfo)
	for obj := range client.ListObjects(ctx, g.Bucket, minio.ListObjectsOptions{Prefix: prefix}) {
		if obj.Err != nil {
			op.Err = obj.Err.Error()
			g.Error("list error: ", obj.Err)
			break
		}
		if op.FirstByte == nil {
			now := time.Now()
			op.FirstByte = &now
		}
		objs[obj.Key] = obj
	}
	op.End = time.Now()
	op.ObjPerOp = len(objs)
	return op, objs, func() { g.hist.readEnd(t) }
}

func (g *Atomic) remove(ctx context.Context, thread uint32, key string) Operation {
	client, clDone := g.Client()
	defer clDone()
	op := Operation{
		OpType:   http.MethodDelete,
		Thread:   thread,
		File:     key,
		ObjPerOp: 1,
		Endpoint: client.EndpointURL().String(),
	}
	op.Start = time.Now()
	err := client.RemoveObject(ctx, g.Bucket, key, minio.RemoveObjectOptions{})
	op.End = time.Now()
	if err != nil {
		op.Err = err.Error()
		g.Error("delete error: ", err)
	}
	return op
}

// finalPass reads every key once all writers have stopped, so the last
// acknowledged state is checked without any write in flight.
func (g *Atomic) finalPass() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	g.UpdateStatus("Verifying final state of all keys")
	rcv := g.Collector.Receiver()
	for i := range g.Keys {
		client, clDone := g.Client()
		op := Operation{OpType: http.MethodGet, ObjPerOp: 1, File: g.keyName(i), Endpoint: client.EndpointURL().String(), Start: time.Now()}
		g.verifyGet(ctx, client, &op)
		clDone()
		if op.Err != "" {
			rcv <- op
		}
	}
	g.summary()
}

func (g *Atomic) summary() {
	g.countsMu.Lock()
	defer g.countsMu.Unlock()
	if len(g.counts) == 0 {
		return
	}
	if g.Custom == nil {
		g.Custom = make(map[string]string, len(g.counts))
	}
	for k, n := range g.counts {
		g.Custom[fmt.Sprintf("atomic.%d.%s", g.ClientIdx, k)] = strconv.FormatInt(n, 10)
	}
	g.Error(violationSummary(g.counts) + " seed=" + strconv.FormatUint(g.Seed, 10))
}

// ClientViolations adds up the violation counts returned by every client.
func (g *Atomic) ClientViolations(custom map[string]string) string {
	counts := make(map[string]int64)
	for k, v := range custom {
		rest, ok := strings.CutPrefix(k, "atomic.")
		_, kind, found := strings.Cut(rest, ".")
		n, err := strconv.ParseInt(v, 10, 64)
		if ok && found && err == nil {
			counts[kind] += n
		}
	}
	if len(counts) == 0 {
		return ""
	}
	return violationSummary(counts)
}

func violationSummary(counts map[string]int64) string {
	kinds := make([]string, 0, len(counts))
	for k := range counts {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	var sb strings.Builder
	for _, k := range kinds {
		fmt.Fprintf(&sb, " %s=%d", k, counts[k])
	}
	return "atomic violations:" + sb.String()
}

// Cleanup deletes the keys this benchmark wrote and nothing else.
func (g *Atomic) Cleanup(ctx context.Context) {
	cl, done := g.Client()
	defer done()
	for i := range g.Keys {
		if err := cl.RemoveObject(ctx, g.Bucket, g.keyName(i), minio.RemoveObjectOptions{}); err != nil {
			g.Error("cleanup: ", err)
		}
	}
	g.deleteAllInBucket(ctx, strings.TrimSuffix(g.listDir(), "/"))
}
