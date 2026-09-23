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
	"crypto/md5"
	"encoding/hex"
	"io"
	"math/rand"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

const testBlock = 4096

func atomicBody(t *testing.T, st atomicStamp) []byte {
	t.Helper()
	b, err := io.ReadAll(newAtomicReader(st, testBlock))
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(b)) != st.Size {
		t.Fatalf("reader produced %d bytes, want %d", len(b), st.Size)
	}
	return b
}

func verify(body []byte, chunk int) *atomicVerifier {
	v := newAtomicVerifier(testBlock)
	for len(body) > 0 {
		n := min(chunk, len(body))
		v.Write(body[:n])
		body = body[n:]
	}
	v.finish()
	return v
}

func TestAtomicVerifier(t *testing.T) {
	key := "atomic-0"
	a := atomicStamp{ID: AtomicID{Writer: 1, Gen: 7}, Size: 10*testBlock + 123, KeyHash: atomicKeyHash(key)}
	b := atomicStamp{ID: AtomicID{Writer: 2, Gen: 9}, Size: 10*testBlock + 123, KeyHash: atomicKeyHash(key)}
	bodyA, bodyB := atomicBody(t, a), atomicBody(t, b)

	spliced := append(append([]byte{}, bodyA[:5*testBlock]...), bodyB[5*testBlock:]...)
	unaligned := append(append([]byte{}, bodyA[:5*testBlock+100]...), bodyB[5*testBlock+100:]...)
	flipped := append([]byte{}, bodyA...)
	flipped[3*testBlock+999] ^= 1
	longer := atomicBody(t, atomicStamp{ID: a.ID, Size: a.Size + testBlock, KeyHash: a.KeyHash})

	tests := []struct {
		name string
		body []byte
		want string
	}{
		{"whole", bodyA, ""},
		{"spliced at block", spliced, AtomicTorn},
		{"spliced inside block", unaligned, AtomicCorrupt},
		{"bit flip", flipped, AtomicCorrupt},
		{"truncated", bodyA[:len(bodyA)-1], AtomicLength},
		{"truncated at block", bodyA[:4*testBlock], AtomicLength},
		{"extended", append(append([]byte{}, bodyA...), 0), AtomicLength},
		{"stamp says shorter", longer[:a.Size], ""},
		{"no stamp", make([]byte, 2*testBlock), AtomicCorrupt},
		{"empty", nil, AtomicLength},
	}
	for _, tc := range tests {
		for _, chunk := range []int{1, 1000, testBlock, 1 << 20} {
			if tc.name == "stamp says shorter" {
				continue
			}
			v := verify(tc.body, chunk)
			if v.fault != tc.want {
				t.Errorf("%s (chunk %d): fault %q (%s), want %q", tc.name, chunk, v.fault, v.detail, tc.want)
			}
		}
	}

	v := verify(longer[:a.Size], testBlock)
	if v.fault != AtomicLength {
		t.Errorf("prefix of a longer write: fault %q, want %q", v.fault, AtomicLength)
	}

	v = verify(bodyA, 777)
	sum := md5.Sum(bodyA)
	if v.md5Hex() != hex.EncodeToString(sum[:]) {
		t.Errorf("md5 %s, want %x", v.md5Hex(), sum)
	}
	if st := v.first; st == nil || st.ID != a.ID || st.KeyHash != atomicKeyHash(key) {
		t.Errorf("stamp %+v, want %+v", st, a)
	}
	if v.first.KeyHash == atomicKeyHash("atomic-1") {
		t.Error("distinct keys hash equal")
	}
}

func TestAtomicReaderSeek(t *testing.T) {
	st := atomicStamp{ID: AtomicID{Writer: 3, Gen: 1}, Size: 3*testBlock + 5}
	want := atomicBody(t, st)
	r := newAtomicReader(st, testBlock)
	io.CopyN(io.Discard, r, 2*testBlock+17)
	if _, err := r.Seek(testBlock-3, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(r)
	if string(got) != string(want[testBlock-3:]) {
		t.Fatal("content after seek differs")
	}
}

func TestAtomicStamp(t *testing.T) {
	st := atomicStamp{ID: AtomicID{Writer: 5<<32 | 3, Gen: 42}, Size: 1234, KeyHash: 99}
	blk := make([]byte, testBlock)
	st.fillBlock(blk, 6)
	got, idx, ok := parseAtomicHeader(blk)
	if !ok || got != st || idx != 6 {
		t.Fatalf("parsed %+v idx %d ok %v", got, idx, ok)
	}
	blk[10] ^= 0x80
	if _, _, ok := parseAtomicHeader(blk); ok {
		t.Fatal("corrupted header accepted")
	}
	id, ok := ParseAtomicID(st.ID.String())
	if !ok || id != st.ID {
		t.Fatalf("round trip %v -> %v", st.ID, id)
	}
	for _, s := range []string{"", "1", "x.1", "1.y", "1.2.3"} {
		if _, ok := ParseAtomicID(s); ok {
			t.Errorf("ParseAtomicID(%q) accepted", s)
		}
	}
}

func TestAtomicHistory(t *testing.T) {
	base := time.Unix(1000, 0)
	at := func(ms int) time.Time { return base.Add(time.Duration(ms) * time.Millisecond) }
	const owner = 5
	w := func(thread, gen uint64) AtomicID { return AtomicID{Writer: owner<<32 | thread, Gen: gen} }
	const k = "key"

	h := newAtomicHistory(owner)
	h.begin(k, w(1, 1), at(0), 10, "")
	h.finish(k, w(1, 1), at(10), true, "e1")
	h.begin(k, w(2, 1), at(20), 10, "")
	h.finish(k, w(2, 1), at(30), true, "e2")
	h.begin(k, w(3, 1), at(5), 10, "")
	h.finish(k, w(3, 1), at(50), true, "e3")
	h.begin(k, w(4, 1), at(60), 10, "")
	h.begin(k, w(5, 1), at(1), 10, "")
	h.finish(k, w(5, 1), at(2), false, "")

	tests := []struct {
		name      string
		id        AtomicID
		readStart time.Time
		want      string
	}{
		{"latest before any overwrite", w(1, 1), at(15), ""},
		{"overwritten before read", w(1, 1), at(31), AtomicStale},
		{"read overlaps overwrite", w(1, 1), at(25), ""},
		{"concurrent writes, either may win", w(3, 1), at(31), ""},
		{"overlapping write wins", w(2, 1), at(51), ""},
		{"in flight", w(4, 1), at(0), ""},
		{"failed write may land", w(5, 1), at(1000), ""},
		{"never written", w(1, 99), at(40), AtomicPhantom},
		{"other client", AtomicID{Writer: 6<<32 | 1, Gen: 1}, at(40), ""},
	}
	for _, tc := range tests {
		if got, detail := h.check(k, tc.id, tc.readStart); got != tc.want {
			t.Errorf("%s: %q (%s), want %q", tc.name, got, detail, tc.want)
		}
	}
	if etag, size, ok := h.etag(k, w(2, 1)); !ok || etag != "e2" || size != 10 {
		t.Errorf("etag %q %d %v", etag, size, ok)
	}
	if _, _, ok := h.etag(k, w(4, 1)); ok {
		t.Error("etag of in-flight write reported")
	}
}

func TestAtomicHistoryPrune(t *testing.T) {
	base := time.Unix(1000, 0)
	at := func(ms int) time.Time { return base.Add(time.Duration(ms) * time.Millisecond) }
	const owner = 1
	id := func(gen uint64) AtomicID { return AtomicID{Writer: owner<<32 | 7, Gen: gen} }
	const k = "key"

	h := newAtomicHistory(owner)
	slow := h.readBeginAt(at(15))
	for g := uint64(1); g <= 100; g++ {
		ms := int(g) * 10
		h.begin(k, id(g), at(ms), 1, "")
		h.finish(k, id(g), at(ms+5), true, "")
	}
	if got, _ := h.check(k, id(1), at(15)); got != "" {
		t.Fatalf("read active since before the overwrite flagged %q", got)
	}
	if got, _ := h.check(k, id(2), at(15)); got != "" {
		t.Fatalf("write overlapping an active read flagged %q", got)
	}
	h.readEnd(slow)

	h.begin(k, id(101), at(1010), 1, "")
	h.finish(k, id(101), at(1015), true, "")
	for g := uint64(102); g <= 140; g++ {
		ms := int(g) * 10
		h.begin(k, id(g), at(ms), 1, "")
		h.finish(k, id(g), at(ms+5), true, "")
	}
	if n := len(h.keys[k].writes); n > 40 {
		t.Fatalf("%d writes retained after pruning", n)
	}
	for _, g := range []uint64{1, 50, 100} {
		if got, _ := h.check(k, id(g), at(2000)); got != AtomicStale {
			t.Errorf("pruned gen %d: %q, want %q", g, got, AtomicStale)
		}
	}
	if got, _ := h.check(k, id(140), at(2000)); got != "" {
		t.Errorf("latest write flagged %q", got)
	}
	if got, _ := h.check(k, id(500), at(2000)); got != AtomicPhantom {
		t.Errorf("unissued gen: %q, want %q", got, AtomicPhantom)
	}
}

func TestAtomicCheckListed(t *testing.T) {
	base := time.Unix(1000, 0)
	at := func(ms int) time.Time { return base.Add(time.Duration(ms) * time.Millisecond) }
	const owner = 2
	id := func(gen uint64) AtomicID { return AtomicID{Writer: owner<<32 | 1, Gen: gen} }
	const k = "key"

	h := newAtomicHistory(owner)
	h.begin(k, id(1), at(0), 100, "http://a")
	h.finish(k, id(1), at(10), true, "e1")
	h.begin(k, id(2), at(20), 200, "http://b")
	h.finish(k, id(2), at(30), true, "e2")
	h.begin(k, id(3), at(40), 300, "http://c")

	tests := []struct {
		name  string
		etag  string
		size  int64
		start time.Time
		want  string
	}{
		{"current", "e2", 200, at(35), ""},
		{"replaced before listing", "e1", 100, at(35), AtomicListStale},
		{"listing overlaps overwrite", "e1", 100, at(25), ""},
		{"size of another write", "e2", 100, at(35), AtomicListSize},
		{"in flight, ETag unknown", "e3", 300, at(45), ""},
	}
	for _, tc := range tests {
		if got, detail := h.checkListed(k, tc.etag, tc.size, tc.start); got != tc.want {
			t.Errorf("%s: %q (%s), want %q", tc.name, got, detail, tc.want)
		}
	}

	for g := uint64(4); g <= 80; g++ {
		ms := int(g) * 100
		h.begin(k, id(g), at(ms), 1, "")
		h.finish(k, id(g), at(ms+5), true, "e"+strconv.FormatUint(g, 10))
	}
	if _, ok := h.keys[k].writes[id(1)]; ok {
		t.Fatal("write 1 was not pruned")
	}
	if got, _ := h.checkListed(k, "e1", 100, at(100000)); got != AtomicListStale {
		t.Errorf("pruned ETag: %q, want %q", got, AtomicListStale)
	}
	h.forget(k)
	if _, ok := h.keys[k]; ok {
		t.Error("forget kept the key")
	}
}

func TestAtomicSeed(t *testing.T) {
	sizes := func(g *Atomic, thread uint64) []int64 {
		rng := g.threadRNG(thread)
		out := make([]int64, 8)
		for i := range out {
			out[i] = g.stamp(rng, "k", AtomicID{}).Size
		}
		return out
	}
	sizeFn := func(rng *rand.Rand) int64 { return 1 + rng.Int63n(1<<20) }
	a := &Atomic{Seed: 42, SizeFn: sizeFn}
	b := &Atomic{Seed: 42, SizeFn: sizeFn}
	if !slices.Equal(sizes(a, 3), sizes(b, 3)) {
		t.Fatal("same seed and thread gave different sequences")
	}
	for name, other := range map[string][]int64{
		"thread": sizes(a, 4),
		"seed":   sizes(&Atomic{Seed: 43, SizeFn: sizeFn}, 3),
		"client": sizes(&Atomic{Seed: 42, SizeFn: sizeFn, Common: Common{ClientIdx: 1}}, 3),
	} {
		if slices.Equal(sizes(a, 3), other) {
			t.Errorf("different %s gave the same sequence", name)
		}
	}
	if got := (&Atomic{SizeFn: func(*rand.Rand) int64 { return 1 }}).stamp(rand.New(rand.NewSource(1)), "k", AtomicID{}).Size; got != atomicHeaderSize {
		t.Errorf("size below the stamp was not raised: %d", got)
	}
}

func TestAtomicClientAvoiding(t *testing.T) {
	hosts := []string{"a:9000", "b:9000"}
	clients := make([]*minio.Client, 0, len(hosts))
	for _, host := range hosts {
		cl, err := minio.New(host, &minio.Options{})
		if err != nil {
			t.Fatal(err)
		}
		clients = append(clients, cl)
	}
	running := []int{0, 5}
	leastRunning := func() (*minio.Client, func()) {
		idx := 0
		if running[1] < running[0] {
			idx = 1
		}
		running[idx]++
		return clients[idx], func() { running[idx]-- }
	}
	g := &Atomic{Common: Common{Client: leastRunning}}
	avoid := clients[0].EndpointURL().String()
	cl, done := g.clientAvoiding(avoid)
	if cl.EndpointURL().String() == avoid {
		t.Fatal("returned the avoided host")
	}
	done()
	if running[0] != 0 || running[1] != 5 {
		t.Errorf("clients not released: running %v", running)
	}
	if cl, done := g.clientAvoiding(""); cl != clients[0] {
		t.Error("with nothing to avoid, did not take the idle host")
	} else {
		done()
	}
}

func TestAtomicClientViolations(t *testing.T) {
	custom := map[string]string{"atomic.0.stale": "2", "atomic.1.stale": "3", "atomic.1.torn": "1", "upload-id": "x"}
	if got, want := (&Atomic{}).ClientViolations(custom), "atomic violations: stale=5 torn=1"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if got := (&Atomic{}).ClientViolations(map[string]string{"upload-id": "x"}); got != "" {
		t.Fatalf("got %q, want empty", got)
	}
}
