/*
 * Datagen (C) 2025 Signal65 / Futurum Group LLC.
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

// package datagen provides fast, block-based pseudo-random data generators
package generator

import (
    "math"
    "math/rand"
    "runtime"
    "sync"
    "time"
)

const (
    BLK_SIZE = 512
    HALF_BLK = BLK_SIZE / 2
    MOD_SIZE = 32
)

var (
    // baseBlock is a single random block used for generateRandomData
    baseBlock = func() []byte {
        b := make([]byte, BLK_SIZE)
        rand.Seed(time.Now().UnixNano())
        rand.Read(b)
        return b
    }()

    // aBaseBlock is the template for controlled unique-block cloning
    aBaseBlock = func() []byte {
        b := make([]byte, BLK_SIZE)
        rand.Seed(time.Now().UnixNano() + 1)
        rand.Read(b)
        return b
    }()
)

// GenerateRandomData returns a size-byte slice where each 512 B block
// is seeded from baseBlock, then has its first and last MOD_SIZE bytes
// randomized to avoid dedupe/compress optimizations.
func GenerateRandomData(size int) []byte {
    if size < BLK_SIZE {
        size = BLK_SIZE
    }
    data := make([]byte, size)

    // Fill from baseBlock
    for offset := 0; offset < size; offset += BLK_SIZE {
        end := offset + BLK_SIZE
        if end > size {
            end = size
        }
        copy(data[offset:end], baseBlock[:end-offset])
    }

    rng := rand.New(rand.NewSource(time.Now().UnixNano()))
    // Mutate each block
    for offset := 0; offset < size; offset += BLK_SIZE {
        blockEnd := offset + BLK_SIZE
        if blockEnd > size {
            blockEnd = size
        }
        blockSize := blockEnd - offset

        // 1) first MOD_SIZE bytes
        firstLen := MOD_SIZE
        if blockSize < firstLen {
            firstLen = blockSize
        }
        rng.Read(data[offset : offset+firstLen])

        // 2) last MOD_SIZE bytes if block > HALF_BLK
        if blockSize > HALF_BLK {
            rng.Read(data[blockEnd-MOD_SIZE : blockEnd])
        }
    }

    return data
}

// GenerateControlledData returns a size-byte slice with roughly 1/dedup
// unique blocks and compressibility ~compress, using the same block-
// distribution strategy as the Rust version.
func GenerateControlledData(size, dedup, compress int) []byte {
    if size < BLK_SIZE {
        size = BLK_SIZE
    }
    blockSize := BLK_SIZE
    nblocks := (size + blockSize - 1) / blockSize

    // dedup factor → number of unique blocks
    if dedup <= 0 {
        dedup = 1
    }
    var uniqueBlocks int
    if dedup > 1 {
        uniqueBlocks = int(math.Round(float64(nblocks) / float64(dedup)))
        if uniqueBlocks < 1 {
            uniqueBlocks = 1
        }
    } else {
        uniqueBlocks = nblocks
    }

    // compression fraction = (compress-1)/compress
    fNum, fDen := 0, 1
    if compress > 1 {
        fNum, fDen = compress-1, compress
    }
    floorLen := (fNum * blockSize) / fDen
    rem := (fNum * blockSize) % fDen

    // Bresenham-style zero-prefix lengths
    constLens := make([]int, uniqueBlocks)
    errAcc := 0
    for i := 0; i < uniqueBlocks; i++ {
        errAcc += rem
        if errAcc >= fDen {
            errAcc -= fDen
            constLens[i] = floorLen + 1
        } else {
            constLens[i] = floorLen
        }
    }

    // 1) Build unique blocks (cloning + small mutations)
    unique := make([][]byte, uniqueBlocks)
    workers := runtime.NumCPU()
    jobs := make(chan int, uniqueBlocks)
    var wg sync.WaitGroup
    wg.Add(workers)
    for w := 0; w < workers; w++ {
        go func() {
            defer wg.Done()
            rng := rand.New(rand.NewSource(time.Now().UnixNano()))
            for i := range jobs {
                // cheap clone of aBaseBlock
                block := make([]byte, blockSize)
                copy(block, aBaseBlock)

                // zero-prefix
                cl := constLens[i]
                for j := 0; j < cl; j++ {
                    block[j] = 0
                }

                // mutate first MOD_SIZE of remainder
                regionStart := cl
                regionLen := blockSize - regionStart
                mlen := MOD_SIZE
                if regionLen < mlen {
                    mlen = regionLen
                }
                rng.Read(block[regionStart : regionStart+mlen])

                // optionally mutate a second chunk
                secondOffset := HALF_BLK
                if regionStart > secondOffset {
                    secondOffset = regionStart
                }
                if secondOffset+mlen <= blockSize {
                    rng.Read(block[secondOffset : secondOffset+mlen])
                }

                unique[i] = block
            }
        }()
    }
    for i := 0; i < uniqueBlocks; i++ {
        jobs <- i
    }
    close(jobs)
    wg.Wait()

    // 2) Round-robin copy into final buffer
    totalSize := nblocks * blockSize
    data := make([]byte, totalSize)

    jobs2 := make(chan int, nblocks)
    wg.Add(workers)
    for w := 0; w < workers; w++ {
        go func() {
            defer wg.Done()
            for i := range jobs2 {
                src := unique[i%uniqueBlocks]
                start := i * blockSize
                copy(data[start:start+blockSize], src)
            }
        }()
    }
    for i := 0; i < nblocks; i++ {
        jobs2 <- i
    }
    close(jobs2)
    wg.Wait()

    // trim to exact size
    return data[:size]
}

