/*
 * Warp (C) 2019-2023 MinIO, Inc.
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
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
)

// Snowball benchmarks snowball upload speed.
type Snowball struct {
	Common
	prefixes map[string]struct{}

	enc        []*zstd.Encoder
	NumObjs    int // Number objects in each snowball.
	WindowSize int
	Duplicate  bool // Duplicate object content.
	Compress   bool // Zstandard compress snowball.
}

// Prepare will create an empty bucket or delete any content already there
// and upload a number of objects.
func (s *Snowball) Prepare(ctx context.Context) error {
	if s.Compress {
		s.enc = make([]*zstd.Encoder, s.Concurrency)
		for i := range s.enc {
			var err error
			s.enc[i], err = zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithWindowSize(s.WindowSize), zstd.WithNoEntropyCompression(true), zstd.WithAllLitEntropyCompression(false))
			if err != nil {
				return err
			}
		}
	}
	s.addCollector()
	s.prefixes = make(map[string]struct{}, s.Concurrency)
	return s.createEmptyBucket(ctx)
}

// Start will execute the main benchmark.
// Operations should begin executing when the start channel is closed.
func (s *Snowball) Start(ctx context.Context, wait chan struct{}) (Operations, error) {
	var wg sync.WaitGroup
	wg.Add(s.Concurrency)
	c := s.Collector
	if s.AutoTermDur > 0 {
		ctx = c.AutoTerm(ctx, http.MethodPut, s.AutoTermScale, autoTermCheck, autoTermSamples, s.AutoTermDur)
	}
	s.prefixes = make(map[string]struct{}, s.Concurrency)

	// Non-terminating context.
	nonTerm := context.Background()

	for i := 0; i < s.Concurrency; i++ {
		src := s.Source()
		s.prefixes[src.Prefix()] = struct{}{}
		go func(i int) {
			var buf bytes.Buffer
			rcv := c.Receiver()
			defer wg.Done()
			opts := s.PutOpts
			opts.UserMetadata = map[string]string{"X-Amz-Meta-Snowball-Auto-Extract": "true"}
			done := ctx.Done()

			<-wait
			for {
				select {
				case <-done:
					return
				default:
				}

				if s.rpsLimit(ctx) != nil {
					return
				}

				buf.Reset()
				w := io.Writer(&buf)
				if s.Compress {
					s.enc[i].Reset(&buf)
					w = s.enc[i]
				}
				obj := src.Object()
				op := Operation{
					OpType:   http.MethodPut,
					Thread:   uint16(i),
					File:     path.Join(obj.Prefix, "snowball.tar"),
					ObjPerOp: s.NumObjs,
				}

				{
					tw := tar.NewWriter(w)
					content, err := io.ReadAll(obj.Reader)
					if err != nil {
						s.Error("obj data error: ", err)
						return
					}

					for i := 0; i < s.NumObjs; i++ {
						err := tw.WriteHeader(&tar.Header{
							Typeflag: tar.TypeReg,
							Name:     fmt.Sprintf("%s/%d.obj", obj.Name, i),
							Size:     obj.Size,
							Mode:     int64(os.ModePerm),
							ModTime:  time.Now(),
						})
						if err != nil {
							s.Error("tar header error: ", err)
							return
						}
						_, err = tw.Write(content)
						if err != nil {
							s.Error("tar write error: ", err)
							return
						}
						op.Size += int64(len(content))
						if !s.Duplicate {
							obj = src.Object()
							content, err = io.ReadAll(obj.Reader)
							if err != nil {
								s.Error("obj data error: ", err)
								return
							}
						}
					}
					tw.Close()
					if s.Compress {
						err := s.enc[i].Close()
						if err != nil {
							s.Error("zstd close error: ", err)
							return
						}
					}
				}
				opts.ContentType = obj.ContentType
				opts.DisableMultipart = true

				client, cldone := s.Client()
				op.Endpoint = client.EndpointURL().String()
				op.Start = time.Now()
				tarLength := int64(buf.Len())
				// fmt.Println(op.Size, "->", tarLength, math.Round(100*float64(tarLength)/float64(op.Size)), "%")
				res, err := client.PutObject(nonTerm, s.Bucket, obj.Name+".tar", &buf, tarLength, opts)
				op.End = time.Now()
				if err != nil {
					s.Error("upload error: ", err)
					op.Err = err.Error()
				}
				obj.VersionID = res.VersionID

				if res.Size != tarLength && op.Err == "" {
					err := fmt.Sprint("short upload. want:", tarLength, ", got:", res.Size)
					if op.Err == "" {
						op.Err = err
					}
					s.Error(err)
				}
				cldone()
				rcv <- op
			}
		}(i)
	}
	wg.Wait()
	return c.Close(), nil
}

// Cleanup deletes everything uploaded to the bucket.
func (s *Snowball) Cleanup(ctx context.Context) {
	if s.Compress {
		for i := range s.enc {
			s.enc[i] = nil
		}
	}
	pf := make([]string, 0, len(s.prefixes))
	for p := range s.prefixes {
		pf = append(pf, p)
	}
	s.deleteAllInBucket(ctx, pf...)
}
