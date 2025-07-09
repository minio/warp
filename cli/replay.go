/*
 * This file (C) 2025 Signal65 / Futurum Group LLC.
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
/*  (C) 2025 Signal65 / Futurum Group LLC.
    SPDX-License-Identifier: AGPL-3.0-or-later                                   */

package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/minio/cli"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/russfellows/warp-replay/pkg/generator"
)

var replayCmd = cli.Command{
	Name:   "replay",
	Usage:  "Replay an object-storage workload from a WARP output file.",
	Action: mainReplay,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "file",
			Usage: "Path to the WARP output file (.csv or .csv.zst) to replay (required)",
		},
		cli.StringFlag{
			Name:  "bucket",
			Usage: "S3 bucket to use for all replay operations (required)",
		},
		cli.StringFlag{
			Name:   "access-key",
			Usage:  "S3 access key",
			EnvVar: "MINIO_ACCESS_KEY",
		},
		cli.StringFlag{
			Name:   "secret-key",
			Usage:  "S3 secret key",
			EnvVar: "MINIO_SECRET_KEY",
		},
		cli.BoolFlag{
			Name:  "insecure",
			Usage: "Disable TLS certificate verification",
		},
		cli.IntFlag{
			Name:  "dedupe",
			Usage: "Dedupe factor for generated data (higher → *more* repetition)",
			Value: 4,
		},
		cli.IntFlag{
			Name:  "compress",
			Usage: "Compression factor for generated data (higher → easier to compress)",
			Value: 2,
		},
		cli.BoolFlag{
			Name:  "log-warp-ops",
			Usage: "Generate a WARP-style compressed CSV containing the replayed operations",
		},
	},
}

/* column indexes in the original WARP trace */
const (
	colIdx = iota
	colThread
	colOp
	colClientID
	colNObjects
	colBytes
	colEndpoint
	colFile
	colError
	colStart
	colFirstByte
	colEnd
	colDuration
)

/* A single operation from the input trace */
type warpLogEntry struct {
	Index    string
	Thread   string
	Op       string
	Bytes    int64
	Endpoint string
	Bucket   string
	Object   string
	Start    time.Time
}

func mainReplay(c *cli.Context) error {
	/* ---------- validate CLI ---------- */
	replayFile := c.String("file")
	if replayFile == "" {
		return cli.NewExitError("Error: --file flag is required", 1)
	}
	replayBucket := c.String("bucket")
	if replayBucket == "" {
		return cli.NewExitError("Error: --bucket flag is required", 1)
	}
	accessKey, secretKey := c.String("access-key"), c.String("secret-key")
	if accessKey == "" || secretKey == "" {
		return cli.NewExitError("Error: supply S3 credentials via flags or env vars", 1)
	}

	dedupeFactor := c.Int("dedupe")
	compressFactor := c.Int("compress")
	insecureTLS := c.Bool("insecure")
	logWarpOps := c.Bool("log-warp-ops")

	/* ---------- logfile plumbing ---------- */
	logName := fmt.Sprintf("warp-replay-%s.log", time.Now().Format("20060102-150405"))
	logFH, err := os.OpenFile(logName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return cli.NewExitError(fmt.Sprintf("cannot create logfile: %v", err), 1)
	}
	defer logFH.Close()
	log.SetOutput(io.MultiWriter(os.Stdout, logFH))

	/* ---------- optional WARP-style CSV writer ---------- */
	var (
		csvChan   chan []string
		csvWG     sync.WaitGroup
		csvCloser func()
	)
	if logWarpOps {
		outName := fmt.Sprintf("warp-replay-ops-%s.csv.zst", time.Now().Format("20060102-150405"))
		outFH, err := os.Create(outName)
		if err != nil {
			return cli.NewExitError(fmt.Sprintf("cannot create ops log: %v", err), 1)
		}
		zw, _ := zstd.NewWriter(outFH)
		writer := csv.NewWriter(zw)
		writer.Comma = '\t'
		/* header identical to WARP */
		header := []string{
			"idx", "thread", "op", "client_id", "n_objects", "bytes",
			"endpoint", "file", "error", "start", "first_byte", "end", "duration_ns",
		}
		_ = writer.Write(header)
		writer.Flush()

		csvChan = make(chan []string, 1000)
		csvWG.Add(1)
		go func() {
			defer csvWG.Done()
			for rec := range csvChan {
				_ = writer.Write(rec)
			}
			writer.Flush()
			zw.Close()
			outFH.Close()
		}()
		csvCloser = func() { close(csvChan); csvWG.Wait() }
	} else {
		csvCloser = func() {}
	}

	/* ---------- open & decode trace ---------- */
	f, err := os.Open(replayFile)
	if err != nil {
		return cli.NewExitError(fmt.Sprintf("cannot open trace: %v", err), 1)
	}
	defer f.Close()

	var reader io.Reader = f
	if strings.HasSuffix(strings.ToLower(replayFile), ".zst") {
		dec, err := zstd.NewReader(f)
		if err != nil {
			return cli.NewExitError(fmt.Sprintf("zstd reader: %v", err), 1)
		}
		defer dec.Close()
		reader = dec
	}

	cr := csv.NewReader(bufio.NewReader(reader))
	cr.Comma = '\t'
	cr.Comment = '#'

	/* discard header row */
	if _, err = cr.Read(); err != nil {
		return cli.NewExitError(fmt.Sprintf("bad trace header: %v", err), 1)
	}

	/* ---------- bookkeeping ---------- */
	var (
		firstTraceTime time.Time      // absolute time of very first op in the trace
		replayEpoch    = time.Now()   // wall clock when we started replay
		s3Clients      = map[string]*minio.Client{}
		opWG           sync.WaitGroup // waits for all executeOperation goroutines
	)

	log.Println("Starting replay…")

	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return cli.NewExitError(fmt.Sprintf("trace read error: %v", err), 1)
		}

		entry, err := parseLogRecord(rec)
		if err != nil {
			log.Printf("skip bad record: %v", err)
			continue
		}
		entry.Bucket = replayBucket

		if firstTraceTime.IsZero() {
			firstTraceTime = entry.Start
			replayEpoch = time.Now()
		}

		/* schedule using an absolute timeline */
		targetTime := replayEpoch.Add(entry.Start.Sub(firstTraceTime))
		if delay := time.Until(targetTime); delay > 0 {
			time.Sleep(delay)
		}

		/* one MinIO client per unique endpoint */
		cl, ok := s3Clients[entry.Endpoint]
		if !ok {
			cl, err = newS3Client(entry.Endpoint, accessKey, secretKey, insecureTLS)
			if err != nil {
				log.Printf("cannot create S3 client for %s: %v – skipping op",
					entry.Endpoint, err)
				continue
			}
			s3Clients[entry.Endpoint] = cl
		}

		opWG.Add(1)
		go executeOperation(context.Background(), cl, entry,
			dedupeFactor, compressFactor, csvChan, &opWG)
	}

	/* wait for all PUT/GET/… goroutines */
	opWG.Wait()
	csvCloser() // flush CSV & close file if enabled
	log.Println("Replay complete.")
	return nil
}

/* ---------- helpers ---------- */

func parseLogRecord(r []string) (*warpLogEntry, error) {
	if len(r) < colStart+1 {
		return nil, fmt.Errorf("invalid record length %d", len(r))
	}
	if r[colStart] == "" {
		return nil, fmt.Errorf("missing start time")
	}
	b, err := strconv.ParseInt(r[colBytes], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("bytes field: %w", err)
	}
	st, err := time.Parse(time.RFC3339Nano, r[colStart])
	if err != nil {
		return nil, fmt.Errorf("time parse: %w", err)
	}
	return &warpLogEntry{
		Index:    r[colIdx],
		Thread:   r[colThread],
		Op:       r[colOp],
		Bytes:    b,
		Endpoint: r[colEndpoint],
		Object:   r[colFile],
		Start:    st,
	}, nil
}

func newS3Client(ep, ak, sk string, insecure bool) (*minio.Client, error) {
	ep = strings.TrimPrefix(strings.TrimPrefix(ep, "https://"), "http://")
	return minio.New(ep, &minio.Options{
		Creds:  credentials.NewStaticV4(ak, sk, ""),
		Secure: !insecure,
	})
}

func executeOperation(
	ctx context.Context,
	cl *minio.Client,
	e *warpLogEntry,
	dedupe, comp int,
	logChan chan<- []string,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	start := time.Now()
	var firstByte time.Time
	var errStr string

	switch e.Op {
	case "GET":
		obj, err := cl.GetObject(ctx, e.Bucket, e.Object, minio.GetObjectOptions{})
		if err == nil {
			buf := make([]byte, 1)
			_, _ = obj.Read(buf) // ensure at least one byte ⇒ firstByte timestamp
			firstByte = time.Now()
			_ = obj.Close()
		}
		if err != nil {
			errStr = err.Error()
		}
	case "STAT":
		_, err := cl.StatObject(ctx, e.Bucket, e.Object, minio.StatObjectOptions{})
		if err != nil {
			errStr = err.Error()
		}
	case "LIST":
		// list all objects under the given prefix (no first_byte)
		listOpts := minio.ListObjectsOptions{
			Prefix:    e.Object,
			Recursive: true,
		}
		for info := range cl.ListObjects(ctx, e.Bucket, listOpts) {
			if info.Err != nil {
				errStr = info.Err.Error()
				break
			}
		}
	case "PUT":
		data := generator.GenerateControlledData(int(e.Bytes), dedupe, comp)
		_, err := cl.PutObject(ctx, e.Bucket, e.Object,
			bytes.NewReader(data), e.Bytes, minio.PutObjectOptions{})
		if err != nil {
			errStr = err.Error()
		}
	case "DELETE":
		err := cl.RemoveObject(ctx, e.Bucket, e.Object, minio.RemoveObjectOptions{})
		if err != nil {
			errStr = err.Error()
		}
	default:
		log.Printf("unknown op %q – skipped", e.Op)
		return
	}

	end := time.Now()
	duration := end.Sub(start)

	// only emit first_byte on GET
	firstByteStr := ""
	if e.Op == "GET" && !firstByte.IsZero() {
		firstByteStr = firstByte.Format(time.RFC3339Nano)
	}
	
	if logChan != nil {
		logChan <- []string{
			e.Index,
			e.Thread,
			e.Op,
			"replay",           // client_id – arbitrary
			"1",                // n_objects
			strconv.FormatInt(e.Bytes, 10),
			cl.EndpointURL().String(),
			e.Object,
			errStr,
			start.Format(time.RFC3339Nano),
			//firstByte.Format(time.RFC3339Nano),
			firstByteStr,
			end.Format(time.RFC3339Nano),
			//duration.String(),		// This emits milliseconds
			strconv.FormatInt(duration.Nanoseconds(), 10), // ← emit ns, not ms	
		}
	}

	if errStr != "" {
		log.Printf("%s %s/%s failed: %s", e.Op, e.Bucket, e.Object, errStr)
	}
}

/* ---------- log helper ---------- */

func init() {
	/* make logfile land next to binary instead of CWD if CWD is unwritable */
	exe, _ := os.Executable()
	_ = os.MkdirAll(filepath.Dir(exe), 0o755)
}

