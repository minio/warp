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
	"strconv"
	"strings"
	"time"

	"github.com/russfellows/warp-replay/pkg/generator"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/cli"
)

// replayCmd is the command to replay a warp log file.
var replayCmd = cli.Command{
	Name:   "replay",
	Usage:  "Replay an object storage workload from a warp output file.",
	Action: mainReplay,
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "file",
			Usage: "Path to the warp output file (.csv or .csv.zst) to replay (required)",
		},
		cli.StringFlag{
                        Name:  "bucket",
                        Usage: "S3 bucket name to use for all replay operations (required)",
		},
		cli.StringFlag{
			Name:   "access-key",
			Usage:  "S3 access key.",
			EnvVar: "MINIO_ACCESS_KEY",
		},
		cli.StringFlag{
			Name:   "secret-key",
			Usage:  "S3 secret key.",
			EnvVar: "MINIO_SECRET_KEY",
		},
		cli.BoolFlag{
			Name:  "insecure",
			Usage: "Disable TLS certificate verification",
		},
	},
}

const (
	// As per the provided log format
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

// warpLogEntry holds a parsed log entry for a single operation.
type warpLogEntry struct {
	Op       string
	Bytes    int64
	Endpoint string
	Bucket   string
	Object   string
	Start    time.Time
}

// mainReplay is the main function for the replay command.
func mainReplay(c *cli.Context) error {
	replayFile := c.String("file")
	replayBucket := c.String("bucket")
	replayAccessKey := c.String("access-key")
	replaySecretKey := c.String("secret-key")
	replayInsecure := c.Bool("insecure")

	if replayFile == "" {
		return cli.NewExitError("Error: --file flag is required.", 1)
	}

	if replayBucket == "" {
		return cli.NewExitError("Error: --bucket flag is required.", 1)
	}

	if replayAccessKey == "" || replaySecretKey == "" {
		return cli.NewExitError("Error: S3 credentials must be provided via flags or environment variables.", 1)
	}

	f, err := os.Open(replayFile)
	if err != nil {
		return cli.NewExitError(fmt.Sprintf("Failed to open replay file: %v", err), 1)
	}
	defer f.Close()

	var reader io.Reader = f
	if strings.HasSuffix(strings.ToLower(replayFile), ".zst") {
		zstdReader, err := zstd.NewReader(f)
		if err != nil {
			return cli.NewExitError(fmt.Sprintf("Failed to create zstd reader: %v", err), 1)
		}
		defer zstdReader.Close()
		reader = zstdReader
	}

	csvReader := csv.NewReader(bufio.NewReader(reader))
	csvReader.Comma = '\t'
	csvReader.Comment = '#'

	if _, err := csvReader.Read(); err != nil {
		return cli.NewExitError(fmt.Sprintf("Failed to read header: %v", err), 1)
	}

	var previousStartTime time.Time
	clients := make(map[string]*minio.Client)
	opCount := 0

	log.Println("Starting replay...")

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return cli.NewExitError(fmt.Sprintf("Error reading CSV record: %v", err), 1)
		}

		entry, err := parseLogRecord(record)
		if err != nil {
			log.Printf("Skipping malformed record: %v", err)
			continue
		}

		entry.Bucket = replayBucket

		if !previousStartTime.IsZero() {
			delay := entry.Start.Sub(previousStartTime)
			if delay > 0 {
				time.Sleep(delay)
			}
		}
		previousStartTime = entry.Start

		client, ok := clients[entry.Endpoint]
		if !ok {
			client, err = newS3Client(entry.Endpoint, replayAccessKey, replaySecretKey, replayInsecure)
			if err != nil {
				log.Printf("Failed to create S3 client for %s: %v. Skipping operation.", entry.Endpoint, err)
				continue
			}
			clients[entry.Endpoint] = client
		}

		go executeOperation(context.Background(), client, entry)
		opCount++
		if opCount%100 == 0 {
			log.Printf("Dispatched %d operations...", opCount)
		}
	}

	log.Printf("Replay finished. Total operations dispatched: %d", opCount)
	time.Sleep(5 * time.Second) // Give goroutines time to finish
	return nil
}

// parseLogRecord converts a CSV record into a structured log entry.
// Note: bucket comes from the cli --bucket parameter 
func parseLogRecord(record []string) (*warpLogEntry, error) {
    if len(record) < colStart+1 {
        return nil, fmt.Errorf("record has too few columns: %d", len(record))
    }

    // Some lines might have an empty start time (e.g., DELETE ops in some versions)
    if record[colStart] == "" {
        return nil, fmt.Errorf("start time is empty")
    }

	bytes, err := strconv.ParseInt(record[colBytes], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("could not parse bytes: %w", err)
	}

	startTime, err := time.Parse(time.RFC3339Nano, record[colStart])
	if err != nil {
		return nil, fmt.Errorf("could not parse start time: %w", err)
	}

	return &warpLogEntry{
		Op:       record[colOp],
		Bytes:    bytes,
		Endpoint: record[colEndpoint],
		//Bucket:   pathParts[0], // Note: bucket is passed via cli --bucket param
		Object:   record[colFile],
		Start:    startTime,
	}, nil
}

// newS3Client creates a new MinIO client for a given endpoint.
func newS3Client(endpoint, accessKey, secretKey string, insecure bool) (*minio.Client, error) {
	endpoint = strings.TrimPrefix(endpoint, "http://")
	endpoint = strings.TrimPrefix(endpoint, "https://")

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: !insecure,
	})
	return client, err
}

// executeOperation runs the S3 command.
func executeOperation(ctx context.Context, client *minio.Client, entry *warpLogEntry) {
	var err error
	switch entry.Op {
	case "GET", "STAT":
		// We can ignore the returned object as we just want to perform the operation
		_, err = client.GetObject(ctx, entry.Bucket, entry.Object, minio.GetObjectOptions{})
	case "PUT":
        /* Old code, delete 
		nullReader := io.LimitReader(nullReader{}, entry.Bytes)
		_, err = client.PutObject(ctx, entry.Bucket, entry.Object, nullReader, entry.Bytes, minio.PutObjectOptions{})
	*/

	// generate pseudo-random payload that won’t dedupe or compress away
        // you can wire these to flags if you want to tune them at runtime
        const dedupFactor   = 4
        const compressFactor = 2

        data := generator.GenerateControlledData(int(entry.Bytes), dedupFactor, compressFactor)
        reader := bytes.NewReader(data)
        _, err = client.PutObject(ctx,
                entry.Bucket,
                entry.Object,
                reader,
                entry.Bytes,
                minio.PutObjectOptions{},
            )
	case "DELETE":
		err = client.RemoveObject(ctx, entry.Bucket, entry.Object, minio.RemoveObjectOptions{})
	default:
		log.Printf("Skipping unrecognized operation type: '%s'", entry.Op)
		return
	}
	if err != nil {
		errResp := minio.ToErrorResponse(err)
		log.Printf("%s failed for s3://%s/%s: %v", entry.Op, entry.Bucket, entry.Object, errResp)
	}
}

/*
// nullReader is an io.Reader that reads endless zero bytes.
type nullReader struct{}
func (r nullReader) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}
*/

