/*
 * Warp (C) 2019-2020 MinIO, Inc.
 * Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

package generator

import (
	"io"
	"io/ioutil"
	"strings"
	"testing"
)

func TestNew(t *testing.T) {
	type args struct {
		opts []Option
	}
	tests := []struct {
		name     string
		args     args
		wantErr  bool
		wantSize int
	}{
		{
			name: "Default",
			args: args{
				opts: nil,
			},
			wantErr:  false,
			wantSize: 1 << 20,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				t.Error(err)
				return
			}
			if got == nil {
				t.Errorf("New() got = nil, want not nil")
				return
			}
			obj := got.Object()
			b, err := io.ReadAll(obj.Reader)
			if err != nil {
				t.Error(err)
				return
			}
			if len(b) != tt.wantSize {
				t.Errorf("New() size = %v, wantSize = %v", len(b), tt.wantSize)
				return
			}
			n, err := obj.Reader.Seek(0, 0)
			if err != nil {
				t.Error(err)
				return
			}
			if n != 0 {
				t.Errorf("Expected 0, got %v", n)
				return
			}
			b, err = ioutil.ReadAll(obj.Reader)
			if err != nil {
				t.Error(err)
				return
			}
			if len(b) != tt.wantSize {
				t.Errorf("New() size = %v, wantSize = %v", len(b), tt.wantSize)
				return
			}
			n, err = obj.Reader.Seek(10, 0)
			if err != nil {
				t.Error(err)
				return
			}
			if n != 10 {
				t.Errorf("Expected 10, got %v", n)
				return
			}
			b, err = io.ReadAll(obj.Reader)
			if err != nil {
				return
			}
			if len(b) != tt.wantSize-10 {
				t.Errorf("New() size = %v, wantSize = %v", len(b), tt.wantSize)
				return
			}
			n, err = obj.Reader.Seek(10, io.SeekCurrent)
			if err != io.ErrUnexpectedEOF {
				t.Errorf("Expected io.ErrUnexpectedEOF, got %v", err)
				return
			}
			if n != 0 {
				t.Errorf("Expected 0, got %v", n)
				return
			}
		})
	}
}

func TestPrefixCombinations(t *testing.T) {
	type objInfo struct {
		threadID int
		objNum   int
		prefix   string
		name     string
	}

	tests := []struct {
		name            string
		customPrefixes  []string
		randomPrefix    int
		wantPrefixCheck func(t *testing.T, objects []objInfo) // Custom validation function
	}{
		{
			name:           "NoFlags_Default",
			customPrefixes: nil,
			randomPrefix:   8,
			wantPrefixCheck: func(t *testing.T, objects []objInfo) {
				// With no custom prefixes and randomPrefix=8, each object gets
				// a random 8-char prefix. Since there's no list to cycle through,
				// each source generates its own random prefix at creation time.
				// Objects from same thread should have same prefix (per-thread random)
				// objects[0] and objects[2] are from thread 0
				// objects[1] and objects[3] are from thread 1
				if objects[0].prefix != objects[2].prefix {
					t.Errorf("Thread 0 objects have different prefixes: %q vs %q", objects[0].prefix, objects[2].prefix)
				}
				if objects[1].prefix != objects[3].prefix {
					t.Errorf("Thread 1 objects have different prefixes: %q vs %q", objects[1].prefix, objects[3].prefix)
				}
				// Check prefix length
				for _, obj := range objects {
					if len(obj.prefix) != 8 {
						t.Errorf("Thread %d, Obj %d: Prefix length = %d, want 8 (prefix=%q)", obj.threadID, obj.objNum, len(obj.prefix), obj.prefix)
					}
				}
			},
		},
		{
			name:           "JustPrefix_abc12",
			customPrefixes: []string{"abc12"},
			randomPrefix:   8,
			wantPrefixCheck: func(t *testing.T, objects []objInfo) {
				// Should be "abc12/<8-random>"
				// Each thread gets different random part, but all objects from same thread share it
				// objects[0] and objects[2] are from thread 0
				// objects[1] and objects[3] are from thread 1
				for _, obj := range objects {
					if !strings.HasPrefix(obj.prefix, "abc12/") {
						t.Errorf("Thread %d, Obj %d: Prefix %q doesn't start with 'abc12/'", obj.threadID, obj.objNum, obj.prefix)
					}
					expectedLen := len("abc12") + 1 + 8 // "abc12" + "/" + 8 random chars
					if len(obj.prefix) != expectedLen {
						t.Errorf("Thread %d, Obj %d: Prefix length = %d, want %d (prefix=%q)", obj.threadID, obj.objNum, len(obj.prefix), expectedLen, obj.prefix)
					}
				}
				// Same thread should have same prefix
				if objects[0].prefix != objects[2].prefix {
					t.Errorf("Thread 0 objects have different prefixes: %q vs %q", objects[0].prefix, objects[2].prefix)
				}
				if objects[1].prefix != objects[3].prefix {
					t.Errorf("Thread 1 objects have different prefixes: %q vs %q", objects[1].prefix, objects[3].prefix)
				}
			},
		},
		{
			name:           "JustNoPrefix",
			customPrefixes: nil,
			randomPrefix:   0,
			wantPrefixCheck: func(t *testing.T, objects []objInfo) {
				// All should have empty prefix
				for _, obj := range objects {
					if obj.prefix != "" {
						t.Errorf("Thread %d, Obj %d: Prefix = %q, want empty", obj.threadID, obj.objNum, obj.prefix)
					}
				}
			},
		},
		{
			name:           "BothNoPrefixAndPrefix_abc12",
			customPrefixes: []string{"abc12"},
			randomPrefix:   0,
			wantPrefixCheck: func(t *testing.T, objects []objInfo) {
				// All should have "abc12" (no random part)
				for _, obj := range objects {
					if obj.prefix != "abc12" {
						t.Errorf("Thread %d, Obj %d: Prefix = %q, want 'abc12'", obj.threadID, obj.objNum, obj.prefix)
					}
				}
			},
		},
		{
			name:           "ThreePrefixes_WithRandom",
			customPrefixes: []string{"p1", "p2", "p3"},
			randomPrefix:   8,
			wantPrefixCheck: func(t *testing.T, objects []objInfo) {
				// With multiple prefixes, each thread independently cycles through p1, p2, p3, wrapping around
				// Thread 0 generates: p1 (n=0), p2 (n=1), p3 (n=2), p1 (n=3 wraps around)
				// Thread 1 generates: p1 (n=0), p2 (n=1), p3 (n=2), p1 (n=3 wraps around)
				// Objects are interleaved in test: T0-O0, T1-O0, T0-O1, T1-O1, T0-O2, T1-O2, T0-O3, T1-O3
				// Expected prefix sequence:        p1,    p1,    p2,    p2,    p3,    p3,    p1,    p1
				expectedPrefixes := []string{"p1/", "p1/", "p2/", "p2/", "p3/", "p3/", "p1/", "p1/"}
				for i, obj := range objects {
					if !strings.HasPrefix(obj.prefix, expectedPrefixes[i]) {
						t.Errorf("Object %d (T%d-O%d): Prefix %q doesn't start with %q", i, obj.threadID, obj.objNum, obj.prefix, expectedPrefixes[i])
					}
					// Check length: "pN" + "/" + 8 random chars = 11
					if len(obj.prefix) != 11 {
						t.Errorf("Object %d (T%d-O%d): Prefix length = %d, want 11 (prefix=%q)", i, obj.threadID, obj.objNum, len(obj.prefix), obj.prefix)
					}
				}

				// Verify that objects from the same thread have the SAME random suffix
				// This preserves the original semantic: random suffix differentiates threads
				// Extract random suffix (everything after "pN/")
				thread0Suffixes := make(map[string]struct{})
				thread1Suffixes := make(map[string]struct{})
				for _, obj := range objects {
					parts := strings.Split(obj.prefix, "/")
					if len(parts) != 2 {
						t.Errorf("Object (T%d-O%d): Prefix %q doesn't have expected format 'pN/random'", obj.threadID, obj.objNum, obj.prefix)
						continue
					}
					randomSuffix := parts[1]
					if obj.threadID == 0 {
						thread0Suffixes[randomSuffix] = struct{}{}
					} else {
						thread1Suffixes[randomSuffix] = struct{}{}
					}
				}
				// Each thread should have exactly ONE unique random suffix
				if len(thread0Suffixes) != 1 {
					t.Errorf("Thread 0 has %d different random suffixes, want 1 (all objects should share same suffix)", len(thread0Suffixes))
				}
				if len(thread1Suffixes) != 1 {
					t.Errorf("Thread 1 has %d different random suffixes, want 1 (all objects should share same suffix)", len(thread1Suffixes))
				}
				// Threads should have different random suffixes (very likely)
				if len(thread0Suffixes) == 1 && len(thread1Suffixes) == 1 {
					var t0Suffix, t1Suffix string
					for s := range thread0Suffixes {
						t0Suffix = s
					}
					for s := range thread1Suffixes {
						t1Suffix = s
					}
					if t0Suffix == t1Suffix {
						t.Logf("Warning: Thread 0 and Thread 1 have same random suffix %q (rare but possible)", t0Suffix)
					}
				}
			},
		},
		{
			name:           "ThreePrefixes_NoRandom",
			customPrefixes: []string{"p1", "p2", "p3"},
			randomPrefix:   0,
			wantPrefixCheck: func(t *testing.T, objects []objInfo) {
				// Each thread independently cycles through p1, p2, p3, wrapping around (no random part)
				// Thread 0 generates: p1 (n=0), p2 (n=1), p3 (n=2), p1 (n=3 wraps around)
				// Thread 1 generates: p1 (n=0), p2 (n=1), p3 (n=2), p1 (n=3 wraps around)
				// Objects are interleaved in test: T0-O0, T1-O0, T0-O1, T1-O1, T0-O2, T1-O2, T0-O3, T1-O3
				// Expected prefix sequence:         p1,    p1,    p2,    p2,    p3,    p3,    p1,    p1
				expectedPrefixes := []string{"p1", "p1", "p2", "p2", "p3", "p3", "p1", "p1"}
				for i, obj := range objects {
					if obj.prefix != expectedPrefixes[i] {
						t.Errorf("Object %d (T%d-O%d): Prefix = %q, want %q", i, obj.threadID, obj.objNum, obj.prefix, expectedPrefixes[i])
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create options for generator
			opts := []Option{
				WithRandomData().Apply(),
				WithSize(1024),
				WithCustomPrefixes(tt.customPrefixes),
				WithPrefixSize(tt.randomPrefix),
			}

			// Simulate 2 threads by creating 2 separate generator sources
			sources := make([]Source, 2)
			for i := 0; i < 2; i++ {
				src, err := New(opts...)
				if err != nil {
					t.Fatalf("Thread %d: New() error = %v", i, err)
				}
				sources[i] = src
			}

			// Generate objects interleaved between threads to test round-robin
			var objects []objInfo

			// For 3-prefix tests, generate 8 objects total (4 per thread) to prove round-robin wraps around
			numObjsPerThread := 2
			if len(tt.customPrefixes) == 3 {
				numObjsPerThread = 4
			}

			// Interleave object generation between threads
			// IMPORTANT: Immediately dereference to copy values, matching real benchmark usage
			// Real benchmarks do: obj := src.Object(); then use *obj or fields immediately
			for objNum := 0; objNum < numObjsPerThread; objNum++ {
				for threadID := 0; threadID < 2; threadID++ {
					objPtr := sources[threadID].Object()
					// Dereference immediately to get value copy, like real benchmarks do
					obj := *objPtr
					objects = append(objects, objInfo{
						threadID: threadID,
						objNum:   objNum,
						prefix:   obj.Prefix,
						name:     obj.Name,
					})
				}
			}

			// Use custom validation function
			tt.wantPrefixCheck(t, objects)

			// Check Name field for all objects
			for _, obj := range objects {
				if obj.prefix == "" {
					// Name should not have a prefix part
					if len(obj.name) == 0 {
						t.Errorf("Thread %d, Obj %d: Name is empty", obj.threadID, obj.objNum)
					}
					// Should not contain "/" anywhere when there's no prefix
					if strings.Contains(obj.name, "/") {
						t.Errorf("Thread %d, Obj %d: Name %q contains '/' but prefix is empty", obj.threadID, obj.objNum, obj.name)
					}
				} else {
					// Name should be Prefix + "/" + object_name
					switch {
					case len(obj.name) <= len(obj.prefix)+1:
						t.Errorf("Thread %d, Obj %d: Name %q too short for prefix %q", obj.threadID, obj.objNum, obj.name, obj.prefix)
					case obj.name[:len(obj.prefix)] != obj.prefix:
						t.Errorf("Thread %d, Obj %d: Name %q doesn't start with prefix %q", obj.threadID, obj.objNum, obj.name, obj.prefix)
					case obj.name[len(obj.prefix)] != '/':
						t.Errorf("Thread %d, Obj %d: Name %q doesn't have '/' after prefix %q", obj.threadID, obj.objNum, obj.name, obj.prefix)
					}
				}
			}
		})
	}
}

func BenchmarkWithRandomData(b *testing.B) {
	type args struct {
		opts []Option
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "64KiB",
			args: args{opts: []Option{WithSize(1 << 16), WithRandomData().Apply()}},
		},
		{
			name: "1MiB",
			args: args{opts: []Option{WithSize(1 << 20), WithRandomData().Apply()}},
		},
		{
			name: "10MiB",
			args: args{opts: []Option{WithSize(10 << 20), WithRandomData().Apply()}},
		},
		{
			name: "10GiB",
			args: args{opts: []Option{WithSize(10 << 30), WithRandomData().Apply()}},
		},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			got, err := New(tt.args.opts...)
			if err != nil {
				b.Errorf("New() error = %v", err)
				return
			}
			obj := got.Object()
			n, err := io.Copy(io.Discard, obj.Reader)
			if err != nil {
				b.Errorf("ioutil error = %v", err)
				return
			}
			b.SetBytes(n)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				obj = got.Object()
				_, err := io.Copy(io.Discard, obj.Reader)
				if err != nil {
					b.Errorf("New() error = %v", err)
					return
				}
			}
		})
	}
}
