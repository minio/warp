package generator

import (
	"io"
	"io/ioutil"
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
		{
			name: "CSV",
			args: args{
				opts: []Option{WithCSV().Apply()},
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
				return
			}
			if got == nil {
				t.Errorf("New() got = nil, want not nil")
			}
			t.Log(got)
			obj := got.Object()
			b, err := ioutil.ReadAll(obj.Reader)
			if err != nil {
				return
			}
			if len(b) != tt.wantSize {
				t.Errorf("New() size = %v, wantSize = %v", len(b), tt.wantSize)
				return
			}
			//t.Log(string(b))
		})
	}
}

func BenchmarkWithCSV(b *testing.B) {
	type args struct {
		opts []Option
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "64KB-5x100",
			args: args{opts: []Option{WithSize(1 << 16), WithCSV().Size(5, 100).Apply()}},
		},
		{
			name: "1MB-10x500",
			args: args{opts: []Option{WithSize(1 << 20), WithCSV().Size(10, 500).Apply()}},
		},
		{
			name: "10MB-50x1000",
			args: args{opts: []Option{WithSize(10 << 20), WithCSV().Size(50, 1000).Apply()}},
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
			payload, err := ioutil.ReadAll(obj.Reader)
			b.SetBytes(int64(len(payload)))
			//ioutil.WriteFile(tt.name+".csv", payload, os.ModePerm)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				obj := got.Object()
				_, err := io.Copy(ioutil.Discard, obj.Reader)
				if err != nil {
					b.Errorf("New() error = %v", err)
					return
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
			name: "64KB",
			args: args{opts: []Option{WithSize(1 << 16), WithRandomData().Apply()}},
		},
		{
			name: "1MB",
			args: args{opts: []Option{WithSize(1 << 20), WithRandomData().Apply()}},
		},
		{
			name: "10MB",
			args: args{opts: []Option{WithSize(10 << 20), WithRandomData().Apply()}},
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
			payload, err := ioutil.ReadAll(obj.Reader)
			b.SetBytes(int64(len(payload)))
			//ioutil.WriteFile(tt.name+".bin", payload, os.ModePerm)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				obj = got.Object()
				_, err := io.Copy(ioutil.Discard, obj.Reader)
				if err != nil {
					b.Errorf("New() error = %v", err)
					return
				}
			}
		})
	}
}
