package generator

import (
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
			b, err := ioutil.ReadAll(got.Reader())
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
