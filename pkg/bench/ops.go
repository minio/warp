package bench

import (
	"bufio"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"
)

type Operations []Operation

type Operation struct {
	Op     string
	Start  time.Time
	End    time.Time
	Err    string
	Size   int64
	File   string
	Thread uint16
}

type Collector struct {
	ops   Operations
	rcv   chan Operation
	rcvWg sync.WaitGroup
}

func NewCollector() *Collector {
	r := &Collector{
		ops: make(Operations, 0, 10000),
		rcv: make(chan Operation, 1000),
	}
	r.rcvWg.Add(1)
	go func() {
		defer r.rcvWg.Done()
		for op := range r.rcv {
			r.ops = append(r.ops, op)
		}
	}()
	return r
}

func (c *Collector) Receiver() chan<- Operation {
	return c.rcv
}

func (c *Collector) Close() Operations {
	close(c.rcv)
	c.rcvWg.Wait()
	return c.ops
}

func (o Operations) SortByStartTime() {
	sort.Slice(o, func(i, j int) bool {
		return o[i].Start.Before(o[j].Start)
	})
}

func (o Operations) CSV(w io.Writer) error {
	bw := bufio.NewWriter(w)
	_, err := bw.WriteString("idx,thread,op,bytes,file,error,start,end,duration_ns\n")
	if err != nil {
		return err
	}
	for i, op := range o {
		_, err := fmt.Fprintf(bw, "%d,%d,%s,%d,%s,%s,%s,%s,%d\n", i, op.Thread, op.Op, op.Size, op.File, csvEscapeString(op.Err), op.Start.Format(time.RFC3339Nano), op.End.Format(time.RFC3339Nano), op.End.Sub(op.Start)/time.Nanosecond)
		if err != nil {
			return err
		}
	}
	return bw.Flush()
}
