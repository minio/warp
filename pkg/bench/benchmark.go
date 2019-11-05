package bench

type Benchmark struct {
	Prepare func()
	Start   func(sync chan struct{})
	Cleanup func()
}

func Run() {
	
}