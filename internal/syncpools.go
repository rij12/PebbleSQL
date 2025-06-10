package main

import (
	"fmt"
	"runtime"
	"sync"
	"time"
)

const (
	goroutines = 10
	iterations = 100000
	size       = 500000 // size of the slice
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	fmt.Println("Running sync.Pool benchmark...")
	poolDuration := runWithPool()

	fmt.Println("Running fresh allocation benchmark...")
	freshDuration := runWithFreshAlloc()

	fmt.Printf("\nResults:\n")
	fmt.Printf("  sync.Pool:         %v\n", poolDuration)
	fmt.Printf("  Fresh Allocation:  %v\n", freshDuration)
}

func runWithPool() time.Duration {
	var pool = sync.Pool{
		New: func() any {
			return make([]int, size)
		},
	}

	var wg sync.WaitGroup
	wg.Add(goroutines)
	start := time.Now()

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				buf := pool.Get().([]int)
				buf[0] = 1
				pool.Put(buf)
			}
		}()
	}

	wg.Wait()
	return time.Since(start)
}

func runWithFreshAlloc() time.Duration {
	var wg sync.WaitGroup
	wg.Add(goroutines)
	start := time.Now()

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				buf := make([]int, size)
				buf[0] = 1
			}
		}()
	}

	wg.Wait()
	return time.Since(start)
}
