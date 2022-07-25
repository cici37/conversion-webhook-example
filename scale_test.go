package main

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TODO: TestMain actually runs after benchmarks, so this doesn't help yet
// func TestMain(m *testing.M) {
// 	setupNamespace(emptyNamespace)
// 	setupNamespace(largeDataNamespace)
// 	setupNamespace(largeMetadataNamespace)
// 	os.Exit(m.Run())
// }

func runBenchmark(b *testing.B) {
	// TODO: this is a workaround for go-benchmark not supporting before-benchmark setup
	setupNamespace(emptyNamespace)
	setupNamespace(largeDataNamespace)
	setupNamespace(largeMetadataNamespace)

	// get caller name
	pc, _, _, _ := runtime.Caller(1)
	caller := runtime.FuncForPC(pc).Name()
	setupValidation(strings.Contains(caller, "Validation"))

	var c BenchmarkClient
	if strings.Contains(caller, "Typed") {
		c = mustNewEndpointsBenchmarkClient(getNamespace(caller), getTemplate(caller), getListOptions(caller))
	} else {
		c = mustNewDynamicBenchmarkClient(getGVR(caller), getNamespace(caller), getTemplate(caller), getListOptions(caller))
	}

	if strings.Contains(caller, "CreateLatency") {
		benchmarkCreateLatency(b, c)
	} else if strings.Contains(caller, "CreateThroughput") {
		benchmarkCreateThroughput(b, c)
	} else if strings.Contains(caller, "List") {
		benchmarkList(b, c, testListSize)
	}
}

func benchmarkCreateLatency(b *testing.B, client BenchmarkClient) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.Create(0)
		if err != nil {
			b.Fatalf("failed to create object: %v", err)
		}
	}
}

func Benchmark_CreateLatency_CR_Validation(b *testing.B) {
	runBenchmark(b)
}

func Benchmark_CreateLatency_CR_Validation_LargeData(b *testing.B) {
	runBenchmark(b)
}

func benchmarkCreateThroughput(b *testing.B, client BenchmarkClient) {
	b.ResetTimer()
	var wg sync.WaitGroup
	count := 100
	start := time.Now()
	wg.Add(count)
	for i := 0; i < count; i++ {
		// deep copy i
		idx := i
		go func() {
			_, err := client.Create(idx)
			if err != nil {
				// TODO: b.Fatal doesn't raise
				// b.Fatalf("failed to create object: %v", err)
				panic(err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	fmt.Printf("created %d objects in %v\n", count, time.Now().Sub(start))
}

func Benchmark_CreateThroughput_CR_Validation(b *testing.B) {
	runBenchmark(b)
}

func Benchmark_CreateThroughput_CR_Validation_LargeData(b *testing.B) {
	runBenchmark(b)
}

func benchmarkList(b *testing.B, client BenchmarkClient, listSize int) {
	if err := ensureObjectCount(client, listSize); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.List()
		if err != nil {
			b.Fatalf("failed to list: %v", err)
		}
	}
}

func Benchmark_List_CR_Validation(b *testing.B) {
	runBenchmark(b)
}

func Benchmark_List_CR_Validation_LargeData(b *testing.B) {
	runBenchmark(b)
}

func Benchmark_List_WatchCache_CR_Validation(b *testing.B) {
	runBenchmark(b)
}

func Benchmark_List_WatchCache_CR_Validation_LargeData(b *testing.B) {
	runBenchmark(b)
}

func benchmarkWatch(b *testing.B, client BenchmarkClient, listSize int) {
	watcherCount := 1000
	events := b.N
	var readyWg sync.WaitGroup
	var doneWg sync.WaitGroup
	readyWg.Add(watcherCount)
	doneWg.Add(watcherCount)
	start := time.Now()
	for i := 0; i < watcherCount; i++ {
		go func() {
			watcher, err := client.Watch()
			if err != nil {
				b.Fatalf("failed to watch: %v", err)
			}
			readyWg.Done()
			for j := 0; j < events; j++ {
				<-watcher.ResultChan()
			}
			watcher.Stop()
			doneWg.Done()
		}()
	}
	readyWg.Wait()
	fmt.Printf("created %d watches in %v\n", watcherCount, time.Now().Sub(start))
	start = time.Now()
	b.ResetTimer()
	for i := 0; i < events; i++ {
		go func() {
			client.Create(i)
		}()
	}
	doneWg.Wait()
	fmt.Printf("processed %d watch events in %v\n", watcherCount*events, time.Now().Sub(start))
}

func BenchmarkWatchCR(b *testing.B) {
	c := mustNewDynamicBenchmarkClient(foov1GVR, emptyNamespace, foov1Template, &metav1.ListOptions{})
	benchmarkWatch(b, c, testListSize)
}
