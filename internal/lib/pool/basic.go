package pool

import (
	"fmt"
	"sync"
	"time"
)

type JobResult[T any] struct {
	Output T
	Err    error
}

const defaultWorkers = 5

type Job[T any] func() JobResult[T]

func RunWithWorkers[T any](workerCount int, fn []func() JobResult[T]) <-chan JobResult[T] {
	start := time.Now()

	numberOfWorkers := defaultWorkers
	if workerCount > 0 {
		numberOfWorkers = workerCount
	}

	var wg sync.WaitGroup
	wg.Add(numberOfWorkers)
	jobChannel := make(chan Job[T])
	jobResultChannel := make(chan JobResult[T], len(fn))

	// Start the workers
	for i := 0; i < numberOfWorkers; i++ {
		go worker(i, &wg, jobChannel, jobResultChannel)
	}

	// Send jobs to worker
	for _, job := range fn {
		jobChannel <- job
	}

	close(jobChannel)
	wg.Wait()
	close(jobResultChannel)

	fmt.Printf("Took %s\n", time.Since(start)) // nolint
	return jobResultChannel
}

func worker[T any](_ int, wg *sync.WaitGroup, jobChannel <-chan Job[T], resultChannel chan JobResult[T]) {
	defer wg.Done()
	for job := range jobChannel {
		resultChannel <- job()
	}
}
