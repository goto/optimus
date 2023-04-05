package moderator

import (
	"context"
	"sync"
	"time"
)

type Writer interface {
	Write(messages [][]byte) error
	Close() error
}

type Worker struct {
	mu       sync.Mutex
	messages [][]byte

	batchInterval time.Duration
	wg            sync.WaitGroup
	writer        Writer
}

func (w *Worker) Run(ctx context.Context) {
	defer w.wg.Done()
	for {

		w.Flush() // 5 - 10

		select {
		case <-ctx.Done():
			return
		default:
			// send messages in batches of 5 secs
			time.Sleep(w.batchInterval)
		}
	}
}

func (w *Worker) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.messages) == 0 {
		return
	}

	err := w.writer.Write(w.messages)
	if err == nil {
		w.messages = make([][]byte, 0) // clear the messages
	}
}

func (w *Worker) Close() error { // nolint: unparam
	// drain batches
	w.wg.Wait()
	return nil
}

// Send event when size more than x
// Send if time passed more than x -> from configuration
//

// New event moderator should start the worker, and keep pushing the messages to it.
