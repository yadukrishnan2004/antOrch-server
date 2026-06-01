package queue

import (
	"fmt"

	"github.com/yadukrishnan2004/antOrch-server/domain"
)


type ChannelQueue struct{
	ch chan domain.Task
}

func New(bufferSize int) *ChannelQueue {
	return &ChannelQueue{ch: make(chan domain.Task,bufferSize)}
}

func (q *ChannelQueue) Enqueue(task domain.Task) error {
	select {
	case q.ch <- task:
		return nil
	default:
		return fmt.Errorf("task queue is full (capacity=%d)", cap(q.ch))
	}
}

// Poll returns the read channel so workers can receive tasks.
func (q *ChannelQueue) Poll() <-chan domain.Task {
	return q.ch
}

// Close shuts down the queue (signals workers to stop).
func (q *ChannelQueue) Close() {
	close(q.ch)
}