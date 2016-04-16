package queue

import (
	"log"
)

type JobResult int64

const (
	ClaimdDone JobResult = iota
	ClaimedRetry
)

// JobHandler defines an interface which
// handle job, i.e. *job, and returns a JobResult
// the handler library will do things accordingly
type JobHandler interface {
	Handle(*Job) JobResult
}

type DefaultWorker struct {
	q Queue
	h JobHandler

	stopChan chan struct{}
}

func NewDefaultWorker(q Queue, h JobHandler) *DefaultWorker {
	return &DefaultWorker{
		q:        q,
		h:        h,
		stopChan: make(chan struct{}),
	}
}

// Run starts to deque jobs and handle them
func (d *DefaultWorker) Run() error {
	ready := make(chan struct{})
	jobChan := d.dequeLoop(ready, d.stopChan)
	go d.handleLoop(ready, jobChan)

	<-ready
	<-ready
	// when underlying goroutines are ready to go
	// we go
	return nil
}

// Stop stops goroutines
func (d *DefaultWorker) Stop() {
	close(d.stopChan)
}

func (d *DefaultWorker) dequeLoop(ready chan<- struct{}, stop <-chan struct{}) chan *Job {
	jobChan := make(chan *Job, 10) // not sure how large of buffer we need
	go func(ch chan *Job) {

		ready <- struct{}{} // signaling that I am ready

		for {
			select {
			case <-stop:
				close(jobChan)
				return
			default:
				j, err := d.q.Dequeue()
				if err == ErrNoJob {
					// no job right now, continue
					continue
				}
				if err != nil {
					// we may encountered some errors
					// TODO: do we need to abort now?
					log.Printf("[E] deque error:%v, continue...\n", err)
					continue
				}
				// otherwise, we got a job
				jobChan <- j
			}
		}
	}(jobChan)
	return jobChan
}

func (d *DefaultWorker) handleLoop(ready chan<- struct{}, ch chan *Job) error {
	ready <- struct{}{} // signaling that I am ready

	var err error
	for job := range ch {
		result := d.h.Handle(job)
		if result == ClaimdDone {
			err = d.q.Done(job)
		}
		if result == ClaimedRetry {
			err = d.q.Retry(job)
		}
		if err != nil {
			// we cannot do something except log...
			log.Printf("cannot work, due to err:%v\n", err)
		}
	}
	return nil
}
