package queue

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueue(t *testing.T) {
	rQueue, err := NewRedisQueueFromEndpoint("localhost:6379")
	assert.NoError(t, err)

	totalJobs := jobs()
	for _, j := range totalJobs {
		err := rQueue.Enqueue(j)
		assert.NoError(t, err)
	}

	var gotJobs []*Job
	for {
		j, err := rQueue.Dequeue()
		if err == ErrNoJob {
			break
		}
		// otherwise, should be no error
		if err != nil {
			t.Fatal(err)
		}
		assert.NoError(t, rQueue.Done(j))
		gotJobs = append(gotJobs, j)
	}
	assert.True(t, reflect.DeepEqual(totalJobs, gotJobs))
}

func TestDequeNoJob(t *testing.T) {
	rQueue, err := NewRedisQueueFromEndpoint("localhost:6379")
	assert.NoError(t, err)

	_, err = rQueue.Dequeue()
	assert.Equal(t, ErrNoJob, err)
}

func TestRetryJobs(t *testing.T) {
	rQueue, err := NewRedisQueueFromEndpoint("localhost:6379")
	assert.NoError(t, err)

	totalJobs := jobs()
	for _, j := range totalJobs {
		err := rQueue.Enqueue(j)
		assert.NoError(t, err)
	}

	for _ = range totalJobs {
		j, err := rQueue.Dequeue()
		assert.NoError(t, err)
		// retry count is 0
		assert.EqualValues(t, 0, j.RetriedCount)
		assert.NoError(t, rQueue.Retry(j))
	}

	for _ = range totalJobs {
		j, err := rQueue.Dequeue()
		assert.NoError(t, err)
		// retry count is 1
		assert.EqualValues(t, 1, j.RetriedCount)
		assert.NoError(t, rQueue.Done(j))
	}
}

func jobs() []*Job {
	return []*Job{
		&Job{
			ID:        NewJobID(),
			Data:      []byte("hello world"),
			CreatedAt: 123456789,
		},
		&Job{
			ID:        NewJobID(),
			Data:      []byte("hello world2"),
			CreatedAt: 122222222,
		},
		&Job{
			ID:        NewJobID(),
			Data:      []byte("hello world3"),
			CreatedAt: 133333333,
		},
	}
}
