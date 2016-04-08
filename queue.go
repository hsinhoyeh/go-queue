package queue

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/satori/go.uuid"
	"gopkg.in/redis.v3"
)

type Job struct {
	ID        string
	Data      []byte
	CreatedAt uint64 // in second
}

func (j *Job) Marshal() ([]byte, error) {
	return json.Marshal(j)
}

func (j *Job) Unmarshal(data []byte) error {
	return json.Unmarshal(data, j)
}

type Queue interface {
	// Enqueue sends a job into queue
	Enqueue(j *Job) error

	// Dequeue retrieves the most recent one from the queue
	Dequeue() (*Job, error)

	// Retry resignal the job to the queue
	Retry(*Job) error

	// Done deletes the job permanently
	Done(*Job) error
}

// NewRedisQueue creates and returns an instance of *RedisQueue from the endpoints given
func NewRedisQueueFromEndpoint(endpoints ...string) (*RedisQueue, error) {
	if len(endpoints) != 1 {
		return nil, errors.New("multiple endpoints are not supported")
	}

	client := redis.NewClient(&redis.Options{
		Addr:         endpoints[0],
		DialTimeout:  5 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	})

	return NewNamedRedisQueue(client, defaultName), nil

}

const (
	defaultName = "default"
)

// RedisQueue holds a redis.client to impl Queue interface
type RedisQueue struct {
	client *redis.Client
	name   string
}

// NewRedisQueue creates and returns an instance of *RedisQueue
func NewRedisQueue(client *redis.Client) *RedisQueue {
	return &RedisQueue{
		client: client,
		name:   defaultName,
	}
}

// NewNamedRedisQueue returns an *RedisQueue with the name given
// all jobs will go to that namespace
func NewNamedRedisQueue(client *redis.Client, name string) *RedisQueue {
	return &RedisQueue{
		client: client,
		name:   name,
	}
}

// Enqueue pushes a job into the queue
func (r *RedisQueue) Enqueue(j *Job) error {
	jobBlob, err := j.Marshal()
	if err != nil {
		return err
	}
	jkey := MakeJobKey(r.name, j.ID)

	// send job blob to redis
	if err := r.client.HMSet(jkey,
		"blob", string(jobBlob),
		"jid", j.ID).Err(); err != nil {
		return err
	}

	// then add signal with brpop
	if err := r.client.LPush(MakeQueueName(r.name), jkey).Err(); err != nil {
		return err
	}
	return nil
}

var (
	dequeTimeout = 1 * time.Second

	ErrNoJob = errors.New("no job")
)

// Dequeue pop a job from the queue
// if there is no job available, return nil, ErrNoJob
func (r *RedisQueue) Dequeue() (*Job, error) {
	// do brpop to get job id, then retreve job back
	// TODO: we may lost job id when worker poped jid then dead
	// need to have a resignal process

	replies, err := r.client.BRPop(dequeTimeout, MakeQueueName(r.name)).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, ErrNoJob
		}
		return nil, err
	}
	if len(replies) != 2 {
		// len mismatch
		return nil, errors.New("len mismatch")
	}
	queueName := replies[0]
	jKey := replies[1]
	_ = queueName

	resp, err := r.client.HMGet(jKey, "blob").Result()
	if err != nil {
		if err == redis.Nil {
			return nil, ErrNoJob
		}
		return nil, err
	}
	j := &Job{}
	if err := j.Unmarshal([]byte(resp[0].(string))); err != nil {
		return nil, err
	}
	return j, nil
}

// Retry resignal the job
func (r *RedisQueue) Retry(j *Job) error {
	if err := r.client.LPush(MakeQueueName(r.name), MakeJobKey(r.name, j.ID)).Err(); err != nil {
		return err
	}
	return nil
}

// Done marks "done" tag to the job
func (r *RedisQueue) Done(j *Job) error {
	return r.client.Del(MakeJobKey(r.name, j.ID)).Err()
}

// MakeQueueName defines a queue name
func MakeQueueName(jobType string) string {
	return fmt.Sprintf("/jq/queue/%s", jobType)
}

// MakeJobKey is a helper function to create a job key according to job id
func MakeJobKey(name string, jid string) string {
	return fmt.Sprintf("/jq/j/%s/%s", name, jid)
}

// NewJobID is a helper function to create a random JobID
func NewJobID() string {
	h := fnv.New64()
	h.Write(uuid.NewV4().Bytes())
	return fmt.Sprintf("%d", h.Sum64())

}

// NewJob is a helper function to create *Job by the given data
func NewJob(data []byte) *Job {
	return &Job{
		ID:        NewJobID(),
		CreatedAt: uint64(time.Now().Unix()),
		Data:      data,
	}
}
