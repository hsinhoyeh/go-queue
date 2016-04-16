package queue

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type sentenceHandler struct {
	gotWords []string
}

func (s *sentenceHandler) Handle(j *Job) JobResult {
	s.gotWords = append(s.gotWords, string(j.Data))
	return ClaimdDone
}

func TestSentence(t *testing.T) {
	rQueue := newNamedRedisQueue("sentence")

	s := &sentenceHandler{}
	w := NewDefaultWorker(rQueue, s)
	assert.NoError(t, w.Run())

	sentence := "The man who passes the sentence should swing the sword"
	words := strings.Split(sentence, " ")
	for _, word := range words {
		err := rQueue.Enqueue(&Job{
			ID:   NewJobID(),
			Data: []byte(word),
		})
		assert.NoError(t, err)
	}

	time.Sleep(time.Second) // wait for digest jobs...

	// now, stop worker and check what we got
	w.Stop()
	gotSentence := strings.Join(s.gotWords, " ")
	assert.Equal(t, sentence, gotSentence)
}
