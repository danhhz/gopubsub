// Copyright (C) 2015 Daniel Harrison

package server

import (
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/dustin/randbo"
	"golang.org/x/net/context"
)

const (
	ValueSize = 1024 * 4
)

func makeServer(b *testing.B) *Server {
	dir, err := ioutil.TempDir("", "gopubsub")
	if err != nil {
		b.Fatal(err)
	}

	s, err := NewServer(dir)
	if err != nil {
		b.Fatal(err)
	}

	return s
}

func tidyServer(s *Server) {
	os.RemoveAll(s.dir)
}

func genMessages(b *testing.B, s *Server) []*Message {
	v := make([]byte, ValueSize)
	n, err := io.ReadFull(randbo.New(), v)
	if n != ValueSize || err != nil {
		b.Fatal(err)
	}
	messages := make([]*Message, b.N)
	for i := 0; i < b.N; i++ {
		messages[i] = &Message{0, 0, []byte(strconv.Itoa(i)), v}
		b.SetBytes(int64(len(messages[i].Key) + len(messages[i].Value)))
	}
	return messages
}

func BenchmarkPublishMulti(b *testing.B) {
	s := makeServer(b)
	defer func() {
		b.StopTimer()
		tidyServer(s)
	}()

	messages := genMessages(b, s)
	b.ResetTimer()

	s.PublishMulti(s.ctx, &PublishMultiRequest{"test", messages})
}

func BenchmarkSubscribe(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	s := makeServer(b)
	defer func() {
		b.StopTimer()
		cancel()
		tidyServer(s)
	}()

	messages := genMessages(b, s)
	s.PublishMulti(s.ctx, &PublishMultiRequest{"test", messages})
	topic, _ := s.topics["test"]
	topic.Flush()
	b.ResetTimer()

	f, _ := os.Open(topic.messageSets[0].path)
	mReader := NewMessageSetReader(ctx, f, topic.Listen(ctx))
	for i := 0; i < b.N; i++ {
		_, err := mReader.ReadMessage()
		if err != nil {
			b.Fatal(err)
		}
	}
}
