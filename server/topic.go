// Copyright (C) 2015 Daniel Harrison

package server

import (
	"bufio"
	"fmt"

	"golang.org/x/net/context"
)

type Topic struct {
	name        string
	messageSets []MessageSet
	writer      *bufio.Writer
	listeners   []topicListener
}

func (t *Topic) Write(p []byte) (n int, err error) {
	n, err = t.writer.Write(p)
	if err == nil {
		t.broadcast(int64(n))
	}
	return n, err
}

func (t *Topic) Flush() error {
	return t.writer.Flush()
}

func (t *Topic) Listen(ctx context.Context) chan int64 {
	ping := make(chan int64, 1)
	t.listeners = append(t.listeners, topicListener{ctx, ping})
	return ping
}

func (t *Topic) broadcast(n int64) {
	// TODO(dan): Think more about the channels here and blocking.
	for i, listener := range t.listeners {
		select {
		case <-listener.ctx.Done():
			fmt.Print("[", t.name, "] Removing subscriber from notifications")
			t.listeners = append(t.listeners[:i], t.listeners[i+1:]...)
		default:
			listener.notify <- n
		}
	}
}

type topicListener struct {
	ctx    context.Context
	notify chan int64
}
