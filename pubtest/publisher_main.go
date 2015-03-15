// Copyright (C) 2015 Daniel Harrison

package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"

	pb "github.com/paperstreet/gopubsub/server"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func SendTest(c *pb.PubSubClient, topic string, size int, wg *sync.WaitGroup) {
	var request = pb.PublishMultiRequest{Topic: topic}
	for i := 0; i < size; i++ {
		var message = pb.Message{Key: []byte(fmt.Sprintf("key-%d", i)), Value: []byte(fmt.Sprintf("value-%d", i))}
		request.Messages = append(request.Messages, &message)
	}

	var t = rand.Intn(100)
	time.Sleep(time.Duration(t) * time.Millisecond)
	_, err := (*c).PublishMulti(context.Background(), &request)
	if err != nil {
		log.Fatalf("Could not send: %v", err)
	}
	log.Print("[", topic, "] Wrote ", size, " messages")
	wg.Done()
}

func main() {
	var address = flag.String("address", "localhost:8054", "")
	var size = flag.Int("size", 3, "")
	var topics = flag.Int("topics", 3, "")

	flag.Parse()

	conn, err := grpc.Dial(*address)
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewPubSubClient(conn)

	var wg sync.WaitGroup
	for i := 0; i < *topics; i++ {
		wg.Add(1)
		go SendTest(&c, strconv.Itoa(i), *size, &wg)
	}
	wg.Wait()
}
