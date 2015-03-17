// Copyright (C) 2015 Daniel Harrison

package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
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
	var topic = flag.String("topic", "0", "")
	var offset = flag.Int("offset", 0, "")

	flag.Parse()

	conn, err := grpc.Dial(*address)
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewPubSubClient(conn)

	stream, err := c.Subscribe(context.Background(), &pb.SubscribeRequest{*topic, uint64(*offset)})
	if err != nil {
		log.Fatalf("Could not subscribe: %v", err)
	}

	for {
		response, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("%v.Subscribe(_) = _, %v", c, err)
		}
		for _, message := range response.GetMessages() {
			log.Print("[", *topic, "] ", string(message.Key), "|", string(message.Value))
		}
	}
}
