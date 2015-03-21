// Copyright (C) 2015 Daniel Harrison

package main

import (
	"flag"
	"io"
	"log"
	"time"

	pb "github.com/paperstreet/gopubsub/server"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

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
			var messageTime time.Time
			if err := messageTime.UnmarshalText(message.Key); err == nil {
				diff := time.Now().Sub(messageTime)
				log.Print("[", *topic, "] ", diff.String(), "|", string(message.Value))
			}
		}
	}
}
