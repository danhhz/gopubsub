// Copyright (C) 2015 Daniel Harrison

package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/paperstreet/gopubsub/server"
	"google.golang.org/grpc"
)

func main() {
	var port = flag.Int("port", 8054, "")
	var path = flag.String("path", "/tmp/gopubpub", "")

	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	log.Print("Listening on port ", *port)
	s := grpc.NewServer()

	impl, err := server.NewServer(*path)
	if err != nil {
		log.Fatalf("Failed to configure: %v", err)
	}
	server.RegisterPubSubServer(s, impl)
	s.Serve(lis)
}
