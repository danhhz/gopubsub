// Copyright (C) 2015 Daniel Harrison

package server

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path"

	"github.com/paperstreet/gopubsub/tail"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
)

type Server struct {
	dir          string
	topicWriters map[string]*bufio.Writer
}

func NewServer(dir string) (*Server, error) {
	if info, err := os.Stat(dir); err == nil && info.IsDir() {
		log.Print("Found existing data at ", dir)
	}

	err := os.MkdirAll(dir, 0770)
	if err != nil {
		return nil, err
	}

	// TODO(dan): Read in existing data instead.
	err = os.RemoveAll(path.Join(dir, "*"))
	if err != nil {
		return nil, err
	}

	return &Server{dir, make(map[string]*bufio.Writer)}, nil
}

func (s *Server) PublishMulti(ctx context.Context, in *PublishMultiRequest) (*PublishMultiReply, error) {
	log.Print("[", in.Topic, "] Got ", len(in.GetMessages()), " messages")
	var writer, ok = s.topicWriters[in.Topic]
	if !ok {
		var index = 0
		var chunk = path.Join(s.dir, in.Topic, fmt.Sprintf("%012d.pubsub", index))

		err := os.MkdirAll(path.Dir(chunk), 0770)
		if err != nil {
			return nil, err
		}

		log.Print("[", in.Topic, "] Created ", chunk)
		f, err := os.OpenFile(chunk, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0770)
		if err != nil {
			return nil, err
		}

		writer = bufio.NewWriter(f)
		s.topicWriters[in.Topic] = writer
	}
	var sizeBuf = make([]byte, 4)
	var crc = crc32.NewIEEE()
	for _, message := range in.GetMessages() {
		var encoded, err = proto.Marshal(message)
		if err != nil {
			return nil, err
		}

		binary.LittleEndian.PutUint32(sizeBuf, uint32(len(encoded)+5))
		_, err = writer.Write(sizeBuf)
		if err != nil {
			return nil, err
		}
		err = writer.WriteByte(byte(0))
		if err != nil {
			return nil, err
		}
		crc.Reset()
		crc.Sum(encoded)
		binary.LittleEndian.PutUint32(sizeBuf, uint32(crc.Sum32()))
		_, err = writer.Write(sizeBuf)
		if err != nil {
			return nil, err
		}

		_, err = io.Copy(writer, bytes.NewReader(encoded))
	}

	err := writer.Flush()
	if err != nil {
		return nil, err
	}

	return &PublishMultiReply{}, nil
}

func (s *Server) Subscribe(in *SubscribeRequest, srv PubSub_SubscribeServer) error {
	log.Print("[", in.Topic, "] Opening for subscription")
	var index = 0
	chunk := path.Join(s.dir, in.Topic, fmt.Sprintf("%012d.pubsub", index))
	tailer, err := tail.TailFile(chunk, tail.Config{Follow: true})
	if err != nil {
		return err
	}

	for messageBytes := range tailer.Messages {
		message := new(Message)
		err = proto.Unmarshal(*messageBytes, message)

		response := SubscribeResponse{}
		response.Messages = append(response.Messages, message)
		err = srv.Send(&response)
		if err != nil {
			return err
		}
	}

	return nil
}
