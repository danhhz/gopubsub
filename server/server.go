// Copyright (C) 2015 Daniel Harrison

package server

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"

	"github.com/paperstreet/gopubsub/tail"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
)

type Server struct {
	dir    string
	topics map[string]*TopicMessageSets
}

func NewServer(dir string) (*Server, error) {
	server := Server{dir, make(map[string]*TopicMessageSets)}
	if err := server.init(); err != nil {
		return nil, err
	}

	return &server, nil
}

func (s *Server) init() error {
	if info, err := os.Stat(s.dir); err == nil && info.IsDir() {
		log.Print("Found existing data at ", s.dir)
	} else {
		if err != nil {
			return err
		}
	}

	files, err := ioutil.ReadDir(s.dir)
	if err != nil {
		return err
	}
	for _, fileInfo := range files {
		if fileInfo.IsDir() {
			messageSets, err := ioutil.ReadDir(path.Join(s.dir, fileInfo.Name()))
			if err != nil {
				return err
			}
			topic := TopicMessageSets{name: fileInfo.Name()}
			for _, messageSetFile := range messageSets {
				if filepath.Ext(messageSetFile.Name()) != ".pubsub" {
					continue
				}

				messageSet, err := NewMessageSet(path.Join(s.dir, fileInfo.Name(), messageSetFile.Name()))
				if err != nil {
					return err
				}
				topic.messageSets = append(topic.messageSets, *messageSet)
			}
			if len(topic.messageSets) < 0 {
				continue
			}
			sort.Sort(MessageSetSort(topic.messageSets))
			// TODO(dan): Validate pairwise offsetBegin/offsetEnd

			currentMessageSet := topic.messageSets[len(topic.messageSets)-1]
			topicFile, err := os.OpenFile(currentMessageSet.path, os.O_WRONLY|os.O_APPEND, 0770)
			if err != nil {
				return err
			}
			topic.writer = bufio.NewWriter(topicFile)
			s.topics[topic.name] = &topic
		}
	}
	return nil
}

func (s *Server) PublishMulti(ctx context.Context, in *PublishMultiRequest) (*PublishMultiReply, error) {
	log.Print("[", in.Topic, "] Got ", len(in.GetMessages()), " messages")
	var topic, ok = s.topics[in.Topic]
	if !ok {
		var offset = 0
		var messageSetPath = path.Join(s.dir, in.Topic, fmt.Sprintf("%012d.pubsub", offset))

		err := os.MkdirAll(path.Dir(messageSetPath), 0770)
		if err != nil {
			return nil, err
		}

		f, err := os.OpenFile(messageSetPath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0770)
		if err != nil {
			return nil, err
		}
		log.Print("[", in.Topic, "] Created ", messageSetPath)

		messageSet := MessageSet{path: messageSetPath, offsetBegin: uint64(offset)}
		topic = &TopicMessageSets{name: in.Topic, writer: bufio.NewWriter(f)}
		topic.messageSets = append(topic.messageSets, messageSet)
		s.topics[topic.name] = topic
	}

	var sizeBuf = make([]byte, 4)
	var crc = crc32.NewIEEE()
	for _, message := range in.GetMessages() {
		var encoded, err = proto.Marshal(message)
		if err != nil {
			return nil, err
		}

		binary.LittleEndian.PutUint32(sizeBuf, uint32(len(encoded)+5))
		_, err = topic.writer.Write(sizeBuf)
		if err != nil {
			return nil, err
		}
		err = topic.writer.WriteByte(byte(0))
		if err != nil {
			return nil, err
		}
		crc.Reset()
		crc.Sum(encoded)
		binary.LittleEndian.PutUint32(sizeBuf, uint32(crc.Sum32()))
		_, err = topic.writer.Write(sizeBuf)
		if err != nil {
			return nil, err
		}

		_, err = io.Copy(topic.writer, bytes.NewReader(encoded))
	}

	err := topic.writer.Flush()
	if err != nil {
		return nil, err
	}

	return &PublishMultiReply{}, nil
}

func (s *Server) Subscribe(in *SubscribeRequest, srv PubSub_SubscribeServer) error {
	log.Print("[", in.Topic, "] Opening for subscription")
	topic, ok := s.topics[in.Topic]
	if !ok {
		return errors.New(fmt.Sprintf("No such topic: ", in.Topic))
	}
	if len(topic.messageSets) == 0 {
		return errors.New(fmt.Sprintf("INTERNAL ERROR: No messagesets for topic: ", topic.name, " offset: ", in.Offset))
	}
	messageSet := topic.messageSets[0]
	for i := 0; i < len(topic.messageSets); i++ {
		if topic.messageSets[i].offsetBegin <= in.Offset {
			messageSet = topic.messageSets[i]
		} else {
			break
		}
	}

	tailer, err := tail.TailFile(messageSet.path, tail.Config{Follow: true})
	if err != nil {
		return err
	}

	// TODO(dan): Check for reasonable offset in request, otherwise we'll be here
	// for a while.
	for i := uint64(0); i < in.Offset-messageSet.offsetBegin; i++ {
		<-tailer.Messages
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
