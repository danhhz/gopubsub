// Copyright (C) 2015 Daniel Harrison

package server

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type MessageSet struct {
	path        string
	offsetBegin uint64
	offsetEnd   uint64
}
type MessageSetSort []MessageSet

func (s MessageSetSort) Len() int           { return len(s) }
func (s MessageSetSort) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s MessageSetSort) Less(i, j int) bool { return s[i].offsetBegin < s[j].offsetBegin }

type TopicMessageSets struct {
	name string
	// mu sync.Mutex
	messageSets []MessageSet
	writer      *bufio.Writer
}

func NewMessageSet(path string) (*MessageSet, error) {
	basename := filepath.Base(path)
	offset, err := strconv.ParseUint(strings.TrimSuffix(basename, filepath.Ext(basename)), 10, 64)
	if err != nil {
		return nil, err
	}

	messageSet := MessageSet{path: path, offsetBegin: offset}
	if err = messageSet.validate(); err != nil {
		return nil, err
	}

	return &messageSet, nil
}

func (ms *MessageSet) validate() error {
	log.Print("Validating message set: ", *ms)

	f, err := os.Open(ms.path)
	if err != nil {
		return err
	}

	reader := bufio.NewReader(f)
	ms.offsetEnd = ms.offsetBegin
	for {
		_, err := ms.readMessage(reader)
		if err == io.EOF {
			log.Print("Validated message set:  ", *ms)
			return nil
		} else if err != nil {
			return err
		}
		ms.offsetEnd++
	}
	return nil
}

// TODO(dan): Reduce duplication between this and tail.go.
func (ms *MessageSet) readMessage(reader *bufio.Reader) ([]byte, error) {
	lengthBuf := make([]byte, 4)
	_, err := io.ReadFull(reader, lengthBuf)
	if err != nil {
		return nil, err
	}
	length := binary.LittleEndian.Uint32(lengthBuf)

	magic, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	if magic != byte(0) {
		return nil, errors.New(fmt.Sprintf("Unsupported magic: %d", int(magic)))
	}

	crcBuf := make([]byte, 4)
	_, err = io.ReadFull(reader, crcBuf)
	if err != nil {
		return nil, err
	}
	crcCheck := binary.LittleEndian.Uint32(crcBuf)

	dataBuf := make([]byte, int(length-5))
	_, err = io.ReadFull(reader, dataBuf)
	if err != nil {
		return nil, err
	}

	crc := crc32.NewIEEE()
	crc.Sum(dataBuf)
	crcData := crc.Sum32()
	if crcCheck != crcData {
		return nil, errors.New(fmt.Sprintf("Mismatched crc got %d expected %d", crcCheck, crcData))
	}

	return dataBuf, nil
}
