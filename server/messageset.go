// Copyright (C) 2015 Daniel Harrison

package server

import (
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

	"github.com/paperstreet/gopubsub/follow"

	"golang.org/x/net/context"
)

type MessageSet struct {
	path        string
	offsetBegin uint64
	offsetEnd   uint64
}

func NewMessageSet(ctx context.Context, path string) (*MessageSet, error) {
	basename := filepath.Base(path)
	offset, err := strconv.ParseUint(strings.TrimSuffix(basename, filepath.Ext(basename)), 10, 64)
	if err != nil {
		return nil, err
	}

	messageSet := MessageSet{path: path, offsetBegin: offset}
	if err = messageSet.validate(ctx); err != nil {
		return nil, err
	}

	return &messageSet, nil
}

func (ms *MessageSet) validate(ctx context.Context) error {
	log.Print("Validating message set: ", *ms)

	f, err := os.Open(ms.path)
	if err != nil {
		return err
	}

	r := NewMessageSetReader(ctx, f, nil)
	ms.offsetEnd = ms.offsetBegin
	for {
		_, err = r.readNoWait()
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

type MessageSetReader struct {
	ctx context.Context
	r   *follow.Reader
}

func NewMessageSetReader(ctx context.Context, f *os.File, ping chan int64) *MessageSetReader {
	follower := follow.NewReader(ctx, f, ping)
	return &MessageSetReader{ctx, follower}
}

func (ms *MessageSetReader) ReadMessage() ([]byte, error) {
	if err := ms.r.WaitBytes(4); err != nil {
		return nil, err
	}
	length, err := readLength(ms.r)
	if err != nil {
		return nil, err
	}
	if err := ms.r.WaitBytes(int64(length)); err != nil {
		return nil, err
	}
	return readPayload(ms.r, length)
}

func (ms *MessageSetReader) readNoWait() ([]byte, error) {
	length, err := readLength(ms.r)
	if err != nil {
		return nil, err
	}
	return readPayload(ms.r, length)
}

func readLength(reader io.Reader) (uint32, error) {
	lengthBuf := make([]byte, 4)
	_, err := io.ReadFull(reader, lengthBuf)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(lengthBuf), nil
}

func readPayload(reader io.Reader, length uint32) ([]byte, error) {
	magicBuf := make([]byte, 1)
	_, err := io.ReadFull(reader, magicBuf)
	if err != nil {
		return nil, err
	}
	if magicBuf[0] != 0 {
		return nil, errors.New(fmt.Sprintf("Unsupported magic: %d", magicBuf[0]))
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

type MessageSetSort []MessageSet

func (s MessageSetSort) Len() int           { return len(s) }
func (s MessageSetSort) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s MessageSetSort) Less(i, j int) bool { return s[i].offsetBegin < s[j].offsetBegin }
