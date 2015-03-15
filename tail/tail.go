// Copyright (c) 2013 ActiveState Software Inc. All rights reserved.
//
// TODO(dan): I copied this from https://github.com/ActiveState/tail and
// changed it to read binary instead of lines. Instead, see if it can be merged
// back.

package tail

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/ActiveState/tail/util"
	"github.com/ActiveState/tail/watch"
	"gopkg.in/tomb.v1"
)

var (
	ErrStop = fmt.Errorf("tail should now stop")
)

// SeekInfo represents arguments to `os.Seek`
type SeekInfo struct {
	Offset int64
	Whence int // os.SEEK_*
}

// Config is used to specify how a file must be tailed.
type Config struct {
	// File-specifc
	Location  *SeekInfo // Seek to this location before tailing
	ReOpen    bool      // Reopen recreated files (tail -F)
	MustExist bool      // Fail early if the file does not exist
	Poll      bool      // Poll for file changes instead of using inotify

	// Generic IO
	Follow bool // Continue looking for new messages (tail -f)

	// Logger, when nil, is set to tail.DefaultLogger
	// To disable logging: set field to tail.DiscardingLogger
	Logger *log.Logger
}

type Tail struct {
	Filename string
	Messages chan *[]byte
	Config

	file    *os.File
	reader  *bufio.Reader
	watcher watch.FileWatcher
	changes *watch.FileChanges

	tomb.Tomb // provides: Done, Kill, Dying
}

var (
	// DefaultLogger is used when Config.Logger == nil
	DefaultLogger = log.New(os.Stderr, "", log.LstdFlags)
	// DiscardingLogger can be used to disable logging output
	DiscardingLogger = log.New(ioutil.Discard, "", 0)
)

// TailFile begins tailing the file. Output stream is made available
// via the `Tail.Messages` channel. To handle errors during tailing,
// invoke the `Wait` or `Err` method after finishing reading from the
// `Messages` channel.
func TailFile(filename string, config Config) (*Tail, error) {
	if config.ReOpen && !config.Follow {
		util.Fatal("cannot set ReOpen without Follow.")
	}

	t := &Tail{
		Filename: filename,
		Messages: make(chan *[]byte),
		Config:   config,
	}

	// when Logger was not specified in config, use default logger
	if t.Logger == nil {
		t.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	if t.Poll {
		t.watcher = watch.NewPollingFileWatcher(filename)
	} else {
		t.watcher = watch.NewInotifyFileWatcher(filename)
	}

	if t.MustExist {
		var err error
		t.file, err = OpenFile(t.Filename)
		if err != nil {
			return nil, err
		}
	}

	go t.tailFileSync()

	return t, nil
}

// Return the file's current position, like stdio's ftell().
// But this value is not very accurate.
// func (tail *Tail) Tell() (offset int64, err error) {
// 	if tail.file == nil {
// 		return
// 	}
// 	offset, err = tail.file.Seek(0, os.SEEK_CUR)
// 	if err == nil {
// 		offset -= int64(tail.reader.Buffered())
// 	}
// 	return
// }

func OpenFile(name string) (file *os.File, err error) {
	return os.Open(name)
}

// Stop stops the tailing activity.
func (tail *Tail) Stop() error {
	tail.Kill(nil)
	return tail.Wait()
}

func (tail *Tail) close() {
	close(tail.Messages)
	if tail.file != nil {
		tail.file.Close()
	}
}

func (tail *Tail) reopen() error {
	if tail.file != nil {
		tail.file.Close()
	}
	for {
		var err error
		tail.file, err = OpenFile(tail.Filename)
		if err != nil {
			if os.IsNotExist(err) {
				tail.Logger.Printf("Waiting for %s to appear...", tail.Filename)
				if err := tail.watcher.BlockUntilExists(&tail.Tomb); err != nil {
					if err == tomb.ErrDying {
						return err
					}
					return fmt.Errorf("Failed to detect creation of %s: %s", tail.Filename, err)
				}
				continue
			}
			return fmt.Errorf("Unable to open file %s: %s", tail.Filename, err)
		}
		break
	}
	return nil
}

func (tail *Tail) readLine() (string, error) {
	line, err := tail.reader.ReadString('\n')
	if err != nil {
		// Note ReadString "returns the data read before the error" in
		// case of an error, including EOF, so we return it as is. The
		// caller is expected to process it if err is EOF.
		return line, err
	}

	line = strings.TrimRight(line, "\n")

	return line, err
}

func (tail *Tail) readBytes(length int) ([]byte, error) {
	buf := make([]byte, length)
	n, err := tail.reader.Read(buf)
	if err != nil {
		return nil, err
	}
	if n != length {
		return nil, errors.New("Not enough bytes read")
	}
	return buf, nil
}

func (tail *Tail) readMessage() ([]byte, error) {
	lengthBuf, err := tail.readBytes(4)
	if err != nil {
		return nil, err
	}
	length := binary.LittleEndian.Uint32(lengthBuf)

	magic, err := tail.reader.ReadByte()
	if err != nil {
		return nil, err
	}
	if magic != byte(0) {
		return nil, errors.New(fmt.Sprintf("Unsupported magic: %d", int(magic)))
	}

	crcBuf, err := tail.readBytes(4)
	if err != nil {
		return nil, err
	}
	crcCheck := binary.LittleEndian.Uint32(crcBuf)

	dataBuf, err := tail.readBytes(int(length - 5))
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

func (tail *Tail) tailFileSync() {
	defer tail.Done()
	defer tail.close()

	if !tail.MustExist {
		// deferred first open.
		err := tail.reopen()
		if err != nil {
			if err != tomb.ErrDying {
				tail.Kill(err)
			}
			return
		}
	}

	// Seek to requested location on first open of the file.
	if tail.Location != nil {
		_, err := tail.file.Seek(tail.Location.Offset, tail.Location.Whence)
		tail.Logger.Printf("Seeked %s - %+v\n", tail.Filename, tail.Location)
		if err != nil {
			tail.Killf("Seek error on %s: %s", tail.Filename, err)
			return
		}
	}

	tail.openReader()

	// Read message by message.
	for {
		message, err := tail.readMessage()

		// Process `message` even if err is EOF.
		if err == nil || (err == io.EOF && message != nil) {
			tail.sendMessage(message)
		} else if err == io.EOF {
			if !tail.Follow {
				return
			}
			// When EOF is reached, wait for more data to become
			// available. Wait strategy is based on the `tail.watcher`
			// implementation (inotify or polling).
			err := tail.waitForChanges()
			if err != nil {
				if err != ErrStop {
					tail.Kill(err)
				}
				return
			}
		} else {
			// non-EOF error
			tail.Killf("Error reading %s: %s", tail.Filename, err)
			return
		}

		select {
		case <-tail.Dying():
			return
		default:
		}
	}
}

// waitForChanges waits until the file has been appended, deleted,
// moved or truncated. When moved or deleted - the file will be
// reopened if ReOpen is true. Truncated files are always reopened.
func (tail *Tail) waitForChanges() error {
	if tail.changes == nil {
		st, err := tail.file.Stat()
		if err != nil {
			return err
		}
		tail.changes = tail.watcher.ChangeEvents(&tail.Tomb, st)
	}

	select {
	case <-tail.changes.Modified:
		return nil
	case <-tail.changes.Deleted:
		tail.changes = nil
		if tail.ReOpen {
			// XXX: we must not log from a library.
			tail.Logger.Printf("Re-opening moved/deleted file %s ...", tail.Filename)
			if err := tail.reopen(); err != nil {
				return err
			}
			tail.Logger.Printf("Successfully reopened %s", tail.Filename)
			tail.openReader()
			return nil
		} else {
			tail.Logger.Printf("Stopping tail as file no longer exists: %s", tail.Filename)
			return ErrStop
		}
	case <-tail.changes.Truncated:
		// Always reopen truncated files (Follow is true)
		tail.Logger.Printf("Re-opening truncated file %s ...", tail.Filename)
		if err := tail.reopen(); err != nil {
			return err
		}
		tail.Logger.Printf("Successfully reopened truncated %s", tail.Filename)
		tail.openReader()
		return nil
	case <-tail.Dying():
		return ErrStop
	}
	panic("unreachable")
}

func (tail *Tail) openReader() {
	tail.reader = bufio.NewReader(tail.file)
}

func (tail *Tail) seekEnd() error {
	_, err := tail.file.Seek(0, 2)
	if err != nil {
		return fmt.Errorf("Seek error on %s: %s", tail.Filename, err)
	}
	// Reset the read buffer whenever the file is re-seek'ed
	tail.reader.Reset(tail.file)
	return nil
}

func (tail *Tail) sendMessage(message []byte) bool {
	// now := time.Now()
	tail.Messages <- &message
	return true
}

// Cleanup removes inotify watches added by the tail package. This function is
// meant to be invoked from a process's exit handler. Linux kernel may not
// automatically remove inotify watches after the process exits.
func Cleanup() {
	watch.Cleanup()
}
