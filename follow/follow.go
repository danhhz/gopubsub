// Copyright (C) 2015 Daniel Harrison

package follow

import (
	"bufio"
	"io"
	"os"
	"time"

	"golang.org/x/net/context"
)

// TODO(dan): This is more generally reusable. Add docs and point it out.
type Reader struct {
	ctx    context.Context
	f      *os.File
	r      *bufio.Reader
	Size   int64
	Offset int64
	Notify chan int64
}

// TODO(dan): Use inotify instead of polling if available.
func NewReader(ctx context.Context, file *os.File, ping <-chan int64) *Reader {
	reader := Reader{ctx, file, bufio.NewReader(file), 0, 0, make(chan int64)}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ping:
				fi, err := file.Stat()
				if err == nil && fi.Size() != reader.Size {
					reader.Notify <- fi.Size()
				}
			case <-time.After(250 * time.Millisecond):
				fi, err := file.Stat()
				if err == nil && fi.Size() != reader.Size {
					reader.Notify <- fi.Size()
				}
			}
		}
	}()
	return &reader
}

func (r *Reader) Read(buf []byte) (n int, err error) {
	n, err = r.r.Read(buf)
	r.Offset += int64(n)
	return
}

func (r *Reader) WaitBytes(size int64) error {
	for {
		if fi, err := r.f.Stat(); err != nil {
			return err
		} else {
			r.Size = fi.Size()
		}
		available := r.Size - r.Offset
		if available >= size {
			return nil
		}
		select {
		case <-r.Notify:
			break
		case <-r.ctx.Done():
			return r.ctx.Err()
		}
	}
}

func (r *Reader) WaitReader() io.Reader {
	return &waitReader{r}
}

type waitReader struct {
	r *Reader
}

func (r *waitReader) Read(buf []byte) (n int, err error) {
	if err := r.r.WaitBytes(int64(len(buf))); err != nil {
		return 0, err
	}
	return r.r.Read(buf)
}
