package subagent

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
)

const maxFrameSize = 512 * 1024 * 1024

// frameReader reads LSP-style Content-Length frames. Header lines may end
// with CRLF or LF.
type frameReader struct {
	reader *bufio.Reader
}

func newFrameReader(r io.Reader) *frameReader {
	return &frameReader{reader: bufio.NewReaderSize(r, 64*1024)}
}

func (f *frameReader) read() ([]byte, error) {
	length := -1
	sawHeader := false
	for {
		line, err := f.reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) && !sawHeader && line == "" {
				return nil, io.EOF
			}
			if errors.Is(err, io.EOF) {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}
		trimmed := strings.TrimRight(line, "\r\n")
		if trimmed == "" {
			if !sawHeader {
				continue
			}
			break
		}
		sawHeader = true
		key, value, ok := strings.Cut(trimmed, ":")
		if !ok || !strings.EqualFold(strings.TrimSpace(key), "content-length") {
			continue
		}
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil || parsed < 0 {
			return nil, fmt.Errorf("invalid Content-Length header %q", strings.TrimSpace(value))
		}
		length = parsed
	}
	if length < 0 {
		return nil, errors.New("missing Content-Length header")
	}
	if length > maxFrameSize {
		return nil, fmt.Errorf("frame of %d bytes exceeds the %d byte limit", length, maxFrameSize)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(f.reader, payload); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return payload, nil
}

// frameWriter writes Content-Length frames; it is safe for concurrent use.
type frameWriter struct {
	mu     sync.Mutex
	writer io.Writer
}

func (f *frameWriter) write(payload []byte) error {
	frame := make([]byte, 0, len(payload)+32)
	frame = append(frame, "Content-Length: "...)
	frame = strconv.AppendInt(frame, int64(len(payload)), 10)
	frame = append(frame, "\r\n\r\n"...)
	frame = append(frame, payload...)
	f.mu.Lock()
	defer f.mu.Unlock()
	_, err := f.writer.Write(frame)
	return err
}
