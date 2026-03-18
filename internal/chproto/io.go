package chproto

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// Reader reads ClickHouse native protocol primitives from an io.Reader.
type Reader struct {
	r *bufio.Reader
}

func NewReader(r io.Reader) *Reader {
	if br, ok := r.(*bufio.Reader); ok {
		return &Reader{r: br}
	}
	return &Reader{r: bufio.NewReaderSize(r, 128*1024)}
}

// Underlying returns the underlying buffered reader (for io.Copy after parsing).
func (r *Reader) Underlying() io.Reader {
	return r.r
}

func (r *Reader) UVarInt() (uint64, error) {
	var x uint64
	var s uint
	for i := 0; i < 10; i++ {
		b, err := r.r.ReadByte()
		if err != nil {
			return 0, err
		}
		if b < 0x80 {
			return x | uint64(b)<<s, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, fmt.Errorf("varint overflow")
}

func (r *Reader) String() (string, error) {
	n, err := r.UVarInt()
	if err != nil {
		return "", fmt.Errorf("reading string length: %w", err)
	}
	if n > 10*1024*1024 { // 10MB sanity limit
		return "", fmt.Errorf("string too large: %d bytes", n)
	}
	if n == 0 {
		return "", nil
	}
	buf := make([]byte, n)
	_, err = io.ReadFull(r.r, buf)
	if err != nil {
		return "", fmt.Errorf("reading string body: %w", err)
	}
	return string(buf), nil
}

func (r *Reader) Byte() (byte, error) {
	return r.r.ReadByte()
}

func (r *Reader) Bool() (bool, error) {
	b, err := r.r.ReadByte()
	return b != 0, err
}

func (r *Reader) Int32() (int32, error) {
	var buf [4]byte
	_, err := io.ReadFull(r.r, buf[:])
	return int32(binary.LittleEndian.Uint32(buf[:])), err
}

func (r *Reader) Int64() (int64, error) {
	var buf [8]byte
	_, err := io.ReadFull(r.r, buf[:])
	return int64(binary.LittleEndian.Uint64(buf[:])), err
}

// Writer builds ClickHouse native protocol messages in a buffer.
type Writer struct {
	buf bytes.Buffer
}

func NewWriter() *Writer {
	return &Writer{}
}

func (w *Writer) UVarInt(v uint64) {
	for v >= 0x80 {
		w.buf.WriteByte(byte(v) | 0x80)
		v >>= 7
	}
	w.buf.WriteByte(byte(v))
}

func (w *Writer) String(s string) {
	w.UVarInt(uint64(len(s)))
	w.buf.WriteString(s)
}

func (w *Writer) Byte(b byte) {
	w.buf.WriteByte(b)
}

func (w *Writer) Bool(v bool) {
	if v {
		w.buf.WriteByte(1)
	} else {
		w.buf.WriteByte(0)
	}
}

func (w *Writer) Int32(v int32) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(v))
	w.buf.Write(buf[:])
}

func (w *Writer) Int64(v int64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(v))
	w.buf.Write(buf[:])
}

func (w *Writer) Bytes() []byte {
	return w.buf.Bytes()
}

func (w *Writer) WriteTo(dst io.Writer) (int64, error) {
	return w.buf.WriteTo(dst)
}

func (w *Writer) Raw(p []byte) {
	w.buf.Write(p)
}

func (w *Writer) Reset() {
	w.buf.Reset()
}

// ReadN reads exactly n bytes from the reader.
func (r *Reader) ReadN(n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(r.r, buf)
	return buf, err
}
