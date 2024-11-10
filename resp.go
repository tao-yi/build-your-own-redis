package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

/*
RESP data type	Minimal protocol version	Category	First byte
Simple strings	RESP2	Simple	+
Simple Errors	RESP2	Simple	-
Integers	RESP2	Simple	:
Bulk strings	RESP2	Aggregate	$
Arrays	RESP2	Aggregate	*
Nulls	RESP3	Simple	_
Booleans	RESP3	Simple	#
Doubles	RESP3	Simple	,
Big numbers	RESP3	Simple	(
Bulk errors	RESP3	Aggregate	!
Verbatim strings	RESP3	Aggregate	=
Maps	RESP3	Aggregate	%
Attributes	RESP3	Aggregate	`
Sets	RESP3	Aggregate	~
Pushes	RESP3	Aggregate	>
*/

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
)

type Value struct {
	typ   string  // data type carried by the value
	str   string  // value of the string received from the simple strings
	num   int     // value of the integer received from the integers
	bulk  string  // string received from the bulk strings
	array []Value // all the values received from the array
}

func (v Value) Marshal() []byte {
	switch v.typ {
	case "array":
		return v.marshallArray()
	case "bulk":
		return v.marshallBulk()
	case "string":
		return v.marshallString()
	case "null":
		return v.marshallNull()
	case "error":
		return v.marshallError()
	default:
		return []byte{}
	}
}

func (v Value) marshallString() []byte {
	var bytes []byte
	bytes = append(bytes, STRING)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (v Value) marshallBulk() []byte {
	var bytes []byte
	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(len(v.bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.bulk...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (v Value) marshallArray() []byte {
	size := len(v.array)
	var bytes []byte
	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(size)...)
	bytes = append(bytes, '\r', '\n')
	for i := range size {
		bytes = append(bytes, v.array[i].Marshal()...)
	}
	return bytes
}

func (v Value) marshallError() []byte {
	var bytes []byte
	bytes = append(bytes, ERROR)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (v Value) marshallNull() []byte {
	return []byte("$-1\r\n")
}

type Resp struct {
	reader *bufio.Reader
}

func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

// readLine read one byte at a time until we reach '\r',
// which indicates the end of the line
// Then we return the line without the last 2 bytes, which are '\r\n'
// and the number of bytes in the line
func (r *Resp) readLine() (line []byte, n int, err error) {
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n += 1
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}

	return line[:len(line)-2], n, nil
}

func (r *Resp) Read() (Value, error) {
	_type, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}
	// for now, only handle Array and Bulk
	switch _type {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	default:
		fmt.Printf("Unknown type: %v", string(_type))
		return Value{}, nil
	}
}

func (r *Resp) readArray() (Value, error) {
	v := Value{}
	v.typ = "array"
	// read length of array
	length, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	// foreach line, parse and read the value
	v.array = make([]Value, length)
	for i := range length {
		val, err := r.Read()
		if err != nil {
			return v, err
		}
		// add parsed value to array
		v.array[i] = val
	}

	return v, nil
}

func (r *Resp) readInteger() (x int, n int, err error) {
	line, n, err := r.readLine()
	if err != nil {
		return 0, 0, err
	}
	i64, err := strconv.Atoi(string(line))
	if err != nil {
		return 0, n, err
	}
	return int(i64), n, nil
}

func (r *Resp) readBulk() (Value, error) {
	v := Value{typ: "bulk"}
	len, _, err := r.readInteger()
	if err != nil {
		return v, err
	}
	bulk := make([]byte, len)
	r.reader.Read(bulk)
	v.bulk = string(bulk)
	// reading the trailing CRLF
	r.readLine()
	return v, nil
}

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (w *Writer) Write(v Value) error {
	bytes := v.Marshal()
	_, err := w.writer.Write(bytes)
	return err
}
