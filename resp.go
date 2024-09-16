package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
)

type Value struct {
	typ   string  // type用于确定值所携带的数据类型
	str   string  // str保存从简单字符串接收的字符串的值
	num   int     // num保存从整数接收的整数的值。
	bulk  string  // bulk用于存储从批量字符串接收的字符串。
	array []Value // 数组保存从数组接收的所有值。
}

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

type Resp struct {
	reader *bufio.Reader
}

func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

// 从缓冲区读取行
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

// 从缓冲区读取整数
func (r *Resp) readInteger() (x int, n int, err error) {
	line, n, err := r.readLine()
	if err != nil {
		return 0, 0, err
	}
	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}
	return int(i64), n, nil
}

// 递归读取缓冲区
func (r *Resp) Read() (Value, error) {
	_type, err := r.reader.ReadByte()

	if err != nil {
		return Value{}, err
	}

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

// 跳过第一个字节，因为我们已经在 Read 方法中读取了它。
// 读取表示数组中元素数量的整数。
// 遍历数组并针对每一行调用 Read 方法根据行首的字符解析类型。
// 每次迭代时，将解析的值附加到 Value 对象中的数组并返回它
func (r *Resp) readArray() (Value, error) {
	v := Value{}
	v.typ = "array"

	// read length of array
	len, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	// foreach line, parse and read the value
	v.array = make([]Value, 0)
	for i := 0; i < len; i++ {
		val, err := r.Read()
		if err != nil {
			return v, err
		}

		// append parsed value to array
		v.array = append(v.array, val)
	}

	return v, nil
}

// 实现 Bulk 类型以便 readArray 知道如何返回值
// 跳过第一个字节，因为我们已经在 Read 方法中读取了它。
// 读取表示批量字符串中字节数的整数。
// 读取批量字符串，后跟表示批量字符串结束的“\r\n”。
// 返回 Value 对象
func (r *Resp) readBulk() (Value, error) {
	v := Value{}

	v.typ = "bulk"

	len, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	bulk := make([]byte, len)

	r.reader.Read(bulk)

	v.bulk = string(bulk)

	// Read the trailing CRLF
	// 我们r.readLine()在读取字符串后调用读取每个批量字符串后面的 '\r\n'。如果我们不这样做，指针将留在 '\r' 处，Read 方法将无法正确读取下一个批量字符串
	r.readLine()

	return v, nil
}

// Marshal Value to bytes
func (v Value) Marshal() []byte {
	switch v.typ {
	case "array":
		return v.marshalArray()
	case "bulk":
		return v.marshalBulk()
	case "string":
		return v.marshalString()
	case "null":
		return v.marshallNull()
	case "error":
		return v.marshallError()
	default:
		return []byte{}
	}
}

// 简单字符串
// 我们创建一个字节数组并添加字符串，然后是 CRLF（回车换行符）。请注意，如果没有 CRLF，就会出现问题，因为如果没有它，RESP 客户端将无法理解响应
func (v Value) marshalString() []byte {
	var bytes []byte
	bytes = append(bytes, STRING)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

// 批量字符串
func (v Value) marshalBulk() []byte {
	var bytes []byte
	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(len(v.bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.bulk...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

// 大批量
// 对于 Array，我们在循环内部调用 Value Object 的 Marshal 方法来转换它，而不管它的类型如何。这就是我们所说的递归，我们在第一部分进行解析时提到过
func (v Value) marshalArray() []byte {
	len := len(v.array)
	var bytes []byte
	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(len)...)
	bytes = append(bytes, '\r', '\n')

	for i := 0; i < len; i++ {
		bytes = append(bytes, v.array[i].Marshal()...)
	}

	return bytes
}

// 我们需要 Null 和 Error，以防我们需要响应客户端未找到数据或出现错误。
func (v Value) marshallError() []byte {
	var bytes []byte
	bytes = append(bytes, ERROR)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

// 我们需要 Null 和 Error，以防我们需要响应客户端未找到数据或出现错误。
func (v Value) marshallNull() []byte {
	return []byte("$-1\r\n")
}

// 我们创建一个方法，该方法接受 Value 并将从 Marshal 方法获取的字节写入 Writer
func (w *Writer) Write(v Value) error {
	var bytes = v.Marshal()

	_, err := w.writer.Write(bytes)
	if err != nil {
		return err
	}

	return nil
}
