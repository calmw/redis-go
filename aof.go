package main

import (
	"bufio"
	"io"
	"os"
	"sync"
	"time"
)

type Aof struct {
	file *os.File
	rd   *bufio.Reader
	mu   sync.Mutex
}

// NewAof 我们创建在服务器启动时NewAof使用的方法
// 每秒同步一次的想法可确保我们所做的更改始终存在于磁盘上。如果没有同步，则由操作系统决定何时将文件刷新到磁盘。通过这种方法，我们可以确保即使在发生崩溃的情况下数据也始终可用。如果我们丢失任何数据，那也只会在崩溃后的一秒钟内发生，这是一个可以接受的速率。
// 如果想要 100% 的持久性，我们就不需要 goroutine。相反，我们会在每次执行命令时同步文件。但是，这会导致写入操作性能不佳，因为 IO 操作很昂贵。
func NewAof(path string) (*Aof, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	aof := &Aof{
		file: f,
		rd:   bufio.NewReader(f),
	}

	// Start a goroutine to sync AOF to disk every 1 second
	go func() {
		for {
			aof.mu.Lock()

			aof.file.Sync()

			aof.mu.Unlock()

			time.Sleep(time.Second)
		}
	}()

	return aof, nil
}

// 每当我们收到来自客户端的请求时，该方法将用于将命令写入 AOF 文件
// 我们使用与v.Marshal()收到的 RESP 格式相同的格式将命令写入文件。这样，当我们稍后读取文件时，我们可以解析这些 RESP 行并将其写回内存
func (aof *Aof) Write(value Value) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	_, err := aof.file.Write(value.Marshal())
	if err != nil {
		return err
	}

	return nil
}

func (aof *Aof) Read(fn func(value Value)) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	aof.file.Seek(0, io.SeekStart)

	reader := NewResp(aof.file)

	for {
		value, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		fn(value)
	}

	return nil
}

func (aof *Aof) Close() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	return aof.file.Close()
}
