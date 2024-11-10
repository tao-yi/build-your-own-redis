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

/*
The idea of syncing every second ensures that the changes we made are always present on disk.
Without the sync, it would be up to the OS to decide when to flush the file to disk.
With this approach, we ensure that the data is always available even in case of a crash.
If we lose any data, it would only be within the second of the crash, which is an acceptable rate.
If you want 100% durability, we won’t need the goroutine.
Instead, we would sync the file every time a command is executed.
However, this would result in poor performance for write operations because IO operations are expensive.
*/
func NewAof(path string) (*Aof, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	aof := &Aof{
		file: f,
		rd:   bufio.NewReader(f),
	}
	// start a goroutine to sync AOF to disk every 1 second
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

// Close ensures that the file is properly closed when the server shuts down.
func (aof *Aof) Close() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	return aof.file.Close()
}

// Write the command to the AOF file whenever we receive a request from the client.
func (aof *Aof) Write(value Value) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	_, err := aof.file.Write(value.Marshal())
	return err
}

func (aof *Aof) Read(callback func(value Value)) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()
	resp := NewResp(aof.file)
	for {
		value, err := resp.Read()
		if err == nil {
			callback(value)
		}
		if err == io.EOF {
			break
		}
		return err
	}
	return nil
}
