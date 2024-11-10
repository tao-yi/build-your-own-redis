package main

import (
	"fmt"
	"log/slog"
	"net"
	"strings"
)

func main() {
	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		slog.Error("failed to listen on", "port", 6379)
		return
	}

	aof, err := NewAof("database.aof")
	if err != nil {
		slog.Error(err.Error())
		return
	}
	defer aof.Close()

	aof.Read(func(value Value) {
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]
		handler, ok := Handlers[command]
		if !ok {
			slog.Error(err.Error())
			return
		}
		handler(args)
	})

	slog.Info("Listening on port :", "port", 6379)
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		go handleConn(conn, aof)
	}
}

func handleConn(conn net.Conn, aof *Aof) {
	defer conn.Close()
	for {
		resp := NewResp(conn)
		value, err := resp.Read()
		if err != nil {
			slog.Error("failed to read", "command", err)
			return
		}

		if value.typ != "array" {
			slog.Info("Invalid request, expected array")
			continue
		}

		if len(value.array) == 0 {
			slog.Info("Invalid request, expected array length > 0")
			continue
		}

		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]
		writer := NewWriter(conn)
		handler, ok := Handlers[command]
		if !ok {
			slog.Info("Invalid command: ", "command", command)
			writer.Write(Value{typ: "string", str: ""})
			continue
		}

		if command == "SET" || command == "HSET" {
			if err = aof.Write(value); err != nil {
				slog.Error(err.Error())
			}
		}

		result := handler(args)
		writer.Write(result)
	}
}
