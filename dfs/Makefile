all: client server controller

client: client/client.go messages/message_handler.go util/util.go
	go build -o bin/client client/client.go

server: server/server.go messages/message_handler.go util/util.go
	go build -o bin/server server/server.go

controller: controller/controller.go controller/wal.go messages/message_handler.go util/util.go util/mapper.go
	go build -o bin/controller controller/controller.go controller/wal.go

clean:
	rm -rf bin/
