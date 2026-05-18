.PHONY: all clean client worker master plugin la

all: client worker master plugin la

client: client/client.go messages/message_handler.go
	go build -o bin/client client/client.go

worker: worker/worker.go messages/message_handler.go util/kv.go util/sort.go util/sys_stats.go
	go build -o bin/worker worker/worker.go

master: master/master.go messages/message_handler.go
	go build -o bin/master master/master.go

plugin: plugin/unique_domains.go plugin/unique_ips.go plugin/unique_urls.go plugin/wordcount.go util/kv.go
	go build -o bin/unique_domains.so -buildmode=plugin plugin/unique_domains.go
	go build -o bin/unique_ips.so -buildmode=plugin plugin/unique_ips.go
	go build -o bin/unique_urls.so -buildmode=plugin plugin/unique_urls.go
	go build -o bin/wordcount.so -buildmode=plugin plugin/wordcount.go

# log analyzer
la: client/download_results.go client/delete_results.go client/top5_ips.go client/top10_domains.go client/total_unique.go
	go build -tags downloads -o bin/download_results client/download_results.go
	go build -tags delete -o bin/delete_results client/delete_results.go
	go build -tags top10_domains -o bin/top10_domains client/top10_domains.go
	go build -tags top5_ips -o bin/top5_ips client/top5_ips.go
	go build -tags total_unique -o bin/total_unique client/total_unique.go

clean:
	rm -rf bin/
