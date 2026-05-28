## USFCA Machines
21 machines for storage and computation: orion01-12 except 11, mc01-10 \
controller and master runs on orion11

## Architecture
<div align="center">
  <img src="dfs.png" alt="dfs architecture" width="42%" />
  <img src="mapreduce.png" alt="mapreduce architecture" width="42%" />
</div>

dfs base directory = /bigdata/students/whsiao5/mydfs/ \
chunk path: filename/chunk_<chunk_id> \
checksum path: filename/chunk_<chunk_id>.chksum

mapreduce base directory = /bigdata/students/whsiao5/mr/ \
job directory = <job_id> \
plugin file: plugin.so \
map: <mapper_id>  = <file_name>-<chunk_id> \
&emsp;spill file with index: spill-<mapper_id>-<spill_idx>, index-<mapper_id>-<spill_idx> \
&emsp;intermediate file with index: inter-<mapper_id>, index-<mapper_id> \
shuffle: \
&emsp;fetched file: fetched-<mapper_id>-<reducer_idx> \
&emsp;sorted file: sorted-<reducer_idx> \
&emsp;merged file: merged-<reducer_idx> \
reduce: \
&emsp;result file: res-<job_id>-<reducer_idx> (stored in dfs)

### Mapper
1. retrieve a file chunk locally (if failed, use dfs client to retrieve)
2. use map() to convert into key-value pairs and store into a buffer
3. when the buffer is full, partition key-value pairs by key and sort on memory, then write into a spill file and an index file \
eventually, there are multple spill files with sorted+partitioned data and index files indicating data range for reducers
4. after finishing reading the file chunk, refer to index files and apply k-way merge to merge spill files into an intermediate file and an index file \
k is the number of spill files \
spill files and index files for spill are removed
5. return an array of number of bytes for each reducer, which is used for load balancing
### Reducer
1. iterate through a map of mapper information (where the mappper was), refer to an index file and fetch data from an intermediate file to a fetched file \
fetch is either local or remote based on mapper information \
eventually, there are multiple fetched files
2. apply k-way merge to merge fetched files into a sorted file, then remove fetched files
3. group key-value pairs into key-array by key and write into a merged file, then remove the sorted file
4. use reduce() to compute arrays and write results into a result file
5. upload the result file to dfs by using dfs client
### Load Balance
- each reducer is set to process at least five file chunks, also meaning five mappers or five intermediate files*
- each node is set to have at most reducer, so there are about 100 reducers in total at most*
- it selects a node with *least active tasks* and enough resources for a mapper from where a file chunk is stored
- it selects a node with *most data* and enough resources for a reducer from where a mapper runs

*if client set number of reducers to 0

## DFS Command
### Client
```
dfs/bin/client orion11:39039 put file --chunk-size 104857600
dfs/bin/client orion11:39039 get file ~/downloads
dfs/bin/client orion11:39039 delete file
dfs/bin/client orion11:39039 list
dfs/bin/client orion11:39039 nodes
```
`--chunk-size 104857600` is optional, range is from 64-256 MiB \
`list` shows all files \
`nodes` shows all active nodes' status

## MapReduce Command
### Client
```
mapreduce/bin/client orion11:39079 input.txt,input1.txt,input2.txt wordcount.so 64
```
`64` is the number of reducers
### Others
```
mapreduce/bin/download_results orion11:39039 1778919896880819662 unique-domains
mapreduce/bin/total_unique unique-domains
mapreduce/bin/delete_results orion11:39039 1778919896880819662
```
`1778919896880819662` is a job id
`download_results` pulls down all results files of a job from dfs
`delete_results` removes all results files of a job on dfs

### Cluster
#### Start
```
./cluster.sh dfs start
./cluster.sh dfs start controller
./cluster.sh dfs start server orion01
```
```
./cluster.sh mr start
./cluster.sh mr start master
./cluster.sh mr start worker orion01
```
#### Stop

```
./cluster.sh dfs stop
./cluster.sh dfs stop controller 
./cluster.sh dfs stop server orion01
```
```
./cluster.sh mr stop
./cluster.sh mr stop master
./cluster.sh mr stop worker orion01
```
#### Status
show whether nodes are active or not
```
./cluster.sh status
```
#### Clean
remove all storage directories
```
./cluster.sh dfs clean
```
remove all mapreduce workspace directories
```
./cluster.sh mr clean
```

## Improvement
1\. load balance \
&emsp;Master waits for 2-6 seconds before assigning a task to make load balance decision based on updated resources, but it can still use same snapshot of resources if waiting time is the same \
&emsp;Master should have updated workers' statistics on memory for load balance, then merge with those in heartbeat \
2\. fault tolerance \
&emsp;A task is skipped if failed, but master is supposed to reassign it \
3\. separate load balance into a resource manager \
4\. gRPC \
5\. log \
&emsp;Import a log library to provide better log format and prevent commenting code