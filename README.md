# Project 2

https://www.cs.usfca.edu/~mmalensek/cs677/assignments/project-2.html

## USFCA Machines
21 machines for storage and computation: orion01-12 except 11, mc01-10 \
controller and master runs on orion11

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