# Project 2

https://www.cs.usfca.edu/~mmalensek/cs677/assignments/project-2.html

## USFCA Machines
21 machines: orion01-12 except 11, mc01-10 \
controller runs on orion11 \
each node's storage directory is under /bigdata/students/USERNAME/MACHINE_NAME (i.e. /bigdata/students/whsiao5/orion01)

## Command
### Client
```
dfs/bin/client orion11:39039 put file --chunk-size 139
```
`--chunk-size 139` is optional, range is from 64-256 MiB
```
dfs/bin/client orion11:39039 get file ~/downloads
```
```
dfs/bin/client orion11:39039 delete file
```
```
dfs/bin/client orion11:39039 list
```
`list` shows all files
```
dfs/bin/client orion11:39039 nodes
```
`nodes` shows all active nodes' status

### Cluster
#### Start
```
./cluster.sh start
```
```
./cluster.sh start controller
```
```
./cluster.sh start server orion01
```
#### Stop

```
./cluster.sh stop
```
```
./cluster.sh stop controller 
```
```
./cluster.sh stop server orion01
```
#### Status
show whether nodes are active or not
```
./cluster.sh status
```
#### Clean
remove all storage directories
```
./cluster.sh clean
```