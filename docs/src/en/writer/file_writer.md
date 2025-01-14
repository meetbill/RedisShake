# file_writer

## Introduction

Can use ` file_writer ` to write data to file with type CMD/JSON/AOF .
It is commonly used to extract/migrate/fix data by file.

## configuration

```toml
[file_writer]
filepath = "/tmp/cmd.txt"
type = "cmd" #cmd,aof,json (default cmd)
```

* An absolute filepath should be passed in.
## application scenarios
- share data between two system: one system write aof to disk/s3/oss, another system read file from them.
- partial migrate data with business prefix: extract aof with prefix "XXX:" data from A system, B system import the aof with command `redis-cli --pipe XXX.aof` .
- fix data by cmd file: export cmd data from one system, fix wrong data, and then import cmd file with command `redis-cli < cmd.txt`.
- analysis data with json: export json file, and then import them into mongodb/bi to analysis.

## example output:
### cmd_writer output:
```
SELECT 0
set key1 1
set key2 2
set key3 3
sadd key4 1 2 3 4
lpush key5 1 2 3 4 5
zadd key6 1 2 3 4 5 6
```
### json_writer output:
```
{"DbId":0,"Argv":["SELECT","0"],"CmdName":"SELECT","Group":"CONNECTION","Keys":null,"KeyIndexes":null,"Slots":[],"SerializedSize":23}
{"DbId":0,"Argv":["set","key1","1"],"CmdName":"SET","Group":"STRING","Keys":["key1"],"KeyIndexes":[2],"Slots":[9189],"SerializedSize":30}
{"DbId":0,"Argv":["set","key2","2"],"CmdName":"SET","Group":"STRING","Keys":["key2"],"KeyIndexes":[2],"Slots":[4998],"SerializedSize":30}
{"DbId":0,"Argv":["set","key3","3"],"CmdName":"SET","Group":"STRING","Keys":["key3"],"KeyIndexes":[2],"Slots":[935],"SerializedSize":30}
{"DbId":0,"Argv":["sadd","key4","1","2","3","4"],"CmdName":"SADD","Group":"SET","Keys":["key4"],"KeyIndexes":[2],"Slots":[13120],"SerializedSize":52}
{"DbId":0,"Argv":["lpush","key5","1","2","3","4","5"],"CmdName":"LPUSH","Group":"LIST","Keys":["key5"],"KeyIndexes":[2],"Slots":[9057],"SerializedSize":60}
{"DbId":0,"Argv":["zadd","key6","1","2","3","4","5","6"],"CmdName":"ZADD","Group":"SORTED_SET","Keys":["key6"],"KeyIndexes":[2],"Slots":[4866],"SerializedSize":66}
```
### aof_writer output:
```
*2
$6
SELECT
$1
0
*3
$3
set
$4
key1
$1
1
```
