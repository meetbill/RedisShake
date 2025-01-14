# file_writer

## 介绍

可以使用 ` file_writer` 写文件, 可写的格式有 CMD/JSON/AOF, 常用于通过文件介质抽取/迁移/订正数据.
## 配置

```toml
[file_writer]
filepath = "/tmp/cmd.txt"
type = "cmd" #cmd,aof,json (default cmd)
```

* 绝对路径 filepath 是必填的.

## 应用场景
- 俩系统共享数据: 一个系统把文件写到 disk/s3/oss, 另一系统从中读取.
- 跨系统局部迁移带指定前缀的数据: 从A系统迁出带前缀"XXX:"的数据, B系统通过命令导入这些数据 `redis-cli --pipe XXX.aof` .
- 通过命令文件订正数据: 从一个系统中导出数据成cmd格式, 订正后再导入命令`redis-cli < cmd.txt`.
- 通过json格式做数据分析: 导出成json文件, 导入到mongodb/bi做分析.

## 示例输出
### cmd_writer 输出:
```
SELECT 0
set key1 1
set key2 2
set key3 3
sadd key4 1 2 3 4
lpush key5 1 2 3 4 5
zadd key6 1 2 3 4 5 6
```
### json_writer 输出:
```
{"DbId":0,"Argv":["SELECT","0"],"CmdName":"SELECT","Group":"CONNECTION","Keys":null,"KeyIndexes":null,"Slots":[],"SerializedSize":23}
{"DbId":0,"Argv":["set","key1","1"],"CmdName":"SET","Group":"STRING","Keys":["key1"],"KeyIndexes":[2],"Slots":[9189],"SerializedSize":30}
{"DbId":0,"Argv":["set","key2","2"],"CmdName":"SET","Group":"STRING","Keys":["key2"],"KeyIndexes":[2],"Slots":[4998],"SerializedSize":30}
{"DbId":0,"Argv":["set","key3","3"],"CmdName":"SET","Group":"STRING","Keys":["key3"],"KeyIndexes":[2],"Slots":[935],"SerializedSize":30}
{"DbId":0,"Argv":["sadd","key4","1","2","3","4"],"CmdName":"SADD","Group":"SET","Keys":["key4"],"KeyIndexes":[2],"Slots":[13120],"SerializedSize":52}
{"DbId":0,"Argv":["lpush","key5","1","2","3","4","5"],"CmdName":"LPUSH","Group":"LIST","Keys":["key5"],"KeyIndexes":[2],"Slots":[9057],"SerializedSize":60}
{"DbId":0,"Argv":["zadd","key6","1","2","3","4","5","6"],"CmdName":"ZADD","Group":"SORTED_SET","Keys":["key6"],"KeyIndexes":[2],"Slots":[4866],"SerializedSize":66}
```
### aof_writer 输出:
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
