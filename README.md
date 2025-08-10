# 项目名称
DataLakeDB 是一款使用RUST实现的分布式数据库系统，专为海量数据存储与实时分析场景设计。
系统采用主从架构实现水平扩展能力，既提供高效的批量数据处理能力，又具备实时流式数据消费特性，完美适配物联网、实时数仓等大数据应用场景
## 🚀 分布式架构

- 弹性扩展的节点集群，支持 PB 级数据存储
- 自动化的分区路由机制（根据创建表时的指定分区数量确定分区）
- 主从高可用架构，确保服务连续性

## 💡 混合存储引擎

- 列式存储优化分析查询性能
- 日志结构化存储保障写入吞吐
- 智能文件分段策略（根据slave.file.segment.mb配置）

## ⚡ 实时双模引擎

- 批量插入接口支持高吞吐数据导入
- 流式消费 API 实现毫秒级数据可见
- 精确的 offset 控制机制保障数据完整性

## 🔧 智能运维

- 在线表压缩去重（保留最近 2 个日志版本）
- 可视化集群状态监控（开发中）
- 自动负载均衡与故障转移

## 项目介绍
`````
├── config/                  # 集群配置中心
│   ├── master_config.properties  # 主节点配置（端口/数据目录/从节点列表）
│   └── slave_config.properties   # 从节点配置（节点信息/存储策略/文件分块大小）
├── data-lake-client/        # 智能客户端SDK
├── master/                  # 控制平面（元数据管理/负载均衡）
└── slave/                   # 数据平面（分布式存储引擎）
`````


### config：整个项目的配置文件
  <br>mester_config.properties
  <br>master.data.port : 表示master 的地址和端口
  <br>master.data.path ：表示mester的数据存储位置
  <br>slave.nodes ：表示slave的地址和端口
  <br> slave_config.properties
  <br>slave.node : slave的地址和端口
  <br>slave.data : slave的数据存储位置
  <br>slave.file.segment.bytes : slave每个文件块的大小（单位是mb）
### data-lake-client
  项目的客户端
### master
  项目的master
### slave 
  项目的slave
# 快速开始
使用 rust >= 1.85.0 
## 启动主节点
在master文件夹下执行： 
cargo run --release -- ${data-lake_path}/config/mester_config.properties

## 启动从节点
在slave文件夹下执行：
cargo run --release -- ${data-lake_path}/config/slave_config.properties

## 连接客户端
在data-lake-client文件夹下执行：
cargo run --release -- masterip:masterprot


# 开发者接口示例

## 创建表：
{"sql":"create table table_name(id INT PRIMARY KEY, username string NOT NULL, age long, xingbie string default '男') partition.number = 4, replicas.number=1"}
## 删除表
{"sql":"drop table_name"}
## 批量插入数据：
<br>{"batch_insert":{"data":[{"id":"0","_crud_type":"insert","username":"data-lake","age":"1","xingbie":"0"},{"id":"1","_crud_type":"insert","username":"data-lake","age":"1","xingbie":"0"},{"id":"2","_crud_type":"insert","username":"data-lake","age":"1","xingbie":"0"},{"id":"3","_crud_type":"insert","username":"data-lake","age":"1","xingbie":"0"},{"id":"4","_crud_type":"insert","username":"data-lake","age":"1","xingbie":"0"},{"id":"5","_crud_type":"insert","username":"data-lake","age":"1","xingbie":"0"},{"id":"6","_crud_type":"insert","username":"data-lake","age":"1","xingbie":"0"},{"id":"7","_crud_type":"insert","username":"data-lake","age":"1","xingbie":"0"},{"id":"8","_crud_type":"insert","username":"data-lake","age":"1","xingbie":"0"},{"id":"9","_crud_type":"insert","username":"data-lake","age":"1","xingbie":"0"},{"id":"10","_crud_type":"insert","username":"data-lake","age":"1","xingbie":"0"}],"table_name":"table_name"}}

## 指定分区号，批量插入数据：
<br>{"batch_insert":{"data":[{"id":"0","_crud_type":"insert","username":"data-lake","age":"1","xingbie":"0"},{"id":"1","_crud_type":"insert","username":"data-lake","age":"1","xingbie":"0"},{"id":"2","_crud_type":"insert","username":"data-lake","age":"1","xingbie":"0"},{"id":"3","_crud_type":"insert","username":"data-lake","age":"1","xingbie":"0"},{"id":"4","_crud_type":"insert","username":"data-lake","age":"1","xingbie":"0"},{"id":"5","_crud_type":"insert","username":"data-lake","age":"1","xingbie":"0"},{"id":"6","_crud_type":"insert","username":"data-lake","age":"1","xingbie":"0"},{"id":"7","_crud_type":"insert","username":"data-lake","age":"1","xingbie":"0"},{"id":"8","_crud_type":"insert","username":"data-lake","age":"1","xingbie":"0"},{"id":"9","_crud_type":"insert","username":"data-lake","age":"1","xingbie":"0"},{"id":"10","_crud_type":"insert","username":"data-lake","age":"1","xingbie":"0"}],"partition_code":"0","table_name":"table_name"}}
- 如果使用了 partition_code 参数，指定了插入的批量数据的分区，尽量与data-lake master 的分区方式一致:
`````
 fn hash_code(&self) -> i32 {
  let mut hash = 0i32;
  let multiplier = 31;
  for c in self.chars() {
  let char_value = c as i32;
  hash = hash.wrapping_mul(multiplier).wrapping_add(char_value);
  }
  return hash.abs();
  }
`````
也就是java 的String的hashCode 方法，并对hashcode 取绝对值


[//]: # (## 查询表内的数据：)
[//]: # (<br>{"sql":"select username, age from table_name"})
## 添加列
<br>{"sql":"ALTER TABLE table_name ADD username_a string"}
## 删除列
<br>{"sql":"ALTER TABLE table_name OROP username_a"}
## 查看表的元数据
<br> {"sql":"desc table_name"}

## 查看表内每个分区的最大offset
<br> {"sql":"max_offset table_name"}

## 查看所有表
<br> {"sql":"show_table"}

## 消费表内的数据：
### 从头开始消费：
{"stream_read":{"patition_mess":[],"table_name":"table_name","read_count":1000}}
### 从指定的offset处开始消费：
{"stream_read":{"patition_mess":[{"patition_code":0,"offset":249991},{"patition_code":1,"offset":249991},{"patition_code":2,"offset":249991},{"patition_code":3,"offset":249991}],"table_name":"table_name","read_count":1000}}


# 高级功能
## 对表根据主键唯一id进行压缩去重：（会留下最新的两个snappy文件）
<br>{"sql":"compress table_name"}



# java客户端
https://github.com/shenxushenxu/data-lake-java-client

# 持续开发中
1. 实现标准SQL查询接口支持(开发中.....)
2. 添加副本机制(完成)
3. 支持多块硬盘存储数据(完成)
4. 引入磁盘缓存机制优化批量查询资源利用率(完成)
5. 构建完善的异常处理与错误日志系统
6. 集成Snappy压缩算法降低网络传输带宽占用（完成）
7. 开发实时集群状态可视化监控仪表盘
