# JournalKeeper API

JournalKeeper以接口和事件方式对外提供服务，服务的形式为进程内调用。

![How JournalKeeper cooperating with your clusters](images/cooperating.png)

## 角色

集群所有节点可以配置成如下三种角色中的一种，三种角色为用户提供的API功能完全相同，区别在于本地调用还是远程调用以及访问数据的性能。

角色 | 本地存储数据 | 参与选举 | 说明
-- | -- | -- | --
VOTER | Y| Y| 选民，所有选民构成RAFT集群，拥有选举权和被选举权的节点，可以成为LEADER、FOLLOWER或CANDIDATE三种状态。选民节点数量必须为奇数，建议设置为3/5/7。
OBSERVER | Y| N| 观察者，没有选举权和被选举权的节点，本地存储日志和数据，提供只读服务，从其它选民或者观察者复制日志，更新状态。数据不大的情况下，建议除选民节点外，所有节点都设为观察者。
CLIENT | N| N| 客户端，本地不存储数据，从选民或者观察者节点读取数据，适用于数据量较大的集群。

## RAFT API(JK-RAFT API)

JK-RAFT API包括变更状态服务和读取状态服务。

update方法用于写入操作命令，每条操作命令就是日志中的一个条目，操作命令在安全写入日志后，将在状态机中执行，执行的结果将会更新状态机中的状态。

query方法用于查询状态机中的数据。

例如，状态机是一个关系型数据库（RDBMS），数据库中的数据就是“状态”，或者成为状态数据。在这里，可以把更新数据的SQL（UPDATE，INSERT, ALTER 等）作为操作命令，调用update方法去在JournalKeeper集群中执行。JournalKeeper将操作命令复制到集群的每个节点上，并且可以保证在每个节点，用一样的顺序去执行这些SQL，更新每个节点的数据库，当这些操作命令都在每个节点的数据库中执行完成后，这些节点的数据库中必然有相同的数据。

如果需要查询数据库中的数据，可以把查询SQL（SELECT）作为查询条件，调用query方法，JournalKeeper将查询命令发送给当前LEADER节点的状态机，也就是数据库，去执行查询SQL语句，然后将结果返回给客户端。

### 接口

方法 | 说明
-- |  --
update | 写入操作命令变更状态。
query |  查询状态机中的状态数据。

具体见：[io.journalkeeper.core.api.RaftClient](https://github.com/chubaostream/journalkeeper/blob/master/journalkeeper-core-api/src/main/java/io/journalkeeper/core/api/RaftClient.java)

### 事件

事件 | 参数 | 说明
-- | -- | --
onStateChanged | lastApplied: 当前状态对应日志位置<br/>自定参数 | 集群状态变更

### 状态机

用户需要实现一个状态机，用于执行操作命令和查询状态数据。

具体见：[io.journalkeeper.core.api.State](https://github.com/chubaostream/journalkeeper/blob/master/journalkeeper-core-api/src/main/java/io/journalkeeper/core/api/State.java)

## Partitioned Journal Store API (JK-PS API)

Partitioned Journal Store 维护一个多分区、高可靠、高可用、强一致的、分布式WAL日志。

WAL日志具有如下特性：

* 尾部追加：只能在日志尾部追加写入条目</li>
* 不可变：日志写入成功后不可修改，不可删除</li>
* 严格顺序：日志严格按照写入的顺序排列</li>
* 线性写入：数据写入是线性的，任一时间只能有一个客户端写入。</li>

JournalKeeper支持将一个Journal Store划分为多个逻辑分区，每个分区都是一个WAL日志，分区间可以并行读写。

### 接口

方法 |  说明
-- |  --
append | 写入日志。
get | 根据索引序号查询日志。
minIndices | 当前已提交日志最小索引序号。
maxIndices | 当前已提交日志最大索引序号。
listPartitions | 查询当前所有分区。
queryIndex | 根据时间查询索引序号。

具体见：[io.journalkeeper.core.api.PartitionedJournalStore](https://github.com/chubaostream/journalkeeper/blob/master/journalkeeper-core-api/src/main/java/io/journalkeeper/core/api/PartitionedJournalStore.java)

### 事件

事件 | 内容 | 说明
-- | -- | --
onJournalChanged | partition: 分区。<br/>minIndex: 当前日志最小位置。<br/> maxIndex：当前日志最大位置。| 日志变更后触发。

## JournalKeeper Admin API（JK-A API）

集群配置和管理相关的接口和事件。

### 接口

方法 | 说明
-- | --
getClusterConfiguration | 获取集群配置，包括集群中的所有节点以及当前的LEADER。
updateVoters | 变更集群配置。
convertRoll | 转换节点的角色。
scalePartitions | 变更集群分区配置。
getServerStatus | 获取节点当前的状态。
setPreferredLeader | 设置集群推荐Leader。

具体见：[io.journalkeeper.core.api.AdminClient](https://github.com/chubaostream/journalkeeper/blob/master/journalkeeper-core-api/src/main/java/io/journalkeeper/core/api/AdminClient.java)


### 事件

事件 | 内容 | 说明
-- | -- | --
onLeaderChanged | leader：当前LEADER<br/>term：当前任期 | LEADER节点变更
onVotersChanged | voterAddrs：变更后的所有选民节点 | 选民节点配置变更

## Journal Keeper Coordinating Service(JK-CS API)[TODO]

分布式一致性协调服务相关的接口和事件。

### 接口

方法 | 说明
-- | --
set | 设置指定的key的为给定的值，如果key不存在则自动创建，如果key已存在则覆盖。
get | 获取指定key的值。
list | 根据模式返回匹配的key数组。
exist | 判断给定的key是否存在
remove | 删除指定的key
compareAndSet | 如果指定的key值与提供的值相等，则执行set操作。
watch | 监控一组key，当符合条件的任一一个key对应值变化时给出通知并结束watch。
unWatch | 取消监控

### 事件

事件 | 内容 | 说明
-- | -- | --
onWatch | watchId：监控唯一ID<br/> keyAndValues[]：变更键值数组，数组每个元素包括 {key, oldValue, newValue} | watch: 监控的key发生变化时<br/> watchAndSet: 监控的key等于期望值时