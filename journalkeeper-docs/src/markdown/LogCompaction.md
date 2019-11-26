# 日志删除

为了避免无限增长的日志占满磁盘空间，JournalKeeper需要删除旧日志。日志删除的实现思路总体上参考了[RAFT论文](https://raft.github.io/raft.pdf)中**Log Compaction**一节。使用基于快照来删除日志的方法。


## 快照

在JournalKeeper中快照是，状态（State）在日志（Journal）中某个索引序号上的副本，每个快照对应了日志中的一个固定的索引序号，快照一旦生成不再改变。

快照中包含：

1. User State：用户状态，即状态机中的状态数据；
2. Internal State：JournalKeeper自身的状态数据
   1. lastIncludedIndex & lastIncludedTerm：快照指向的上一条日志的索引序号和这条日志的任期；
   2. config：集群配置，即集群中有哪些节点；
   3. partitions & indices：Journal的分区配置和每个分区当前的索引序号
   4. minOffset：Journal当前最小偏移量
   5. preferredLeader：推荐Leader
   6. timestamp：快照的时间戳

## 快照初始化

每个节点在初始化时都需要创建一个初始快照，这个初始化快照随着节点第一次初始化生成。lastIncludedIndex的初始值为-1，lastIncludedTerm的初始值为-1。

### 同步创建快照

在JournalKeeper中，每个节点自行创建的快照，由LEADER统一指挥。需要创建快照时，节点当前的LEADER写入一条特殊的“CREATE_SNAPSHOT”日志，每个节点（包括LEADER节点）在执行状态机阶段，如果当前执行的日志为“CREATE_SNAPSHOT”日志，则执行创建快照操作。

创建快照的方式就是把当前的状态在磁盘中的所有文件，复制到快照文件夹中。快照对应的lastIncludedIndex一定是“CREATE_SNAPSHOT”日志之前的那条日志的索引序号。

这样，可以确保集群中所有节点快照的数量和对应的日志索引序号是一致的。

### 日志复制与安装快照

主从复制时需要比较FOLLOWER和LEADER的日志，如果不一致就向前回退，直到找到双方一致的日志位置，从这个位置开始主从复制。有一种情况是，回退到了LEADER最旧的一个快照对应的日志位置，LEADER需要发起InstallSnapshot RPC，给FOLLOWER安装第一个快照，然后从这个快照位置开始主从复制。

## 删除日志

考虑到集群中各节点的配置不一定相同，JournalKeeper允许集群中所有节点自行决定日志删除的时机以及保留多少日志。但删除日志时必须满足：

1. 每次都只能删除最旧的一部分日志；
2. 不允许删除未提交的日志；
3. 同步删除日志和快照数据。也就是说，如果删除某条日志的对应的位置存在一个快照，那这个快照也要同步删除；
4. 至少保留1个快照；