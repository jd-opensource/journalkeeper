# 监控JournalKeeper

## 监控

JournalKeeper提供RESTful API用于监控集群每个Server节点的状态。

### 获取节点信息


属性 | 数据类型 | 名称 | 说明
 -- | -- | -- | --
uri | URI | 节点URI |
state | String | 节点状态 | 枚举: <br/> CREATED, STARTING, RUNNING, STOPPING, STOPPED, START_FAILED, STOP_FAILED
roll | String | 角色 | 枚举: <br/> VOTER, OSERVER
leader | URI | LEADER |当前节点中保存的LEADER URI
nodes.jointConsensus | Boolean | JointConsensus | 标识是否处于集群节点配置变更的中间状态
nodes.config | Array of URI | 集群当前配置 | nodes.jointConsensus为false时有效
nodes.config.old | Array of URI | 集群旧配置 | nodes.jointConsensus为true时有效
nodes.config.new | Array of URI | 集群新配置 | nodes.jointConsensus为true时有效
journal.partitions.config | Array of Number | Journal当前分区 |
journal.minIndex | Number | 最小索引序号 |
journal.maxIndex | Number | 最大索引序号 |
journal.flushIndex | Number | 刷盘索引序号 |
journal.commitIndex | Number | 已提交索引序号 |
journal.appliedIndex | Number | 状态机执行索引序号 |
journal.partitions.[partition].minIndex | Number | 分区最小索引序号 |
journal.partitions.[partition].maxIndex | Number | 分区最大索引序号 |
journal.persistence.minOffset | Number | Journal存储最小物理位置| 
journal.persistence.maxOffset | Number | Journal存储最大物理位置| 
journal.persistence.flushOffset | Number | Journal存储物理刷盘位置| 
journal.persistence.index.minOffset | Number | 索引存储最小物理位置| 
journal.persistence.index.maxOffset | Number | 索引存储最大物理位置| 
journal.persistence.index.flushOffset | Number | 索引存储物理刷盘位置| 
journal.persistence.partitions.[partition].minOffset | Number | 分区索引存储最小物理位置| 
journal.persistence.partitions.[partition].maxOffset | Number | 分区索引存储最大物理位置| 
journal.persistence.partitions.[partition].flushOffset | Number | 分区索引存储物理刷盘位置|
voter.term | Number | 选举任期
voter.state | String | 候选人状态 | 枚举：LEADER, FOLLOWER, CANDIDATE
voter.election.lastVote | URI | 投票候选人 | 在当前任期内投票给了哪个候选人，如果未投票可以为NULL。
voter.election.timeout | Number | 选举超时 | 单位为：毫秒（ms）
voter.election.nextElectionTime | Timestamp | 下次发起选举的时间 | 仅当voter.state为CANDIDATE的时候有效
voter.lastHeartbeat | Timestamp | 上次心跳时间 | 记录的上次从LEADER收到的心跳时间
voter.preferredLeader | URI | 推荐LEADER|
voter.leader.state | String | 当前节点LEADER状态 | 枚举: <br/> CREATED, STARTING, RUNNING, STOPPING, STOPPED, START_FAILED, STOP_FAILED
voter.leader.requestQueueSize | Number | 请求队列排队数 | 写入请求队列当前排队数量。所有写入请求先进入这个队列然后再异步串行处理，如何这个数量持续保持高位，说明写入积压。
voter.leader.writeEnabled | Boolean | 是否可写 | 正常情况为true可写，管理员可以通过调用接口禁止写入。
voter.leader.followers.[n].uri | URI | LEADER的从节点URI | 当前LEADER节点记录的FOLLOWER节点的URI
voter.leader.followers.[n].nextIndex | Number | 下一次复制索引序号 | 需要发给FOLLOWER的下一个日志条目的索引（初始化为领导人上一条日志的索引值 +1）
voter.leader.followers.[n].matchIndex | Number | 已复制索引序号 | 已经复制到该FOLLOWER的日志的最高索引值（从 0 开始递增）
voter.leader.followers.[n].repStartIndex | Number | 在途复制请求索引起始值 | 所有在途的日志复制请求中日志位置的最小值（初始化为nextIndex）
voter.leader.followers.[n].lastHeartbeatResponseTime | Timestamp | 心跳响应时间 |上次从FOLLOWER收到心跳（asyncAppendEntries）成功响应的时间戳
voter.leader.followers.[n].lastHeartbeatRequestTime | Timestamp | 心跳发送时间|上次发给FOLLOWER心跳（asyncAppendEntries）的时间戳
voter.follower.state | String| 当前节点FOLLOWER状态 | 枚举: <br/> CREATED, STARTING, RUNNING, STOPPING, STOPPED, START_FAILED, STOP_FAILED
voter.follower.replicationQueueSize | Number| 主从复制队列排队数 | 所有从LEADER发送过来的asyncAppendEntries Request（含心跳）都入队后处理，如果这个排队数量一直保持在高位说明，当前从节点写入数据速度跟不上LEADER节点的写入速度
voter.follower.leaderMaxIndex | Number |  LEADER节点最大索引序号 | 当前FOLLOWER节点记录的LEADER节点最大索引序号

### 节点当前配置

