# JournalKeeper

JournalKeeper是一个高性能，高可靠，强一致性分布式流数据存储集群。JournalKeeper日志一致性算法衍生于RAFT一致性算法并做了扩展和改进，更加适用于超大规模集群，并且具有更好的性能。它将系统清晰分割成一致性日志、状态机和存储三个部分，使得每一部分都能符合单一职责原则。JournalKeeper明确定义了系统边界，使用更加系统化和结构化的描述方法来定义这个算法，使之易于完整正确的实现并应用到工程实践中。

## 实现原理

参见[这篇文章](journalkeeper-docs/src/markdown/JournalKeeperRaft.md)。

## API

参见[JournalKeeper API](journalkeeper-docs/src/markdown/JournalKeeperAPI.md)。

## 参与贡献

JournalKeeper 期待创建一个完善的平台社区，欢迎提出任何想法和问题。

## 开源协议

遵循 Apache License, 版本 2.0:https://www.apache.org/licenses/LICENSE-2.0
