# JournalKeeper

JournalKeeper is a high performance, highly reliable, strong and consistent distributed streaming data storage cluster. The JournalKeeper log consistency algorithm is derived from the RAFT consistency algorithm and has been extended and improved to be more suitable for very large scale clusters with better performance. It clearly divides the system into three parts: consistency log, state machine and storage, so that each part can conform to the principle of single responsibility. JournalKeeper clearly defines system boundaries, using a more systematic and structured description method to define this algorithm, making it easy to implement and apply it to engineering practice.

JournalKeeper是一个高性能，高可靠，强一致性分布式流数据存储集群。JournalKeeper日志一致性算法衍生于RAFT一致性算法并做了扩展和改进，更加适用于超大规模集群，并且具有更好的性能。它将系统清晰分割成一致性日志、状态机和存储三个部分，使得每一部分都能符合单一职责原则。JournalKeeper明确定义了系统边界，使用更加系统化和结构化的描述方法来定义这个算法，使之易于完整正确的实现并应用到工程实践中。

## Documentation

Visit [This post(In Chinese)](journalkeeper-docs/src/markdown/JournalKeeperRaft.md)。

## API

Visit [JournalKeeper API(In Chinese)](journalkeeper-docs/src/markdown/JournalKeeperAPI.md)。

## Contributing

We are dedicate to building high-quality messaging platform product. So any thoughts, pull requests, or issues are appreciated.

JournalKeeper 期待创建一个完善的平台社区，欢迎提出任何想法和问题。

## License

Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).

遵循 [Apache License, 版本 2.0](https://www.apache.org/licenses/LICENSE-2.0).
