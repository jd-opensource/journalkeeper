# JournalKeeper

[![License](https://img.shields.io/github/license/chubaostream/journalkeeper)](./LICENSE) [![Release](https://img.shields.io/github/v/release/chubaostream/journalkeeper)](https://github.com/chubaostream/journalkeeper/releases) [![Maven Central](https://img.shields.io/maven-central/v/io.journalkeeper/journalkeeper?color=blue)](https://search.maven.org/search?q=io.journalkeeper) [![Last commit](https://img.shields.io/github/last-commit/chubaostream/journalkeeper)](https://github.com/chubaostream/journalkeeper/commits) [![Travis](https://img.shields.io/travis/chubaostream/journalkeeper)](https://travis-ci.org/chubaostream/journalkeeper)

[View in English](./README.md)

JournalKeeper是一个高性能，高可靠，强一致性分布式流数据存储集群。JournalKeeper日志一致性算法衍生于RAFT一致性算法并做了扩展和改进，更加适用于超大规模集群，并且具有更好的性能。它将系统清晰分割成一致性日志、状态机和存储三个部分，使得每一部分都能符合单一职责原则。JournalKeeper明确定义了系统边界，使用更加系统化和结构化的描述方法来定义这个算法，使之易于完整正确的实现并应用到工程实践中。

## JournalKeeper是什么？

JournalKeeper是...

### 完整的RAFT实现类库

JournalKeeper实现并隐藏了RAFT大部分的复杂性，提供极简的API：[JournalKeeper RAFT API](./journalkeeper-docs/src/markdown/JournalKeeperAPI.md)，你可以使用JournalKeeper轻松实现一个自己的RAFT集群，参见示例：[journalkeeper-examples](./journalkeeper-examples)。

### 分布式流数据存储

JournalKeeper提供[Partitioned Journal Store API (JK-PS API)](./journalkeeper-docs/src/markdown/JournalKeeperAPI.md)用于存储流数据，可用于存储监控数据、Binlog等日志数据，为流计算提供可靠的数据流存储，也可以用于实现Pub/Sub系统。支持如下特性：

* 弹性多分区
* 强一致性
* 严格顺序
* 支持事务
* 高可靠、高可用和高性能

### 嵌入式分布式协调服务

JournalKeeper提供了[Journal Keeper Coordinating Service API(JK-CS API)](./journalkeeper-docs/src/markdown/JournalKeeperAPI.md)可以用于协调分布式系统，并且它内嵌在你的业务系统中，不需要单独部署和维护。JournalKeeper可以用于：

* 存储元数据，支持KV和SQL
* 领导人选举
* 监控节点状态
* 实现分布式锁
* 生成GUID

## JournalKeeper有哪些特性？

JournalKeeper的核心是一个面向大规模分布式集群优化过的，高性能的RAFT类库。它严格按照[RAFT论文](https://raft.github.io/raft.pdf)完整的实现了的全部特性，包括：

* 复制状态机
* 一致性日志复制
* 领导人选举
* 二阶段集群成员变更
* 自动定期创建状态机快照
* 复制和安装快照
* 日志压缩

此外，JournalKeeper还包含如下这些实用的特性：

* 多分区一致性日志：将一致性日志划分为多个逻辑分区，可以为每个分区建立单独的状态机，在保证因果一致性的前提上实现并行读写
* 日志并行复制：提升主从复制性能
* 从节点读：JournalKeeper在保证顺序一致性的前提下，支持客户端在从节点读取数据，提升集群读性能；
* 优先领导人：支持指定集群某个节点为优先领导人，在优先领导人节点正常的情况下，集群的选举策略总是将优先领导人节点选为领导人。这在Multi-Raft中，可以有效避免领导人倾斜问题
* 观察者：观察者只从集群复制日志并生成状态数据，可以提供读服务，但不参与集群选举。加入观察者可以在不影响集群写入和选举的性能前提下，提升集群读性能。

## 文档

参见 [这篇文章](journalkeeper-docs/src/markdown/JournalKeeperRaft.md)。

## API

参见 [JournalKeeper API](journalkeeper-docs/src/markdown/JournalKeeperAPI.md)。

## 参与贡献

JournalKeeper 期待创建一个完善的平台社区，欢迎提出任何想法和问题。

## 开源协议

遵循 [Apache License, 版本 2.0](https://www.apache.org/licenses/LICENSE-2.0).