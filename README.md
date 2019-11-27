
# JournalKeeper

[![License](https://img.shields.io/github/license/chubaostream/journalkeeper)](./LICENSE) [![Release](https://img.shields.io/github/v/release/chubaostream/journalkeeper)](https://github.com/chubaostream/journalkeeper/releases) [![Maven Central](https://img.shields.io/maven-central/v/io.journalkeeper/journalkeeper?color=blue)](https://search.maven.org/search?q=io.journalkeeper) [![Last commit](https://img.shields.io/github/last-commit/chubaostream/journalkeeper)](https://github.com/chubaostream/journalkeeper/commits) [![Travis](https://img.shields.io/travis/chubaostream/journalkeeper)](https://travis-ci.org/chubaostream/journalkeeper) [![Coverage Status](https://coveralls.io/repos/github/chubaostream/journalkeeper/badge.svg)](https://coveralls.io/github/chubaostream/journalkeeper)

[中文版](./README_cn.md)

JournalKeeper is a high performance, highly reliable, strong and consistent distributed streaming data storage cluster. The JournalKeeper log consistency algorithm is derived from the RAFT consistency algorithm and has been extended and improved to be more suitable for very large scale clusters with better performance. It clearly divides the system into three parts: consistency log, state machine and storage, so that each part can conform to the principle of single responsibility. JournalKeeper clearly defines system boundaries, using a more systematic and structured description method to define this algorithm, making it easy to implement and apply it to engineering practice.

## What is JournalKeeper？

JournalKeeper is a ...

### Complete RAFT implementation

JournalKeeper implements and hides most of the complexity of RAFT, and provides a minimal API: [JournalKeeper RAFT API](./journalkeeper-docs/src/markdown/JournalKeeperAPI.md). Take a look at [journalkeeper-examples](./journalkeeper-examples).

### Distributed streaming data storage

JournalKeeper provides [Partitioned Journal Store API (JK-PS API)](./journalkeeper-docs/src/markdown/JournalKeeperAPI.md) which were used to store sreaming data. It can be used to store monitoring data, log data such as Binlog, and provides streaming data storage for stream computing. It can also be used to implement Pub / Sub systems. The following features are supported:

* Elastic muti-partition journal
* Strong consistency
* Strict order
* Transaction write
* High reliability, high availability, and high performance

### In process distributed coordinating service

JournalKeeper provides [Journal Keeper Coordinating Service API(JK-CS API)](./journalkeeper-docs/src/markdown/JournalKeeperAPI.md) can be used to coordinate distributed systems, and it is embedded in your business system, No extra deployment and maintenance is required. JournalKeeper can be used:

* Store metadata, support KV and SQL
* Leader election
* Monitor node status
* Distributed locks
* GUID Generation

## Features

The core of JournalKeeper is a high-performance RAFT library optimized for large-scale distributed clusters. It strictly implements all the features of [The Raft consensus algorithm](https://raft.github.io/raft.pdf), including:

* Replicated state machines
* Log replication
* Leader election
* Cluster membership changes in Two-phase
* Automatically create snapshots
* Install snapshot
* Log compaction

In addition, JournalKeeper also includes these useful features:

* Multi-partition journal: The consistency log is divided into multiple logical partitions, and a separate state machine can be established for each partition to achieve parallel read and write on the premise of ensuring causal consistency
* Parallel log replication: improve master-slave replication performance
* Reading from follower: JournalKeeper supports clients to read data from followers under the premise of ensuring sequence consistency, improving cluster read performance;
* Preferred Leader: Supports the designation of a node in the cluster as the preferred leader. When the preferred leader node is alive, the cluster's election strategy always selects the preferred leader node as the leader. This in Multi-Raft can effectively avoid the problem of leadership skew.
* Observers: Observers only replicate logs from the cluster and generate status data. They can provide read services, but they do not participate in cluster elections. Joining observers can improve cluster read performance without affecting cluster write and election performance.

## Documentation

Visit [This post(In Chinese)](journalkeeper-docs/src/markdown/JournalKeeperRaft.md)。

## API

Visit [JournalKeeper API(In Chinese)](journalkeeper-docs/src/markdown/JournalKeeperAPI.md)。

## Contributing

We are dedicate to building high-quality messaging platform product. So any thoughts, pull requests, or issues are appreciated.

## License

Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).


