# SimpleDynamo


In this project, i have implemented a simplified version of DynamoDB, Amazon's highly available distributed key-value storage system.  

There are three main pieces you need to implement: 1) Partitioning, 2) Replication, and 3) Failure handling.
The main goal is to provide both availability and linearizability at the same time. In other words, your implementation should always perform read and write operations succ

There were three main pieces that needed to implemented:
Partition
Replication
Failure Handling

Content providers on all the nodes were used to store the data, but the abstraction it provides was a general key-value table. Local Content Providers at every node were created to support Membership, Request Routing, Quorum replication, Chain replication and Failure Handling.
