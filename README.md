# SimpleDynamo


In this project, i have implemented a simplified version of DynamoDB, Amazon's highly available distributed key-value storage system.  
The main goal is to provide both availability and linearizability at the same time. In other words, the implementation always performs read and write operations successfully even under failures. At the same time, a read operation always returns the most recent value.

There were three main pieces that are implemented:<br>
1. Partition<br>
2. Replication<br>
3. Failure Handling<br>

Content providers on all the nodes were used to store the data, but the abstraction it provides was a general key-value table. Local Content Providers at every node were created to support Membership, Request Routing, Quorum replication, Chain replication and Failure Handling.

The code for the content provider is available here: 
https://github.com/ShrutiBendale/SimpleDynamo/blob/master/app/src/main/java/edu/buffalo/cse/cse486586/simpledynamo/SimpleDynamoProvider.java
