# MapReduce with Leader Election

## Miriam Brunet, Charles Moreno, Toby Mui


1. Master pings each worker periodically – If no response is received within a certain time the worker is marked as failed – Map & reduce task given to this worker are reset back to the initial state and rescheduled for other workers

2. On failure:

   Worker failure – Detect failure via periodic heartbeats – Re-execute in-progress map/reduce tasks

   Master failure – Single point of failure; Resume execution from log. So make sure you have a log on the master and this log is replicated on other nodes.

3. This is a Map Reduce implementation so: master will create mappers and reducers and they should operate on different chuncks of file . For simplicity, your task is only counting word frequency


## Log Replication/Consistency using RAFT

If a Paxos/Raft-based server reboots it should resume service where it left off. This requires that Raft keep persistent state that survives a reboot. The paper's Figure 2 mentions which state should be persistent.

Write the functionality that Raft describes to keep logs consistent (as explained in the slides and article)

<img width="687" height="371" alt="Raft Log Duplication" src="https://github.com/user-attachments/assets/1695bea7-9daa-4966-be39-9680c88385fa" />

Before running run the following in each node terminal:
go build -buildmode=plugin wc.go

To run open up [1 + number of nodes] separate terminal instances

On the first:

go run server.go

On the rest:

go run client.go [node_id] [input_file1] [input_file2] ... up to [input_file8]

with a MAX_NODE count of 8, expecting 1-8 and node 1 being the default leader

Example given running in CSC569lab4 folder:

First terminal:

go run client.go 8 pg-being_ernest/pg-being_ernest_1.txt pg-being_ernest/pg-being_ernest_2.txt pg-being_ernest/pg-being_ernest_3.txt pg-being_ernest/pg-being_ernest_4.txt pg-being_ernest/pg-being_ernest_5.txt pg-being_ernest/pg-being_ernest_6.txt pg-being_ernest/pg-being_ernest_7.txt pg-being_ernest/pg-being_ernest_8.txt

Second terminal:

go run client.go 7 pg-being_ernest/pg-being_ernest_1.txt pg-being_ernest/pg-being_ernest_2.txt pg-being_ernest/pg-being_ernest_3.txt pg-being_ernest/pg-being_ernest_4.txt pg-being_ernest/pg-being_ernest_5.txt pg-being_ernest/pg-being_ernest_6.txt pg-being_ernest/pg-being_ernest_7.txt pg-being_ernest/pg-being_ernest_8.txt

...

Original leader terminal:

go run client.go 1 pg-being_ernest/pg-being_ernest_1.txt pg-being_ernest/pg-being_ernest_2.txt pg-being_ernest/pg-being_ernest_3.txt pg-being_ernest/pg-being_ernest_4.txt pg-being_ernest/pg-being_ernest_5.txt pg-being_ernest/pg-being_ernest_6.txt pg-being_ernest/pg-being_ernest_7.txt pg-being_ernest/pg-being_ernest_8.txt
