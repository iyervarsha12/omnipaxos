# omnipaxos
Done as a part of Distributed Systems (CS 651) in Boston University, it implements OmniPaxos, a state-of-the-art replicated state machine protocol.

A replicated service achieves fault tolerance by storing complete copies of its state (i.e., data) on multiple replica servers. Replication allows the service to continue operating even if some of its servers experience failures (crashes or a broken or flaky network). The challenge is that failures may cause the replicas to hold different copies of the data.
A set of OmniPaxos instances talk to each other with RPC to maintain replicated logs. My OmniPaxos interface supports an indefinite sequence of numbered commands, also called log entries.
The protocol is detailed in the paper https://dl.acm.org/doi/pdf/10.1145/3552326.3587441 

To run, please use the command "go test -race", which tests for various scenarios of servers, including simulating disconnecting, and special scenarios like partial chaining (which would make other protocols like RAFT fail).
