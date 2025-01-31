# Assignment 2: OmniPaxos

**Due (2A): Mon, Sept 30, 11:59 PM**

## Introduction

In this series of assignments you'll implement OmniPaxos, a replicated state machine protocol. A replicated service achieves fault tolerance by storing complete copies of its state (i.e., data) on multiple replica servers. Replication allows the service to continue operating even if some of its servers experience failures (crashes or a broken or flaky network). The challenge is that failures may cause the replicas to hold different copies of the data.

OmniPaxos organizes client requests into a sequence, called the log, and ensures that all the replica servers see the same log. Each replica executes client requests in log order, applying them to its local copy of the service's state. Since all the live replicas see the same log contents, they all execute the same requests in the same order, and thus continue to have identical service state. If a server fails but later recovers, OmniPaxos takes care of bringing its log up to date. OmniPaxos will continue to operate as long as at least a majority of the servers are alive and can talk to each other. If there is no such majority, OmniPaxos will make no progress, but will pick up where it left off as soon as a majority can communicate again.

In these assignments you'll implement an OmniPaxos service in Go. A set of OmniPaxos instances talk to each other with RPC to maintain replicated logs. Your OmniPaxos interface will support an indefinite sequence of numbered commands, also called log entries. The entries are numbered with index numbers. The log entry with a given index will eventually be decided (committed). At that point, your OmniPaxos should send the log entry to the application (eg. a key value store) for it to execute.

You should follow the design in the [OmniPaxos paper](https://dl.acm.org/doi/pdf/10.1145/3552326.3587441), with particular attention to Figure 3 and Figure 4. You'll implement most of what's in the paper.

This assignment is due in three parts. You must submit each part on the corresponding due date.

## Getting Started

We supply you with skeleton code `src/OmniPaxos/OmniPaxos.go`. We also supply a set of tests, which you should use to drive your implementation efforts, and which we'll use to grade your submitted assignment. The tests are in `src/OmniPaxos/test_test.go`.

To get up and running, execute the following commands. Don't forget the `git pull` to get the latest software.

```
$ cd OmniPaxos
$ go test -race
Test (2A): initial election ...
--- FAIL: TestInitialElection2A (5.04s)
        config.go:326: expected one leader, got none
Test (2A): election after network failure ...
--- FAIL: TestReElection2A (5.03s)
        config.go:326: expected one leader, got none
...
```

To run a specific set of tests, use `go test -race -run 2A` or `go test -race -run TestInitialElection2A`.

## The Code

Implement OmniPaxos by adding code to `OmniPaxos/OmniPaxos.go`. In that file you'll find skeleton code, plus examples of how to send and receive RPCs.

Your implementation must support the following interface, which the Tester will use. You'll find more details in comments in `OmniPaxos.go`.

```
// create a new OmniPaxos server instance:
rf := Make(peers, me, persister, applyCh)

// start agreement on a new log entry:
// See Figure 3.
op.Proposal(command interface{}) (index, ballot, isleader)

// ask a OmniPaxos for its current ballot, and whether it thinks it is leader
op.GetState() (ballot, isLeader)

// each time a new entry is committed to the log, each OmniPaxos peer
// should send an ApplyMsg to the service (or Tester).
type ApplyMsg
```

A service calls `Make(peers,me,…)` to create an OmniPaxos peer. The peers argument is an array of server identifiers which the peers can use to communicate with each other via RPCs. The `me` argument is the server identifier of the server that executes `Make(...)`. `Start(command)` asks OmniPaxos to start the processing to append the command to the replicated log. `Start()` should return immediately, without waiting for the log appends to complete. The service expects your implementation to send an `ApplyMsg` for each newly committed log entry to the `applyCh` channel, which is given as an argument to `Make()`.

`OmniPaxos.go` contains example code that sends an RPC (`sendHeartBeats()`) and that handles an incoming RPC (`HBRequest()`). Your OmniPaxos peers should exchange RPCs using the labrpc Go package (source in `src/labrpc`). The Tester can tell `labrpc` to delay RPCs, re-order them, and discard them to simulate various network failures. While you can temporarily modify `labrpc`, make sure your OmniPaxos works with the original `labrpc`, since that's what we'll use to test and grade your assignment. Your OmniPaxos instances must interact only with RPC; for example, they are not allowed to communicate using shared Go variables or files.

## Part 2A: Ballot Leader Election

### Task

Implement OmniPaxos ballot leader election and heartbeats. The goal for Part 2A is for a single leader to be elected, for the leader to remain the leader if there are no failures, and for a new leader to take over if the old leader fails or if packets to/from the old leader are lost. Run `go test -run 2A -race` to test your 2A code.

You can import it like this:
```go
import (
	future "cs651/a2-futures"
)
```

And use it like this:

```go
results := make([]*future.Future, 0)

for server := 0; server < len(op.peers); server++{
  // your RPC Specific code here
  results = append(results, YOUR_RPC_CALL(server, args, reply))
}

returnedResults := future.Wait(results, requiredResponseCount, timeout, postCompletionLogic)
```


It is highly recommended that you go through the hints section below, since it contains many tricks and insights that we came across as we created the assignment.

### Hints

- You can't easily run your OmniPaxos implementation directly; instead you should run it by way of the Tester, i.e. `go test -run 2A -race`.
- Follow the paper's Figure 4 and Figure 3's `2. <Leader> from BLE` section. 
- Add the Figure 3 and 4 state for leader election to the `OmniPaxos` struct in `OmniPaxos.go`. 
  - In order to make things convenient, try to use the same variable names and function names as mentioned in the Figure.
  - You'll also need to define a struct to hold information about each log entry.
- Fill in the `HBRequest` and `HBReply` structs. Modify `Make()` to create a background goroutine that will kick off leader election periodically by sending out `HeartBeat` RPCs when it hasn't heard from another peer for a while.
- Since each server needs to gossip with everyone else, try sending out `HBRequest`s to all servers (other than the sender itself) as individual Go routines, and then call `startTimer()`.
- An optimal value for the `delay` for `startTimer()` would be `100ms`.
- The Tester requires that the leader send heartbeat RPCs no more than ten times per second.
- You'll need to write code that takes actions periodically or after delays in time. The easiest way to do this is to create a goroutine with a loop that calls [time.Sleep()](https://golang.org/pkg/time/#Sleep); (see the `ticker()` goroutine that `Make()` creates for this purpose). Don't use Go's `time.Timer` or `time.Ticker`, which are difficult to use correctly.
- If your code has trouble passing the tests, read the paper's Figure 3 and 4 again; the full logic for ballot leader election is spread over multiple parts of the figure. But your primary focus is Figure 4 and Figure 3's `2. <Leader> from BLE` section.
- Don't forget to implement `GetState()`.
- The Tester calls your OmniPaxos's `op.Kill()` when it is permanently shutting down an instance. You can check whether `Kill()` has been called using `op.killed()`. You may want to do this in all loops, to avoid having dead OmniPaxos instances print confusing messages.
- Go RPC sends only struct fields whose names start with capital letters. Sub-structures must also have capitalized field names (e.g. fields of log records in an array). The `labgob` package will warn you about this; don't ignore the warnings.
- You might have RPC calls (such as `Leader`), which do not really need a reply. In such cases, you may create an empty struct (say `DummyReply`) to use as a placeholder.

### The Logger
- The repository provides you with a sophisticated logging mechanism via [ZeroLog](https://github.com/rs/zerolog). You can insert logs into your code using:
```go
log.Info().Msgf("Hello from OmniPaxos!")
```

You may also use the wrappers provided in [`util.go`](./util.go), which print the server IDs along with all logs so that you don't have to do so manually!

```go
op.Debug("Debug Log %v", args)
op.Info("Info Log %v", args)
op.Warn("Warn Log %v", args)
op.Fatal("Fatal Log %v", args)
```

Result:
```shell
12:14AM DBG [SERVER=2] Debug Log
12:14AM INF [SERVER=2] Info Log
12:14AM WRN [SERVER=2] Warn Log
12:14AM FTL [SERVER=2] Fatal Log
```

`zerolog` allows for logging at the following levels (from highest to lowest):
```
panic (zerolog.PanicLevel, 5)
fatal (zerolog.FatalLevel, 4)
error (zerolog.ErrorLevel, 3)
warn (zerolog.WarnLevel, 2)
info (zerolog.InfoLevel, 1)
debug (zerolog.DebugLevel, 0)
trace (zerolog.TraceLevel, -1)
```

When running tests, you can set the minimum allowed log level using the `loglevel` flag.

For example, the below command will only allow logs which are `Warn` or above.
```
go test -race -loglevel 2
```

Be sure you pass the 2A tests before submitting Part 2A, so that you see something like this:

It is okay if you see some Warning logs from the tester such as disconnecting or connecting nodes. They are purely for debugging help.

```
$ go test -race -run 2A -loglevel 5
Test (2A): [TestInitialElection2A] initial election ...
  ... Passed --   4.6  3  273   32250    0
Test (2A): [TestReElection2A] election after network failure ...
  ... Passed --   7.7  3  515   46686    0
Test (2A): [TestManyElections2A] multiple elections ...
  ... Passed --  12.2  7 5563  472662    0
Test (2A): [TestFigure5AQuorumLoss2A] Partial Connectivity - Quorum-Loss Scenario (Fig 5A) ...
  ... Passed --   4.6  5  930   85040    0
Test (2A): [TestFigure5BConstrainedElection2A] Partial Connectivity - Constrained Election Scenario (Fig 5B) ...
  ... Passed --   4.6  5  916   79479    0
Test (2A): [TestFigure5cChained2A] Partial Connectivity - Chained Scenario (Fig 5c) ...
  ... Passed --   4.6  3  281   29023    0
PASS
ok  	cs651/a5-omnipaxos	39.583s
```

Each "Passed" line contains five numbers; these are the time that the test took in seconds, the number of OmniPaxos peers (usually 3 or 5), the number of RPCs sent during the test, the total number of bytes in the RPC messages, and the number of log entries that OmniPaxos reports were committed. Your numbers will differ from those shown here. You can ignore the numbers if you like, but they may help you sanity-check the number of RPCs that your implementation sends. For all of the parts the grading script will fail your solution if it takes more than 600 seconds for all of the tests (`go test`), or if any individual test takes more than 120 seconds.

## Part 2B: Log Replication

### Task

Implement the log entries, so that the `go test -run 2B -race` tests pass.
For this, you need to implement everything remaining in Figure 3.

### Recovery (Important)
If a server has been disconnected from the entire network, it may miss out on some log entries that were committed in the meanwhile. To recover from this, Figure 3 mentions three steps. 
```
10. Upon Recovery
11. <PrepareReq> from Follower
12. <Reconnected> to server s
```

During the failure, another leader might have been elected and already completed the Prepare phase. Since the recovering server is unaware of who is the current leader, it sends `<PrepareReq>` to all its peers `10` . A receiving server that is the leader responds with `<Prepare>` `11` . From this point, the protocol proceeds as usual. Link session drops between two servers are handled similarly. Since they might be unaware that the other has become the leader during the disconnected period, a `<PrepareReq>` is sent `12`

This assignment only takes into account the situations where there are link session drops (aka. a complete network disconnection of a given node.)

The paper does not explicitly define when and how this behavior is triggered. You may choose to implement it however you want, but one possible approach would be the following:
1. Create a new variable in the OmniPaxos struct called `LinkDrop` (initialized to `false`).
2. During the heart beat exchange, if a node does not receive any heart beat for atleast `3` consecutive rounds, set `LinkDrop` to `true`.
3. During the heart beat exchange, if a node receives heart beats and `LinkDrop == true`:
   1. This implies that the given node has just recovered from a network disconnection.
   2. Set `LinkDrop` to `false`.
   3. Change the node's state to `FOLLOWER, RECOVER`.
   4. send `⟨PrepareReq⟩` to all peers
   5. Implement ` 11. <PrepareReq> from follower` (as described in Figure 3).

Using this design, you don't need a separate `12. <Reconnected>` implementation.


### This part contains two types of tests:

#### Don't Require A Recovery Mechanism
1. TestBasicAgree2B 
2. TestTooManyCommandsAgree2B 
3. TestRPCBytes2B 
4. TestConcurrentStarts2B 
5. TestCount2B 
#### Require a Recovery Mechanism
6. TestFailAgree2B
7. TestFailNoAgree2B
8. TestBackup2B
9. TestRejoin2B
10. TestFig5ALogReplication2B
11. TestFig5BLogReplication2B
12. TestFig5cLogReplication2B

Make sure you implement the Recovery Mechanism in order to pass all the tests.


### Hints
- All log entires are 0-indexed.
- This part involves multiple RPCs which once again, don't need replies. You may use the same `DummyReply` struct for all such RPC replies.
- Your first goal should be to pass `TestBasicAgree2B()`. Start by implementing `Proposal()`, then write the code to send and receive new log entries via `<Proposal> -> <Accept> -> <Accepted> -> <Decide>` RPCs, following Figure 3. 
  - **But you will still need** a basic implementation of `<Prepare> -> <Promise> -> <AcceptSync>` to pass the first few tests, and a complete implementation to pass everything.
- Your code may have loops that repeatedly check for certain events. Don't have these loops execute continuously without pausing, since that will slow your implementation enough that it fails tests. Use Go's [condition variables](https://golang.org/pkg/sync/#Cond), or insert a `time.Sleep(10 * time.Millisecond)` in each loop iteration.
- Do yourself a favor and write (or re-write) code that's clean and clear. For ideas, re-visit the [Guidance page](https://cs-people.bu.edu/liagos/651-2022/labs/guidance.html) with tips on how to develop and debug your code.
- If you fail a test, look over the code for the test in `config.go` and `test_test.go` to get a better understanding what the test is testing. `config.go` also illustrates how the Tester uses the OmniPaxos API.
- Ensure than `prefix(idx)` and `suffix(idx)` are in line with your log indexing and do not throw out of bounds errors.
- Typically running the test several times will expose bugs related to non determinism.
The tests may fail your code if it runs too slowly. You can check how much real time and CPU time your solution uses with the `time` command. Here's typical output:
```
$ time go test -race -run 2B --loglevel 5
Test (2B): [TestBasicAgree2B] basic agreement ...
  ... Passed --   0.6  3   57    6086    2
Test (2B): [TestTooManyCommandsAgree2B] high traffic agreement (too many commands submitted fast) ...
  ... Passed --   8.2  3 2883  291988  299
Test (2B): [TestRPCBytes2B] RPC byte count ...
  ... Passed --   0.8  3  133  113846   10
Test (2B): [TestFailAgree2B] agreement despite follower disconnection ...
  ... Passed --   4.8  3  338   35313    6
Test (2B): [TestFailNoAgree2B] no agreement if too many followers disconnect ...
  ... Passed --   5.8  5 1319  140106    3
Test (2B): [TestConcurrentStarts2B] concurrent Proposal()s ...
  ... Passed --   1.6  3  147   16232    5
Test (2B): [TestBackup2B] leader backs up quickly over incorrect follower logs ...
  ... Passed --  55.0  5 12352  942357  122
Test (2B): [TestCount2B] RPC counts aren't too high ...
  ... Passed --   3.6  3  309   34458   11
Test (2B): [TestRejoin2B] rejoin of partitioned leader ...
  ... Passed --   6.0  3  431   43644    3
Test (2B): [TestFig5ALogReplication2B] Partial Connectivity - Quorum-Loss Scenario (Fig 5A) ...
  ... Passed --   5.7  5 1330  128700    5
Test (2B): [TestFig5BLogReplication2B] Partial Connectivity - Constrained Election Scenario (Fig 5B) ...
  ... Passed --   5.7  5 1287  120280    5
Test (2B): [TestFig5cLogReplication2B] Partial Connectivity - Chained Scenario (Fig 5c) ...
  ... Passed --   5.7  3  394   41744    5
PASS
ok  	cs651/a5-omnipaxos	94.833s
go test -run 2B  10.32s user 5.81s system 8% cpu 3:11.40 total
```

The "ok OmniPaxos 94.833s" means that Go measured the time taken for the 2B tests to be 94.833 seconds of real (wall-clock) time. The "10.32s user" means that the code consumed 10.32s seconds of CPU time, or time spent actually executing instructions (rather than waiting or sleeping). If your solution uses an unreasonable amount of time, look for time spent sleeping or waiting for RPC timeouts, loops that run without sleeping or waiting for conditions or channel messages, or large numbers of RPCs sent.

#### A few other hints:

- Run git pull to get the latest lab software.
- Failures may be caused by problems in your code for 2A or log replication. Your code should pass all the 2A and 2B tests. It might be possible that your changes for 2B break some tests for 2A. So keep testing both parts as you work on this assignment.

It is a good idea to run the tests multiple times before submitting and check that each run prints `PASS`.

```
$ go test -race -count 10
```


## Part 2C: Log Persistence

If a OmniPaxos-based server reboots it should resume service where it left off. This requires that OmniPaxos keep persistent state that survives a reboot. The paper's Figure 3 and Figure 4 mention which state should be persistent.

A real implementation would write OmniPaxos's persistent state to disk each time it changed, and would read the state from disk when restarting after a reboot. Your implementation won't use the disk; instead, it will save and restore persistent state from a `Persister` object (see `persister.go`). Whoever calls `OmniPaxos.Make()` supplies a `Persister` that initially holds OmniPaxos's most recently persisted state (if any). OmniPaxos should initialize its state from that `Persister`, and should use it to save its persistent state each time the state changes. Use the `Persister`'s `ReadOmnipaxosState()` and `SaveOmnipaxosState()` methods.

### Task

Complete the functions `persist()` and `readPersist()` in `omnipaxos.go` by adding code to save and restore persistent state. You will need to encode (or "serialize") the state as an array of bytes in order to pass it to the `Persister`. Use the `labgob` encoder; see the comments in `persist()` and `readPersist()`. `labgob` is like Go's `gob` encoder but prints error messages if you try to encode structures with lower-case field names.

Insert calls to `persist()` at the points where your implementation changes persistent state. Once you've done this, you should pass the remaining tests.

### Hints

* Many of the 2C tests involve servers failing and the network losing RPC requests or replies. These events are non-deterministic, and you may get lucky and pass the tests, even though your code has bugs. Typically running the test several times will expose those bugs.
* The 2C tests are more demanding than those for 2A or 2B, and failures may be caused by problems in your code for 2A or 2B.


Your code should pass all the 2C tests (as shown below), as well as the 2A and 2B tests.

```
Test (2C): basic persistence ...
  ... Passed --   5.5  3  620   53068    4
Test (2C): more persistence ...
  ... Passed --  15.2  5 3626  280225    7
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   4.6  3  291   25741    3
Test (2C): [TestFig5APersistence2C] Partial Connectivity - Quorum-Loss Scenario (Fig 5A) ...
  ... Passed --   6.6  5 1274  116814    4
Test (2C): [TestFig5BPersistence2C] Partial Connectivity - Constrained Election Scenario (Fig 5B) ...
  ... Passed --   6.7  5 1301  115856    5
Test (2C): [TestFig5CPersistence2C] Partial Connectivity - Chained Scenario (Fig 5c) ...
  ... Passed --   6.7  3  372   37088    5
PASS
ok  	cs651/a2-omnipaxos	46.822s
```

It is a good idea to run the tests multiple times before submitting and check that each run prints `PASS`.

```
$ for i in {0..10}; do go test; done
```