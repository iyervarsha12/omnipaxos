package omnipaxos

//
// Omni-Paxos tests.
//
// We will use the original test_test.go to test your code for grading.
// So, while you can modify this code to help you debug, please
// test with the original before submitting.
//
// You can run the tester with the following flags in addition to the
// standard ones provided by `go test`:
// 	-pretty (this will enable pretty printing)
// 	-loglevel [n] (this will set the log level accordingly)

import (
	"flag"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
)

var prettyPrint bool
var logLevel int

func init() {
	flag.BoolVar(&prettyPrint, "pretty", true, "Enable pretty printing for zerolog")
	flag.IntVar(&logLevel, "loglevel", 1, "Set the log level for zerolog (-1=trace, 0=debug, 1=info, 2=warn, 3=error, 4=fatal, 5=panic)")
}

func TestMain(m *testing.M) {
	// Parse the flags
	flag.Parse()

	// Set up the logger with the pretty print flag
	SetupLogger(logLevel, prettyPrint)

	// Run the tests
	os.Exit(m.Run())
}

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const PaxosElectionTimeout = 1000 * time.Millisecond

// TestInitialElection4A tests the initial election process in an Omni-Paxos cluster with 3 servers.
// The test ensures that:
//  1. A leader is elected in the initial startup.
//  2. All peers agree on the same ballot shortly after the election.
//  3. The leader and ballot remain consistent if there are no network failures over a specific period.
//  4. There is still a leader after a certain period without any network failures.
func TestInitialElection2A(t *testing.T) {
	servers := 3
	cfg := makeConfig(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2A): [TestInitialElection2A] initial election")

	// Is a leader elected in the initial setup?
	cfg.checkOneLeader()

	// Sleep a bit to avoid racing with followers learning of the
	// election, then check that all peers agree on the ballot.
	ballot1 := cfg.checkballots()

	// TODO: PG Delete the following check, keeping for future ref for now
	// if ballot1 < 1 {
	// 	t.Fatalf("ballot is %v, but should be at least 1", ballot1)
	// }

	// Does the leader+ballot remain consistent if there is are no network failures?
	time.Sleep(2 * PaxosElectionTimeout)
	ballot2 := cfg.checkballots()
	if ballot1 != ballot2 {
		log.Warn().Msgf("ballot changed from %d to %d even though there were no failures", ballot1, ballot2)
	}

	// Is there still a leader after a certain period without any network failures?
	cfg.checkOneLeader()

	// end of the test. Cleanup.
	cfg.end()
}

// TestReElection4A tests the re-election process in an Omni-Paxos cluster with 3 servers.
// The test ensures that:
//  1. A new leader is elected after the current leader disconnects.
//  2. The old leader rejoining the network does not disturb the new leader.
//  3. No leader is elected if there is no quorum.
//  4. A new leader is elected when a quorum arises.
//  5. The re-join of the last node does not prevent a leader from existing.
func TestReElection2A(t *testing.T) {
	servers := 3
	cfg := makeConfig(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2A): [TestReElection2A] election after network failure")

	// Is a leader elected in the initial setup?
	leader1 := cfg.checkOneLeader()

	// If the leader disconnects, a new one should be elected.
	cfg.disconnect(leader1)
	log.Warn().Msgf("Disconnecting Leader %v", leader1)
	cfg.checkOneLeader()

	// If the old leader rejoins, that shouldn't disturb the new leader.
	log.Warn().Msgf("If the old leader rejoins, that shouldn't disturb the new leader. %v", leader1)
	cfg.connect(leader1)
	leader2 := cfg.checkOneLeader()
	log.Warn().Msgf("Disconnecting Leader2 %v", leader2)
	log.Warn().Msgf("Disconnecting Leader2 + 1 %v", leader2+1)
	// If there's no quorum, no leader should be elected.
	cfg.disconnect(leader2)
	cfg.disconnect((leader2 + 1) % servers)
	time.Sleep(2 * PaxosElectionTimeout)
	cfg.checkNoLeader()

	log.Warn().Msgf("If a quorum arises, it should elect a leader - connecting %v", (leader2+1)%servers)
	// If a quorum arises, it should elect a leader.
	cfg.connect((leader2 + 1) % servers)
	cfg.checkOneLeader()

	log.Warn().Msgf("Re-join of last node shouldn't prevent leader from existing. - connecting %v", leader2)
	// Re-join of last node shouldn't prevent leader from existing.
	cfg.connect(leader2)
	// wait for a steady state (prepare to be sent to everyone)
	// time.Sleep(2 * PaxosElectionTimeout)
	cfg.checkOneLeader()

	// end of the test. Cleanup.
	cfg.end()
}

func TestManyElections2A(t *testing.T) {
	servers := 7
	cfg := makeConfig(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2A): [TestManyElections2A] multiple elections")

	log.Warn().Msgf("Initial Leader Check")
	cfg.checkOneLeader()

	iters := 10
	for ii := 1; ii < iters; ii++ {
		// Disconnect three nodes.
		i1 := rand.Int() % servers
		i2 := rand.Int() % servers
		i3 := rand.Int() % servers
		log.Warn().Msgf(" [%v/%v] Disconnecting Nodes: %v %v %v", ii, iters, i1, i2, i3)
		cfg.disconnect(i1)
		cfg.disconnect(i2)
		cfg.disconnect(i3)
		// either the current leader should still be alive,
		// or the remaining four should elect a new one.
		log.Warn().Msgf(" [%v/%v] Checking for leader: %v %v %v", ii, iters, i1, i2, i3)
		cfg.checkOneLeader()

		log.Warn().Msgf(" [%v/%v] Reconnecting Nodes: %v %v %v", ii, iters, i1, i2, i3)
		cfg.connect(i1)
		cfg.connect(i2)
		cfg.connect(i3)
	}

	log.Warn().Msgf("Final Leader Check")
	cfg.checkOneLeader()

	cfg.end()
}

func TestFigure5AQuorumLoss2A(t *testing.T) {
	servers := 5
	cfg := makeConfig(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2A): [TestFigure5AQuorumLoss2A] Partial Connectivity - Quorum-Loss Scenario (Fig 2A)")
	// Get an initial leader
	leader1 := cfg.checkOneLeader()
	log.Warn().Msgf("Initial leader is %v", leader1)

	// Now, we will mimic the scenario presented in Fig 2B
	// of the OmniPaxos paper

	// Let S != leader1, be a server in our clutser
	// S will be connected to everyone
	// every other server would ONLY be connected to S

	for i := 0; i < servers; i++ {
		// disconnect everyone
		cfg.disconnect(i)
	}

	// define S
	S := rand.Int() % servers
	for ; S == leader1; S = rand.Int() % servers {
	}
	log.Warn().Msgf("S is %v", S)

	// connect S to everyone
	var serversToConnectTo []int
	for i := 0; i < servers; i++ {
		// if i != S {
		serversToConnectTo = append(serversToConnectTo, i)
		// }
	}
	cfg.partiallyConnect(S, serversToConnectTo)

	log.Warn().Msgf("Partial Connectivity Established. Server with QC is %v", S)

	// wait for some time so that the system can reach a steady state
	time.Sleep(2 * PaxosElectionTimeout)

	// should have a new leader since leader1
	// has lost quorum connectivity
	leader2 := cfg.checkOneLeader()
	log.Warn().Msgf("New leader is %v", leader2)
	if leader2 != S {
		t.Fatalf("Server%v is the only server with QC, yet got %v as a leader", S, leader2)
	}

	cfg.end()
}

func TestFigure5BConstrainedElection2A(t *testing.T) {
	servers := 5
	cfg := makeConfig(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2A): [TestFigure5BConstrainedElection2A] Partial Connectivity - Constrained Election Scenario (Fig 2B)")
	// Get an initial leader
	leader1 := cfg.checkOneLeader()
	log.Warn().Msgf("Initial leader is %v", leader1)

	// Now, we will mimic the scenario presented in Fig 2A
	// of the OmniPaxos paper

	// Let S != leader1, be a server in our clutser
	// S will be connected to everyone but leader1
	// every other server (except leader1) would ONLY be connected to S

	for i := 0; i < servers; i++ {
		// disconnect everyone
		cfg.disconnect(i)
	}

	// define S
	S := rand.Int() % servers
	for ; S == leader1; S = rand.Int() % servers {
	}
	log.Warn().Msgf("S is %v", S)

	// connect S to everyone (but leader1)
	var serversToConnectTo []int
	for i := 0; i < servers; i++ {
		if i != leader1 {
			serversToConnectTo = append(serversToConnectTo, i)
		}
	}
	cfg.partiallyConnect(S, serversToConnectTo)

	log.Warn().Msgf("Partial Connectivity Established. Server with QC is %v", S)

	// wait for some time so that the system can reach a steady state
	time.Sleep(2 * PaxosElectionTimeout)

	// should have a new leader since leader1
	// has lost quorum connectivity
	leader2 := cfg.checkOneLeader()
	log.Warn().Msgf("New leader is %v", leader2)
	if leader2 != S {
		t.Fatalf("Server%v is the only server with QC, yet got %v as a leader", S, leader2)
	}

	cfg.end()
}

func TestFigure5cChained2A(t *testing.T) {
	servers := 3
	cfg := makeConfig(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2A): [TestFigure5cChained2A] Partial Connectivity - Chained Scenario (Fig 5c)")
	// Get an initial leader
	leader1 := cfg.checkOneLeader()
	log.Warn().Msgf("Initial leader is %v", leader1)

	// Now, we will mimic the scenario presented in Fig 5c
	// of the OmniPaxos paper

	// Let S != leader1, be a server in our clutser
	// S and leader1 will be disconnected from each other

	// define S
	S := rand.Int() % servers
	for ; S == leader1; S = rand.Int() % servers {
	}
	log.Warn().Msgf("S is %v", S)

	// connect S to everyone (but leader1)
	cfg.partiallyDisconnect(S, []int{leader1})

	log.Warn().Msgf("Partial Connectivity Established. Server with QC is %v", S)

	// wait for some time so that the system can reach a steady state
	time.Sleep(2 * PaxosElectionTimeout)

	// should have a new leader since leader1
	// has can't commnunicate with S
	// so S will timeout waiting for leader1's Heartbeat
	// and then increase it's ballot
	leader2 := cfg.checkOneLeader()
	log.Warn().Msgf("New leader is %v", leader2)
	if leader2 != S {
		t.Fatalf("Server%v is the only server with QC, yet got %v as a leader", S, leader2)
	}

	cfg.end()

}

// elect a leader
// sleep for some time to reach a stable state
// submit 3 commands
// make sure each one is committed!
func TestBasicAgree2B(t *testing.T) {
	servers := 3
	cfg := makeConfig(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): [TestBasicAgree2B] basic agreement")

	log.Warn().Msgf("some time to reach the steady state\n")

	iters := 3
	for index := 0; index < iters; index++ {
		// time.Sleep(1000 * time.Millisecond)
		nd, _ := cfg.nCommitted(index)
		if nd > 0 {
			t.Fatalf("some have committed before Proposal()")
		}

		xindex := cfg.one(index*100, servers, false)
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
	}

	cfg.end()
}

// same as the prev test, but just too many commands
// just to ensure that the service is stable
// enough to handle an intense load
func TestTooManyCommandsAgree2B(t *testing.T) {
	servers := 3
	cfg := makeConfig(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): [TestTooManyCommandsAgree2B] high traffic agreement (too many commands submitted fast)")

	// some time to reach the steady state
	time.Sleep(500 * time.Millisecond)
	log.Warn().Msgf("some time to reach the steady state\n")

	iters := 300
	for index := 0; index < iters; index++ {
		// time.Sleep(1000 * time.Millisecond)
		nd, _ := cfg.nCommitted(index)
		if nd > 0 {
			t.Fatalf("some have committed before Proposal()")
		}

		xindex := cfg.one(index*100, servers, false)
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
	}

	cfg.end()
}

// Check, based on counting bytes of RPCs, that
// each command is sent to each peer just once.
func TestRPCBytes2B(t *testing.T) {
	servers := 3
	cfg := makeConfig(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): [TestRPCBytes2B] RPC byte count")

	cfg.one(99, servers, false)
	bytes0 := cfg.bytesTotal()

	iters := 10
	var sent int64 = 0
	for index := 1; index < iters+1; index++ {
		cmd := randstring(5000)
		xindex := cfg.one(cmd, servers, false)
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
		sent += int64(len(cmd))
	}

	bytes1 := cfg.bytesTotal()
	got := bytes1 - bytes0
	expected := int64(servers) * sent
	if got > expected+50000 {
		t.Fatalf("too many RPC bytes; got %v, expected %v", got, expected)
	}

	cfg.end()
}

// submits a command
// then disconnects one node
// then submnits a bunch of commands
// then reconnets the node
// (it should manage to get all the logs it missed)
// submits two more commands
// fin.
func TestFailAgree2B(t *testing.T) {
	servers := 3
	cfg := makeConfig(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): [TestFailAgree2B] agreement despite follower disconnection")

	cfg.one(101, servers, false)

	// Disconnect one follower from the network.
	leader := cfg.checkOneLeader()
	log.Warn().Msgf("Tester - Leader is %v", leader)
	cfg.disconnect((leader + 1) % servers)

	// The leader and remaining follower should be
	// able to agree despite the disconnected follower.
	cfg.one(102, servers-1, false)
	cfg.one(103, servers-1, false)
	time.Sleep(PaxosElectionTimeout)
	cfg.one(104, servers-1, false)
	cfg.one(105, servers-1, false)

	// Re-connect.
	cfg.connect((leader + 1) % servers)

	time.Sleep(PaxosElectionTimeout)

	// The full set of servers should preserve previous
	// agreements, and be able to agree on new commands.
	cfg.one(106, servers, true)
	time.Sleep(PaxosElectionTimeout)
	cfg.one(107, servers, true)

	cfg.end()
}

// submit a command
// 3 of 5 followers disconnect
// submit another command
func TestFailNoAgree2B(t *testing.T) {
	servers := 5
	cfg := makeConfig(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): [TestFailNoAgree2B] no agreement if too many followers disconnect")

	cfg.one(10, servers, false)

	// 3 of 5 followers disconnect.
	leader := cfg.checkOneLeader()
	log.Warn().Msgf("Tester - Going to disconnect %v, %v, %v",
		(leader+1)%servers, (leader+2)%servers, (leader+3)%servers)

	cfg.disconnect((leader + 1) % servers)
	cfg.disconnect((leader + 2) % servers)
	cfg.disconnect((leader + 3) % servers)

	index, _, ok := cfg.paxos[leader].Proposal(20)
	if ok != true {
		t.Fatalf("leader rejected Proposal()")
	}
	if index != 1 {
		t.Fatalf("expected index 1, got %v", index)
	}

	time.Sleep(PaxosElectionTimeout)

	n, _ := cfg.nCommitted(index)
	if n > 0 {
		t.Fatalf("%v committed but no majority", n)
	}

	// Repair.
	log.Warn().Msgf("Tester - Going to re-connect %v, %v, %v",
		(leader+1)%servers, (leader+2)%servers, (leader+3)%servers)
	cfg.connect((leader + 1) % servers)
	cfg.connect((leader + 2) % servers)
	cfg.connect((leader + 3) % servers)

	// give some time to reach a steady state
	time.Sleep(PaxosElectionTimeout)
	// The disconnected majority may have chosen a leader from
	// among their own ranks, forgetting index 2.
	leader2 := cfg.checkOneLeader()

	log.Warn().Msgf("Leader2: %v", leader2)

	index2, _, ok2 := cfg.paxos[leader2].Proposal(30)
	time.Sleep(PaxosElectionTimeout)
	if ok2 == false {
		t.Fatalf("leader2 rejected Proposal()")
	}
	if index2 < 2 || index2 > 4 {
		t.Fatalf("unexpected index %v", index2)
	}

	cfg.one(1000, servers, true)

	cfg.end()
}

func TestConcurrentProposals2B(t *testing.T) {
	servers := 3
	cfg := makeConfig(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): [TestConcurrentStarts2B] concurrent Proposal()s")

	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// Give the solution some time to settle.
			time.Sleep(3 * time.Second)
		}

		leader := cfg.checkOneLeader()
		_, ballot, ok := cfg.paxos[leader].Proposal(1)
		if !ok {
			// Leader moved on really quickly.
			continue
		}

		iters := 5
		var wg sync.WaitGroup
		is := make(chan int, iters)
		for ii := 0; ii < iters; ii++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				i, ballot1, ok := cfg.paxos[leader].Proposal(100 + i)
				if ballot1 != ballot {
					return
				}
				if ok != true {
					return
				}
				is <- i
			}(ii)
		}

		wg.Wait()
		close(is)

		for j := 0; j < servers; j++ {
			if t, _ := cfg.paxos[j].GetState(); t != ballot {
				// ballot changed -- can't expect low RPC counts.
				continue loop
			}
		}

		failed := false
		var cmds []int
		for index := range is {
			sameballot, e := cfg.wait(index, servers, ballot)
			if !sameballot {
				// Peers have moved on to later ballots, so we
				// can't expect all Proposal()s to have succeeded.
				failed = true
				break
			}
			if e.valid {
				if ix, ok := e.command.(int); ok {
					cmds = append(cmds, ix)
				} else {
					t.Fatalf("value %v is not an int", e.command)
				}
			}
		}

		if failed {
			// Avoid leaking goroutines.
			go func() {
				for range is {
				}
			}()
			continue
		}

		for ii := 0; ii < iters; ii++ {
			x := 100 + ii
			ok := false
			for j := 0; j < len(cmds); j++ {
				if cmds[j] == x {
					ok = true
				}
			}
			if ok == false {
				t.Fatalf("cmd %v missing in %v", x, cmds)
			}
		}

		success = true
		break
	}

	if !success {
		t.Fatalf("ballot changed too often")
	}

	cfg.end()
}

func TestBackup2B(t *testing.T) {
	servers := 5
	cfg := makeConfig(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): [TestBackup2B] leader backs up quickly over incorrect follower logs")

	cfg.one(rand.Int(), servers, true)

	// Put leader and one follower in a partition.
	leader1 := cfg.checkOneLeader()
	cfg.disconnect((leader1 + 2) % servers)
	cfg.disconnect((leader1 + 3) % servers)
	cfg.disconnect((leader1 + 4) % servers)

	// Submit lots of commands that won't commit.
	for i := 0; i < 50; i++ {
		cfg.paxos[leader1].Proposal(rand.Int())
	}

	time.Sleep(PaxosElectionTimeout / 2)

	cfg.disconnect((leader1 + 0) % servers)
	cfg.disconnect((leader1 + 1) % servers)

	// Allow the other partition to recover.
	cfg.connect((leader1 + 2) % servers)
	cfg.connect((leader1 + 3) % servers)
	cfg.connect((leader1 + 4) % servers)

	// Lots of successful commands to new group.
	for i := 0; i < 50; i++ {
		cfg.one(rand.Int(), 3, true)
	}

	// Now another partitioned leader and one follower
	leader2 := cfg.checkOneLeader()
	other := (leader1 + 2) % servers
	if leader2 == other {
		other = (leader2 + 1) % servers
	}
	cfg.disconnect(other)

	// Lots more commands that won't commit.
	for i := 0; i < 50; i++ {
		cfg.paxos[leader2].Proposal(rand.Int())
	}

	time.Sleep(PaxosElectionTimeout / 2)

	// Bring original leader back to life.
	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
	}
	cfg.connect((leader1 + 0) % servers)
	cfg.connect((leader1 + 1) % servers)
	cfg.connect(other)

	// Lots of successful commands to new group.
	for i := 0; i < 50; i++ {
		cfg.one(rand.Int(), 3, true)
	}

	// Now everyone.
	for i := 0; i < servers; i++ {
		cfg.connect(i)
	}
	cfg.one(rand.Int(), servers, true)

	cfg.end()
}

func TestCount2B(t *testing.T) {
	servers := 3
	cfg := makeConfig(t, servers, false, false)
	defer cfg.cleanup()
	RPCsForOneGossipSession := cfg.NumberOfGossipRPCs(servers)
	// Since the paper mentions that there is guaranteed recovery in
	// at most four election timeouts under extreme partial network partitions

	// TODO: Fine tune this later, I just winged it for now
	// seems to be scaling fine with enough room for error,
	// tried with 3, 5 and 10 servers
	gossipMultiplier := 10 * 4
	minimumRPCs := 1
	maxElectionRPCs := RPCsForOneGossipSession * gossipMultiplier
	cfg.begin("Test (2B): [TestCount2B] RPC counts aren't too high")

	rpcs := func() (n int) {
		for j := 0; j < servers; j++ {
			n += cfg.rpcCount(j)
		}
		return
	}

	leader := cfg.checkOneLeader()

	rpcCountAfterElection := rpcs()

	if rpcCountAfterElection >= maxElectionRPCs {
		t.Fatalf("too many RPCs (%v) to elect initial leader - expected <= %v\n",
			rpcCountAfterElection, maxElectionRPCs)
	}

	if rpcCountAfterElection < minimumRPCs {
		t.Fatalf("too few RPCs (%v) to elect initial leader  - expected <= %v\n", rpcCountAfterElection, minimumRPCs)
	}

	log.Info().Msgf("RPC Count for Elections: %v (<= %v)", rpcCountAfterElection, maxElectionRPCs)

	var rpcCountAfterLogReplication int
	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// Give the solution some time to settle.
			time.Sleep(3 * time.Second)
		}

		leader = cfg.checkOneLeader()
		rpcCountAfterElection = rpcs()

		iters := 10
		starti, ballot, ok := cfg.paxos[leader].Proposal(1)
		if !ok {
			// Leader moved on really quickly.
			continue
		}
		var cmds []int
		for i := 1; i < iters+2; i++ {
			x := int(rand.Int31())
			cmds = append(cmds, x)
			index1, ballot1, ok := cfg.paxos[leader].Proposal(x)
			if ballot1 != ballot {
				// ballot changed while starting.
				continue loop
			}
			if !ok {
				// No longer the leader, so ballot has changed.
				continue loop
			}
			if starti+i != index1 {
				t.Fatalf("Proposal() failed")
			}
		}

		for i := 1; i < iters+1; i++ {
			sameballot, e := cfg.wait(starti+i, servers, ballot)
			if !sameballot {
				// ballot changed -- try again.
				continue loop
			}
			if e != (logEntry{true, cmds[i-1]}) {
				t.Fatalf("wrong value %+v committed for index %v; expected %+v\n",
					e, starti+i, logEntry{true, cmds[i-1]})
			}
		}

		failed := false
		rpcCountAfterLogReplication = 0
		for j := 0; j < servers; j++ {
			if t, _ := cfg.paxos[j].GetState(); t != ballot {
				// ballot changed -- can't expect low RPC counts.
				// Need to keep going to update total2.
				failed = true
			}
			rpcCountAfterLogReplication += cfg.rpcCount(j)
		}

		if failed {
			continue loop
		}

		rpcsForLogReplication := rpcCountAfterLogReplication - rpcCountAfterElection

		// if rpcsForLogReplication > (iters+1+3)*3 {
		// 	t.Fatalf("too many RPCs (%v) for %v entries\n", rpcsForLogReplication, iters)
		// }

		// TODO: Fix this magic number as well?
		// Eye balled it again and seems fine for now
		maxAllowedRPCsForReplication := (iters + 1 + 3) * 10

		if rpcsForLogReplication >= maxAllowedRPCsForReplication {
			t.Fatalf("too many RPCs (%v) for %v entries - expected <= %v\n",
				rpcsForLogReplication, iters, maxAllowedRPCsForReplication)
		}
		log.Info().Msgf("RPC Count for Log Replication: %v (<= %v)", rpcsForLogReplication, maxAllowedRPCsForReplication)
		success = true
		break
	}

	if !success {
		t.Fatalf("ballot changed too often")
	}

	time.Sleep(PaxosElectionTimeout)

	total3 := 0
	for j := 0; j < servers; j++ {
		total3 += cfg.rpcCount(j)
	}

	rpcCountsForIdleness := total3 - rpcCountAfterLogReplication
	approxHBSessionsInOneSecond := 10
	maxRPCsForIdleness := servers * RPCsForOneGossipSession * approxHBSessionsInOneSecond

	if rpcCountsForIdleness > maxRPCsForIdleness {
		t.Fatalf("too many RPCs (%v) for 1 second of idleness - expected <= %v\n", rpcCountsForIdleness, maxRPCsForIdleness)
	}
	log.Info().Msgf("RPC Count for 1 second of idleness: %v (<= %v)", rpcCountsForIdleness, maxRPCsForIdleness)
	cfg.end()
}

// submit a command (101)
// then disconnect the leader (L1)
// submit 3 commands (102, 103, 104) to the same leader (who won't be able to commit those)
// submit a command to the new leader (L2) of the cluster (103)
// disconnect new leader (L2)
// connect old leader (L1) again
// submit a command to it (L1) (104)
// reconnect L2
// everyone agrees to something
// submit a final command (105)
// Distributed Consensus Everyone!
func TestRejoin2B(t *testing.T) {
	servers := 3
	cfg := makeConfig(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): [TestRejoin2B] rejoin of partitioned leader")

	cfg.one(101, servers, true)

	// Leader network failure.
	leader1 := cfg.checkOneLeader()
	cfg.disconnect(leader1)

	// Make old leader try to agree on some entries.
	cfg.paxos[leader1].Proposal(102)
	cfg.paxos[leader1].Proposal(103)
	cfg.paxos[leader1].Proposal(104)

	// New leader commits, also for index=2.
	cfg.one(103, 2, true)

	// New leader network failure.
	leader2 := cfg.checkOneLeader()
	cfg.disconnect(leader2)

	// Old leader connected again.
	cfg.connect(leader1)

	// some sleep to make the system stable
	time.Sleep(PaxosElectionTimeout)

	cfg.one(104, 2, true)

	// All together now.
	cfg.connect(leader2)

	cfg.one(105, servers, true)

	cfg.end()
}

func TestFig5ALogReplication2B(t *testing.T) {
	servers := 5
	cfg := makeConfig(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): [TestFig5ALogReplication2B] Partial Connectivity - Quorum-Loss Scenario (Fig 5A)")
	// Get an initial leader
	leader1 := cfg.checkOneLeader()
	log.Warn().Msgf("Initial leader is %v", leader1)

	// should be able to commit
	cfg.one(101, servers, false)

	// Now, we will mimic the scenario presented in Fig 2B
	// of the OmniPaxos paper

	// Let S != leader1, be a server in our clutser
	// S will be connected to everyone
	// every other server would ONLY be connected to S

	for i := 0; i < servers; i++ {
		// disconnect everyone
		cfg.disconnect(i)
	}

	// define S - any random server other than leader1
	S := rand.Int() % servers
	for ; S == leader1; S = rand.Int() % servers {
	}
	log.Warn().Msgf("S is %v", S)

	// connect S to everyone
	var serversToConnectTo []int
	for i := 0; i < servers; i++ {
		// if i != S {
		serversToConnectTo = append(serversToConnectTo, i)
		// }
	}
	cfg.partiallyConnect(S, serversToConnectTo)

	log.Warn().Msgf("Partial Connectivity Established. Server with QC is %v", S)

	// wait for some time so that the system can reach a steady state
	time.Sleep(2 * PaxosElectionTimeout)

	// should have a new leader since leader1
	// has lost quorum connectivity
	leader2 := cfg.checkOneLeader()
	log.Warn().Msgf("New leader is %v", leader2)

	if leader2 != S {
		t.Fatalf("Server %v is the only server with QC, yet got %v as a leader", S, leader2)
	}

	// should also commit
	cfg.paxos[leader2].Proposal(102)
	cfg.paxos[leader2].Proposal(103)
	cfg.paxos[leader2].Proposal(104)

	// restore original state
	// where everyone is connected

	for i := 0; i < servers; i++ {
		cfg.connect(i)
	}

	// wait for some time so that the system can reach a steady state
	time.Sleep(PaxosElectionTimeout)

	cfg.one(105, servers, false)
	cfg.one(106, servers, false)
	cfg.end()
}

func TestFig5BLogReplication2B(t *testing.T) {
	servers := 5
	cfg := makeConfig(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): [TestFig5BLogReplication2B] Partial Connectivity - Constrained Election Scenario (Fig 5B)")
	// Get an initial leader

	// should be able to commit
	cfg.one(101, servers, false)

	leader1 := cfg.checkOneLeader()
	log.Warn().Msgf("Initial leader is %v", leader1)

	// Now, we will mimic the scenario presented in Fig 2A
	// of the OmniPaxos paper

	// Let S != leader1, be a server in our clutser
	// S will be connected to everyone but leader1
	// every other server (except leader1) would ONLY be connected to S

	for i := 0; i < servers; i++ {
		// disconnect everyone
		cfg.disconnect(i)
	}

	// define S
	S := rand.Int() % servers
	for ; S == leader1; S = rand.Int() % servers {
	}
	log.Warn().Msgf("S is %v", S)

	// connect S to everyone (but leader1)
	var serversToConnectTo []int
	for i := 0; i < servers; i++ {
		if i != leader1 {
			serversToConnectTo = append(serversToConnectTo, i)
		}
	}
	cfg.partiallyConnect(S, serversToConnectTo)

	log.Warn().Msgf("Partial Connectivity Established. Server with QC is %v", S)

	// wait for some time so that the system can reach a steady state
	time.Sleep(2 * PaxosElectionTimeout)

	// should have a new leader since leader1
	// has lost quorum connectivity
	leader2 := cfg.checkOneLeader()
	log.Warn().Msgf("New leader is %v", leader2)
	if leader2 != S {
		t.Fatalf("Server %v is the only server with QC, yet got %v as a leader", S, leader2)
	}

	// should also commit
	// but only by (server-1) servers, since
	// leader1 is not connected to anyone
	cfg.paxos[leader2].Proposal(102)
	cfg.paxos[leader2].Proposal(103)
	cfg.paxos[leader2].Proposal(104)

	// restore original state
	// where everyone is connected

	for i := 0; i < servers; i++ {
		cfg.connect(i)
	}

	// wait for some time so that the system can reach a steady state
	time.Sleep(PaxosElectionTimeout)

	cfg.one(105, servers, false)
	cfg.one(106, servers, false)

	cfg.end()
}

func TestFig5CLogReplication2B(t *testing.T) {
	servers := 3
	cfg := makeConfig(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2B): [TestFig5cLogReplication2B] Partial Connectivity - Chained Scenario (Fig 5c)")
	// Get an initial leader
	leader1 := cfg.checkOneLeader()
	log.Warn().Msgf("Initial leader is %v", leader1)
	cfg.one(101, servers, false)

	// Now, we will mimic the scenario presented in Fig 5c
	// of the OmniPaxos paper

	// Let S != leader1, be a server in our clutser
	// S and leader1 will be disconnected from each other

	// define S
	S := rand.Int() % servers
	for ; S == leader1; S = rand.Int() % servers {
	}
	log.Warn().Msgf("S is %v", S)

	// connect S to everyone (but leader1)
	cfg.partiallyDisconnect(S, []int{leader1})

	log.Warn().Msgf("Partial Connectivity Established. Server with QC is %v", S)

	// wait for some time so that the system can reach a steady state
	time.Sleep(2 * PaxosElectionTimeout)

	// should have a new leader since leader1
	// has can't commnunicate with S
	// so S will timeout waiting for leader1's Heartbeat
	// and then increase it's ballot
	leader2 := cfg.checkOneLeader()
	log.Warn().Msgf("New leader is %v", leader2)
	if leader2 != S {
		t.Fatalf("Server %v is the only server with QC, yet got %v as a leader", S, leader2)
	}

	// should also commit
	// but only by (server-1) servers
	// since leader1 is not connected to
	// leader2 (see Fig5 C)
	cfg.paxos[leader2].Proposal(102)
	cfg.paxos[leader2].Proposal(103)
	cfg.paxos[leader2].Proposal(104)

	// restore original state
	// where everyone is connected

	for i := 0; i < servers; i++ {
		cfg.connect(i)
	}

	// wait for some time so that the system can reach a steady state
	time.Sleep(PaxosElectionTimeout)

	cfg.one(105, servers, false)
	cfg.one(106, servers, false)

	cfg.end()

}

func TestPersist12C(t *testing.T) {
	servers := 3
	cfg := makeConfig(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): basic persistence")

	cfg.one(11, servers, true)

	// disconnect the leader
	leader_og := cfg.checkOneLeader()
	cfg.disconnect(leader_og)
	leader_post_dc := cfg.checkOneLeader()
	log.Warn().Msgf("TESTER: Leader before huge crash: %v", leader_post_dc)
	cfg.connect(leader_og)

	// crash and re-start all
	for i := 0; i < servers; i++ {
		// if i == 2 {
		// 	continue
		// }

		log.Warn().Msgf("Crashing %v\n", i)
		cfg.start1(i, cfg.applier)
	}
	for i := 0; i < servers; i++ {
		log.Warn().Msgf("Reconnecting %v\n", i)
		cfg.disconnect(i)
		cfg.connect(i)
	}
	cfg.one(12, servers, true)

	leader1 := cfg.checkOneLeader()
	cfg.disconnect(leader1)
	cfg.start1(leader1, cfg.applier)
	cfg.connect(leader1)

	cfg.one(13, servers, true)

	leader2 := cfg.checkOneLeader()
	cfg.disconnect(leader2)
	index := cfg.one(14, servers-1, true)
	cfg.start1(leader2, cfg.applier)
	cfg.connect(leader2)

	cfg.wait(index, servers, -1) // wait for leader2 to join before killing i3

	i3 := (cfg.checkOneLeader() + 1) % servers
	cfg.disconnect(i3)
	cfg.one(15, servers-1, true)
	cfg.start1(i3, cfg.applier)
	cfg.connect(i3)

	// TODO:
	// cfg.one(16, servers, true)

	cfg.end()
}

func TestPersist22C(t *testing.T) {
	servers := 5
	cfg := makeConfig(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): more persistence")
	// disconnect the leader
	leader_og := cfg.checkOneLeader()
	cfg.disconnect(leader_og)
	leader_post_dc := cfg.checkOneLeader()
	log.Debug().Msgf("TESTER: Leader before huge crash: %v", leader_post_dc)
	cfg.connect(leader_og)

	index := 1
	for iters := 0; iters < 2; iters++ {
		cfg.one(10+index, servers, true)
		index++

		leader1 := cfg.checkOneLeader()

		cfg.disconnect((leader1 + 1) % servers)
		cfg.disconnect((leader1 + 2) % servers)

		cfg.one(10+index, servers-2, true)
		index++

		cfg.disconnect((leader1 + 0) % servers)
		cfg.disconnect((leader1 + 3) % servers)
		cfg.disconnect((leader1 + 4) % servers)

		cfg.start1((leader1+1)%servers, cfg.applier)
		cfg.start1((leader1+2)%servers, cfg.applier)
		cfg.connect((leader1 + 1) % servers)
		cfg.connect((leader1 + 2) % servers)

		time.Sleep(2 * PaxosElectionTimeout)
		cfg.start1((leader1+3)%servers, cfg.applier)
		cfg.connect((leader1 + 3) % servers)
		leader2 := cfg.checkOneLeader()
		log.Info().Msgf("New leader %v", leader2)

		cfg.one(10+index, servers-2, true)
		index++

		cfg.connect((leader1 + 4) % servers)
		cfg.connect((leader1 + 0) % servers)

		time.Sleep(2 * PaxosElectionTimeout)
	}

	cfg.one(1000, servers, true)

	cfg.end()
}

func TestPersist32C(t *testing.T) {
	servers := 3
	cfg := makeConfig(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): partitioned leader and one follower crash, leader restarts")

	cfg.one(101, 3, true)

	// disconnect the leader
	leader_og := cfg.checkOneLeader()
	cfg.disconnect(leader_og)
	leader_post_dc := cfg.checkOneLeader()
	log.Debug().Msgf("TESTER: Leader before huge crash: %v", leader_post_dc)
	cfg.connect(leader_og)

	leader := cfg.checkOneLeader()
	cfg.disconnect((leader + 2) % servers)

	cfg.one(102, 2, true)

	cfg.connect((leader + 2) % servers)
	cfg.crash1((leader + 0) % servers)
	cfg.crash1((leader + 1) % servers)

	time.Sleep(PaxosElectionTimeout)

	cfg.start1((leader+0)%servers, cfg.applier)
	cfg.connect((leader + 0) % servers)

	time.Sleep(PaxosElectionTimeout)
	cfg.one(103, 2, true)

	cfg.start1((leader+1)%servers, cfg.applier)
	cfg.connect((leader + 1) % servers)

	time.Sleep(PaxosElectionTimeout)
	cfg.one(104, servers, true)

	cfg.end()
}

func TestFig5APersistence2C(t *testing.T) {
	servers := 5
	cfg := makeConfig(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): [TestFig5APersistence2C] Partial Connectivity - Quorum-Loss Scenario (Fig 5A)")
	// Get an initial leader
	leader1 := cfg.checkOneLeader()
	log.Warn().Msgf("Initial leader is %v", leader1)

	// should be able to commit
	cfg.one(101, servers, false)

	// Now, we will mimic the scenario presented in Fig 5A
	// of the OmniPaxos paper

	// Let S != leader1, be a server in our clutser
	// S will be connected to everyone
	// every other server would ONLY be connected to S

	for i := 0; i < servers; i++ {
		// disconnect everyone
		cfg.disconnect(i)
	}

	// define S - any random server other than leader1
	S := rand.Int() % servers
	for ; S == leader1; S = rand.Int() % servers {
	}
	log.Warn().Msgf("S is %v", S)

	// connect S to everyone
	var serversToConnectTo []int
	for i := 0; i < servers; i++ {
		// if i != S {
		serversToConnectTo = append(serversToConnectTo, i)
		// }
	}
	cfg.partiallyConnect(S, serversToConnectTo)

	log.Warn().Msgf("Partial Connectivity Established. Server with QC is %v", S)

	// wait for some time so that the system can reach a steady state
	time.Sleep(2 * PaxosElectionTimeout)

	// should have a new leader since leader1
	// has lost quorum connectivity
	leader2 := cfg.checkOneLeader()
	log.Warn().Msgf("New leader is %v", leader2)

	if leader2 != S {
		t.Fatalf("Server %v is the only server with QC, yet got %v as a leader", S, leader2)
	}

	// should also commit
	cfg.paxos[leader2].Proposal(102)
	cfg.paxos[leader2].Proposal(103)
	cfg.paxos[leader2].Proposal(104)

	time.Sleep(PaxosElectionTimeout)

	// lets now crash servers other than the leader
	for i := 0; i < servers; i++ {
		if i == leader2 {
			continue
		}
		log.Warn().Msgf("Tester - Crashing and starting Server-%v", i)
		cfg.start1(i, cfg.applier) // crash the remaining servers
		cfg.connect(i)
	}

	// wait for some time so that the system can reach a steady state
	time.Sleep(PaxosElectionTimeout)

	cfg.one(105, servers, false)
	cfg.end()
}

func TestFig5BPersistence2C(t *testing.T) {
	servers := 5
	cfg := makeConfig(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): [TestFig5BPersistence2C] Partial Connectivity - Constrained Election Scenario (Fig 5B)")
	// Get an initial leader

	// should be able to commit
	cfg.one(101, servers, false)

	leader1 := cfg.checkOneLeader()
	log.Warn().Msgf("Initial leader is %v", leader1)

	// Now, we will mimic the scenario presented in Fig 2A
	// of the OmniPaxos paper

	// Let S != leader1, be a server in our clutser
	// S will be connected to everyone but leader1
	// every other server (except leader1) would ONLY be connected to S

	for i := 0; i < servers; i++ {
		// disconnect everyone
		cfg.disconnect(i)
	}

	// define S
	S := rand.Int() % servers
	for ; S == leader1; S = rand.Int() % servers {
	}
	log.Warn().Msgf("S is %v", S)

	// connect S to everyone (but leader1)
	var serversToConnectTo []int
	for i := 0; i < servers; i++ {
		if i != leader1 {
			serversToConnectTo = append(serversToConnectTo, i)
		}
	}
	cfg.partiallyConnect(S, serversToConnectTo)

	log.Warn().Msgf("Partial Connectivity Established. Server with QC is %v", S)

	// wait for some time so that the system can reach a steady state
	time.Sleep(2 * PaxosElectionTimeout)

	// should have a new leader since leader1
	// has lost quorum connectivity
	leader2 := cfg.checkOneLeader()
	log.Warn().Msgf("New leader is %v", leader2)
	if leader2 != S {
		t.Fatalf("Server %v is the only server with QC, yet got %v as a leader", S, leader2)
	}

	// should also commit
	// but only by (server-1) servers, since
	// leader1 is not connected to anyone
	cfg.paxos[leader2].Proposal(102)
	cfg.paxos[leader2].Proposal(103)
	cfg.paxos[leader2].Proposal(104)

	// wait for some time so that the system can reach a steady state
	time.Sleep(PaxosElectionTimeout)

	// lets now crash servers other than the leader
	for i := 0; i < servers; i++ {
		if i == leader2 {
			continue
		}
		log.Warn().Msgf("Tester - Crashing and Starting Server-%v", i)
		cfg.start1(i, cfg.applier) // crash the remaining servers
		cfg.connect(i)
	}

	// wait for some time so that the system can reach a steady state
	time.Sleep(PaxosElectionTimeout)

	cfg.one(105, servers, false)
	cfg.one(106, servers, false)

	cfg.end()
}

func TestFig5CPersistence2C(t *testing.T) {
	servers := 3
	cfg := makeConfig(t, servers, false, false)
	defer cfg.cleanup()

	cfg.begin("Test (2C): [TestFig5CPersistence2C] Partial Connectivity - Chained Scenario (Fig 5c)")
	// Get an initial leader
	leader1 := cfg.checkOneLeader()
	log.Warn().Msgf("Initial leader is %v", leader1)
	cfg.one(101, servers, false)

	// Now, we will mimic the scenario presented in Fig 5c
	// of the OmniPaxos paper

	// Let S != leader1, be a server in our clutser
	// S and leader1 will be disconnected from each other

	// define S
	S := rand.Int() % servers
	for ; S == leader1; S = rand.Int() % servers {
	}
	log.Warn().Msgf("S is %v", S)

	// connect S to everyone (but leader1)
	cfg.partiallyDisconnect(S, []int{leader1})

	log.Warn().Msgf("Partial Connectivity Established. Server with QC is %v", S)

	// wait for some time so that the system can reach a steady state
	time.Sleep(2 * PaxosElectionTimeout)

	// should have a new leader since leader1
	// has can't commnunicate with S
	// so S will timeout waiting for leader1's Heartbeat
	// and then increase it's ballot
	leader2 := cfg.checkOneLeader()
	log.Warn().Msgf("New leader is %v", leader2)
	if leader2 != S {
		t.Fatalf("Server %v is the only server with QC, yet got %v as a leader", S, leader2)
	}

	// should also commit
	// but only by (server-1) servers
	// since leader1 is not connected to
	// leader2 (see Fig5 C)
	cfg.paxos[leader2].Proposal(102)
	cfg.paxos[leader2].Proposal(103)
	cfg.paxos[leader2].Proposal(104)

	// wait for some time so that the system can reach a steady state
	time.Sleep(PaxosElectionTimeout)

	// lets now crash servers other than the leader
	for i := 0; i < servers; i++ {
		if i == leader2 {
			continue
		}
		log.Warn().Msgf("Tester - Crashing and starting Server-%v", i)
		cfg.start1(i, cfg.applier) // crash the remaining servers
		cfg.connect(i)
	}

	// wait for some time so that the system can reach a steady state
	time.Sleep(PaxosElectionTimeout)

	cfg.one(105, servers, false)
	cfg.one(106, servers, false)

	cfg.end()

}
