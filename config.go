package omnipaxos

//
// support for Omni-Paxos tester.
//
// we will use the original config.go to test your code for grading.
// so, while you can modify this code by, for example,
// adding print statements to help you debug, please
// test with the original before submitting. Thanks :)
//

import (
	"bytes"
	"cs651/labgob"
	"cs651/labrpc"
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"github.com/rs/zerolog/log"

	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"math/big"
	"time"
)

func randstring(n int) string {
	b := make([]byte, 2*n)
	_, _ = crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func makeSeed() int64 {
	maxInt := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, maxInt)
	x := bigx.Int64()
	return x
}

type logEntry struct {
	valid   bool
	command interface{}
}

type config struct {
	mu          sync.Mutex
	t           *testing.T
	net         *labrpc.Network
	n           int
	paxos       []*OmniPaxos // protected by `mu`
	connected   []bool       // whether each server is on the net; protected by `mu`
	saved       []*Persister
	endnames    [][]string         // the port file names each sends to
	logs        []map[int]logEntry // copy of each server's committed entries; protected by `mu`
	didRecv     []bool             // did receive any message from each server; protected by `mu`
	nextIndex   []int              // protected by `mu`
	numCommands []int              // number of commited valid commands for each server; protect by `mu`
	start       time.Time          // time at which makeConfig() was called
	// begin()/end() statistics
	t0        time.Time // time at which test_test.go called cfg.begin()
	rpcs0     int       // rpcTotal() at start of test
	bytes0    int64
	maxIndex  int // protected by `mu`
	maxIndex0 int
	stopCh    []chan struct{}

	// TODO add more fields for OmniPaxos testing here.
}

var ncpuOnce sync.Once

func makeConfig(t *testing.T, n int, unreliable bool, snapshot bool) *config {
	ncpuOnce.Do(func() {
		if runtime.NumCPU() < 2 {
			log.Warn().Msgf("Only one CPU, which may conceal locking bugs")
		}
		rand.Seed(makeSeed())
	})
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.t = t
	cfg.net = labrpc.MakeNetwork()
	cfg.n = n
	cfg.paxos = make([]*OmniPaxos, cfg.n)
	cfg.connected = make([]bool, cfg.n)
	cfg.saved = make([]*Persister, cfg.n)
	cfg.endnames = make([][]string, cfg.n)
	cfg.logs = make([]map[int]logEntry, cfg.n)
	cfg.didRecv = make([]bool, cfg.n)
	cfg.nextIndex = make([]int, cfg.n)
	cfg.numCommands = make([]int, cfg.n)
	cfg.start = time.Now()
	cfg.stopCh = make([]chan struct{}, cfg.n)

	cfg.setunreliable(unreliable)

	cfg.net.LongDelays(false)

	applier := cfg.applier

	for i := 0; i < cfg.n; i++ {
		cfg.logs[i] = map[int]logEntry{}
	}
	for i := 0; i < cfg.n; i++ {
		cfg.start1(i, applier)
	}
	// connect everyone
	for i := 0; i < cfg.n; i++ {
		cfg.connect(i)
	}

	return cfg
}

// shut down a Raft server but save its persistent state.
func (cfg *config) crash1(i int) {
	cfg.disconnect(i)
	cfg.net.DeleteServer(i) // disable client connections to the server.

	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	// a fresh persister, in case old instance
	// continues to update the Persister.
	// but copy old persister's content so that we always
	// pass Make() the last persisted state.
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	}

	rf := cfg.paxos[i]
	if rf != nil {
		cfg.mu.Unlock()
		close(cfg.stopCh[i])
		rf.Kill()
		cfg.mu.Lock()
		cfg.paxos[i] = nil
	}

	if cfg.saved[i] != nil {
		paxoslog := cfg.saved[i].ReadOmnipaxosState()
		snapshot := cfg.saved[i].ReadSnapshot()
		cfg.saved[i] = &Persister{}
		cfg.saved[i].SaveStateAndSnapshot(paxoslog, snapshot)
	}
}

// Checks that no server has committed a different value for the same index.
// Requires mutex `mu`.
func (cfg *config) checkConsistency(server int, index int, logEntry logEntry) {
	for j := 0; j < len(cfg.logs); j++ {
		e, ok := cfg.logs[j][index]
		if ok && (e != logEntry) {
			log.Fatal().Msgf("%v Trying to commit logEntry: %+v at index: %d "+
				"but server j: %d has already committed e: %+v for the same index",
				server, logEntry, index, j, e)
		}
	}
}

func (cfg *config) apply(server int, m ApplyMsg) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	if cfg.didRecv[server] == false {
		cfg.didRecv[server] = true
		if m.CommandIndex == 1 {
			if cfg.nextIndex[server] != 0 {
				// panic("assertion failed on cfg.nextindex[server]!=0")
				log.Warn().Int("Server", server).Msgf("Setting nextindex to 0 as first message starts at 0")
			} else {
				log.Warn().Int("Server", server).Msgf("Setting nextindex to 1 as first message starts at 1")
				cfg.nextIndex[server] = 1
			}
		}
	}
	log.Warn().Msgf("Tester - Received Apply from server %v - %+v", server, m)
	if m.CommandIndex != cfg.nextIndex[server] {
		log.Fatal().Int("Server", server).Msgf("Applied index %d but expected %d",
			m.CommandIndex, cfg.nextIndex[server])
	}
	cfg.nextIndex[server]++

	logEntry := logEntry{m.CommandValid, m.Command}
	cfg.checkConsistency(server, m.CommandIndex, logEntry)
	cfg.logs[server][m.CommandIndex] = logEntry

	if m.CommandIndex > cfg.maxIndex {
		cfg.maxIndex = m.CommandIndex
	}

	if m.CommandValid {
		cfg.numCommands[server]++
	}
}

// applier reads message from apply ch and checks that they match the log contents.
func (cfg *config) applier(server int, applyCh chan ApplyMsg, stopCh <-chan struct{}) {
	for {
		select {
		case <-stopCh:
			{
				// When applier is stopped, it still continues to empty `applyCh`.
				stopCh = nil
			}
		case m, ok := <-applyCh:
			{
				if !ok {
					return
				}
				if stopCh != nil {
					cfg.apply(server, m)
				}
			}
		}
	}
}

// Check that `command` has been committed at `index`
// Requires mutex `mu`.
func (cfg *config) checkCommitted(index int, command interface{}) {
	for i := 0; i < len(cfg.logs); i++ {
		e, ok := cfg.logs[i][index]
		if ok {
			if !e.valid {
				log.Fatal().Int("Server", i).Msgf("Snapshot with index %d has value %v but server stores a dummy "+
					"entry for this index (snapshots are only created for indices with valid commands)", index, command)
			}
			if e.command != command {
				log.Fatal().Int("Server", i).Msgf("Snapshot with index %d has value %v but server stores command %v "+
					"for this index", index, command, e.command)
			}
			return
		}
	}
	log.Fatal().Msgf("no log entry has been found for snapshot index %d", index)
}

func decodeSnapshot(snapshot []byte) (int, error) {
	buf := bytes.NewBuffer(snapshot)
	dec := labgob.NewDecoder(buf)
	var v int
	err := dec.Decode(&v)
	return v, err
}

func encodeSnapshot(v int) []byte {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	_ = enc.Encode(v)
	return buf.Bytes()
}

// start or re-start a Raft.
// if one already exists, "kill" it first.
// allocate new outgoing port file names, and a new
// state persister, to isolate previous instance of
// this server. since we cannot really kill it.
func (cfg *config) start1(i int, applier func(int, chan ApplyMsg, <-chan struct{})) {
	cfg.crash1(i)

	// a fresh set of outgoing ClientEnd names.
	// so that old crashed instance's ClientEnds can't send.
	cfg.endnames[i] = make([]string, cfg.n)
	for j := 0; j < cfg.n; j++ {
		cfg.endnames[i][j] = randstring(20)
	}

	// a fresh set of ClientEnds.
	ends := make([]*labrpc.ClientEnd, cfg.n)
	for j := 0; j < cfg.n; j++ {
		ends[j] = cfg.net.MakeEnd(cfg.endnames[i][j])
		cfg.net.Connect(cfg.endnames[i][j], j)
	}

	cfg.mu.Lock()

	cfg.nextIndex[i] = 0

	// a fresh persister, so old instance doesn't overwrite
	// new instance's persisted state.
	// but copy old persister's content so that we always
	// pass Make() the last persisted state.
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	} else {
		cfg.saved[i] = MakePersister()
	}

	cfg.mu.Unlock()

	applyCh := make(chan ApplyMsg)
	cfg.stopCh[i] = make(chan struct{})
	go applier(i, applyCh, cfg.stopCh[i])

	rf := Make(ends, i, cfg.saved[i], applyCh)

	cfg.mu.Lock()
	cfg.paxos[i] = rf

	cfg.didRecv[i] = false
	log.Trace().Msgf("Resetting didRecv[%d] to false as new server is created", i)

	cfg.mu.Unlock()

	svc := labrpc.MakeService(rf)
	srv := labrpc.MakeServer()
	srv.AddService(svc)
	cfg.net.AddServer(i, srv)
}

func (cfg *config) checkTimeout() {
	// enforce a two-minute real-time limit on each test
	if !cfg.t.Failed() && time.Since(cfg.start) > 120*time.Second {
		cfg.t.Fatal("test took longer than 120 seconds")
	}
}

func (cfg *config) cleanup() {
	for i := 0; i < len(cfg.paxos); i++ {
		if cfg.paxos[i] != nil {
			cfg.paxos[i].Kill()
		}
	}
	cfg.net.Cleanup()
	cfg.checkTimeout()
}

// attach server i to the net.
func (cfg *config) connect(i int) {
	// log.Warn().Msgf("TESTER - Connecting %v", i)

	cfg.connected[i] = true

	// outgoing ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.connected[j] {
			endname := cfg.endnames[i][j]
			cfg.net.Enable(endname, true)
		}
	}

	// incoming ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.connected[j] {
			endname := cfg.endnames[j][i]
			cfg.net.Enable(endname, true)
		}
	}
}

// detach server i from the net.
func (cfg *config) disconnect(i int) {
	// log.Warn().Msgf("TESTER - Disconnecting %v", i)

	cfg.connected[i] = false

	// outgoing ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.endnames[i] != nil {
			endname := cfg.endnames[i][j]
			cfg.net.Enable(endname, false)
		}
	}

	// incoming ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.endnames[j] != nil {
			endname := cfg.endnames[j][i]
			cfg.net.Enable(endname, false)
		}
	}
}

// detach server i from the the list of given servers.
// DONT use for complete disconnection!
func (cfg *config) partiallyDisconnect(i int, serversToDisconnectFrom []int) {
	// log.Warn().Msgf("TESTER - Partially Disconnecting %v  (from %v)", i, serversToDisconnectFrom)

	// outgoing ClientEnds
	for j := 0; j < len(serversToDisconnectFrom); j++ {
		server := serversToDisconnectFrom[j]
		if cfg.endnames[i] != nil {
			endname := cfg.endnames[i][server]
			cfg.net.Enable(endname, false)
		}
	}

	// incoming ClientEnds
	for j := 0; j < len(serversToDisconnectFrom); j++ {
		server := serversToDisconnectFrom[j]
		if cfg.endnames[server] != nil {
			endname := cfg.endnames[server][i]
			cfg.net.Enable(endname, false)
		}
	}
}

// attach server i to the net.
func (cfg *config) partiallyConnect(i int, serversToConnectTo []int) {
	// log.Warn().Msgf("TESTER - Connecting %v (to %v)", i, serversToConnectTo)

	cfg.connected[i] = true

	// outgoing ClientEnds
	for j := 0; j < len(serversToConnectTo); j++ {
		server := serversToConnectTo[j]
		cfg.connected[server] = true
		if cfg.connected[server] {
			endname := cfg.endnames[i][server]
			cfg.net.Enable(endname, true)
		}
	}

	// incoming ClientEnds
	for j := 0; j < len(serversToConnectTo); j++ {
		server := serversToConnectTo[j]
		if cfg.connected[server] {
			endname := cfg.endnames[server][i]
			cfg.net.Enable(endname, true)
		}
	}
}

func (cfg *config) rpcCount(server int) int {
	return cfg.net.GetCount(server)
}

func (cfg *config) rpcTotal() int {
	return cfg.net.GetTotalCount()
}

func (cfg *config) setunreliable(unrel bool) {
	cfg.net.Reliable(!unrel)
}

func (cfg *config) bytesTotal() int64 {
	return cfg.net.GetTotalBytes()
}

func (cfg *config) setlongreordering(longrel bool) {
	cfg.net.LongReordering(longrel)
}

// check that there's exactly one leader.
// try a few times in case re-elections are needed.
func (cfg *config) checkOneLeader() int {
	for iters := 0; iters < 10; iters++ {
		ms := 1000 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		leaders := make(map[int][]int)
		for i := 0; i < cfg.n; i++ {
			if cfg.connected[i] {
				if ballot, leader := cfg.paxos[i].GetState(); leader {
					leaders[ballot] = append(leaders[ballot], i)
				}
			}
		}

		lastballotWithLeader := -1
		for ballot, leaders := range leaders {
			if len(leaders) > 1 {
				cfg.t.Fatalf("ballot %d has %d (>1) leaders %v", ballot, len(leaders), leaders)
			}
			if ballot > lastballotWithLeader {
				lastballotWithLeader = ballot
			}
		}

		if len(leaders) != 0 {
			return leaders[lastballotWithLeader][0]
		}
	}
	cfg.t.Fatalf("expected one leader, got none")
	return -1
}

// check that everyone agrees on the ballot.
func (cfg *config) checkballots() int {
	ballot := -1
	for i := 0; i < cfg.n; i++ {
		if cfg.connected[i] {
			xballot, _ := cfg.paxos[i].GetState()
			if ballot == -1 {
				ballot = xballot
			} else if ballot != xballot {
				cfg.t.Fatalf("servers disagree on ballot")
			}
		}
	}
	return ballot
}

// check that there's no leader
func (cfg *config) checkNoLeader() {
	for i := 0; i < cfg.n; i++ {
		if cfg.connected[i] {
			_, isLeader := cfg.paxos[i].GetState()
			if isLeader {
				cfg.t.Fatalf("expected no leader, but %v claims to be leader", i)
			}
		}
	}
}

// how many servers think a log entry is committed?
func (cfg *config) nCommitted(index int) (int, logEntry) {
	count := 0
	var logEntry logEntry
	for i := 0; i < len(cfg.paxos); i++ {
		cfg.mu.Lock()
		e, ok := cfg.logs[i][index]
		nextIndex := cfg.nextIndex[i]
		// log.Warn().Msgf("nCommitted for server %v - ok %v, nextIndex %v", i, cfg.logs[i], nextIndex)
		cfg.mu.Unlock()
		if ok && (nextIndex > index) {
			logEntry = e
			count++
		}
	}
	return count, logEntry
}

// wait for at least n servers to commit.
// but don't wait forever.
func (cfg *config) wait(index int, n int, startballot int) (bool, logEntry) {
	to := 10 * time.Millisecond
	for iters := 0; iters < 30; iters++ {
		nd, _ := cfg.nCommitted(index)
		if nd >= n {
			break
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
		if startballot > -1 {
			for _, p := range cfg.paxos {
				if t, _ := p.GetState(); t > startballot {
					// someone has moved on
					// can no longer guarantee that we'll "win"
					return false, logEntry{}
				}
			}
		}
	}
	nd, e := cfg.nCommitted(index)
	if nd < n {
		cfg.t.Fatalf("only %d decided for index %d; wanted %d\n",
			nd, index, n)
	}
	return true, e
}

// do a complete agreement.
// it might choose the wrong leader initially,
// and have to re-submit after giving up.
// entirely gives up after about 10 seconds.
// indirectly checks that the servers agree on the
// same value, since nCommitted() checks this,
// as do the threads that read from applyCh.
// returns index.
// if retry==true, we may submit the command multiple
// times, in case a leader fails just after Start().
// if retry==false, calls Start() only once, in order
// to simplify the early Lab 2B tests.
func (cfg *config) one(cmd interface{}, expectedServers int, retry bool) int {
	// log.Warn().Msgf("Submitting %v", cmd)
	t0 := time.Now()
	starts := 0
	for time.Since(t0).Seconds() < 10 {
		// try all the servers, maybe one is the leader.
		index := -1
		for si := 0; si < cfg.n; si++ {
			starts = (starts + 1) % cfg.n
			var op *OmniPaxos
			cfg.mu.Lock()
			if cfg.connected[starts] {
				op = cfg.paxos[starts]
			}
			cfg.mu.Unlock()
			if op != nil {
				index1, _, ok := op.Proposal(cmd)
				if ok {
					log.Warn().Msgf("Tester - Proposal Accepted by %v - %v", starts, cmd)
					index = index1
					break
				}
			}
		}

		if index != -1 {
			// somebody claimed to be the leader and to have
			// submitted our command; wait a while for agreement.
			t1 := time.Now()
			for time.Since(t1).Seconds() < 2 {
				nd, e := cfg.nCommitted(index)
				// log.Warn().Msgf("Number of Committed is %v", nd)
				if nd > 0 && nd >= expectedServers {
					// committed
					if e == (logEntry{true, cmd}) {
						// and it was the command we submitted.
						return index
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
			if retry == false {
				log.Fatal().Msgf("one(%v) failed to reach agreement", cmd)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	log.Fatal().Msgf("one(%v) failed to reach agreement", cmd)
	return -1
}

// begin starts a test and print the test message.
// e.g. cfg.begin("Test (2B): RPC counts aren't too high")
func (cfg *config) begin(description string) {
	fmt.Printf("%s ...\n", description)
	cfg.t0 = time.Now()
	cfg.rpcs0 = cfg.rpcTotal()
	cfg.bytes0 = cfg.bytesTotal()
	cfg.maxIndex0 = cfg.maxIndex
	// TODO: PG See if this is even needed?
	time.Sleep(500 * time.Millisecond)
}

// end a Test -- the fact that we got here means there
// was no failure. Print the Passed message,
// and some performance numbers.
func (cfg *config) end() {
	cfg.checkTimeout()
	if cfg.t.Failed() == false {

		cfg.mu.Lock()

		t := time.Since(cfg.t0).Seconds()       // real time
		npeers := cfg.n                         // number of Raft peers
		nrpc := cfg.rpcTotal() - cfg.rpcs0      // number of RPC sends
		nbytes := cfg.bytesTotal() - cfg.bytes0 // number of bytes
		ncmds := cfg.maxIndex - cfg.maxIndex0   // number of Raft agreements reported
		cfg.mu.Unlock()

		fmt.Printf("  ... Passed --")
		fmt.Printf("  %4.1f  %d %4d %7d %4d\n", t, npeers, nrpc, nbytes, ncmds)
	}
}

// LogSize returns the maximum log size across all servers
func (cfg *config) LogSize() int {
	logsize := 0
	for i := 0; i < cfg.n; i++ {
		n := cfg.saved[i].OmnipaxosStateSize()
		if n > logsize {
			logsize = n
		}
	}
	return logsize
}

func (cfg *config) NumberOfGossipRPCs(n_servers int) int {
	if n_servers < 2 {
		return 0
	}
	return (n_servers * (n_servers - 1)) / 2
}
