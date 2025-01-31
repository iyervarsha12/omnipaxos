package omnipaxos

//
// This is an outline of the API that OmniPaxos must expose to
// the service (or tester). See comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   Create a new OmniPaxos server.
// op.Start(command interface{}) (index, ballot, isleader)
//   Start agreement on a new log entry
// op.GetState() (ballot, isLeader)
//   ask a OmniPaxos for its current ballot, and whether it thinks it is leader
// ApplyMsg
//   Each time a new entry is committed to the log, each OmniPaxos peer
//   should send an ApplyMsg to the service (or tester) in the same server.
//

import (
	"cs651/labrpc"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"bytes"
	"cs651/labgob"
)

type State struct {
	role  string //follower or leader
	phase string //prepare or accept
}

type Ballot struct {
	Bno1 int
	Bno2 int
	Qc   bool
}

type BallotRnd struct {
	Bno1 int 
	Bno2 int 
}

type Promise struct {
	AcceptedRnd BallotRnd
	LogIdx      int
	F           int
	DecIdx      int
	Sfx         []interface{}
}

// A Go object implementing a single OmniPaxos peer.
type OmniPaxos struct {
	mu            sync.Mutex          // Lock to protect shared access to this peer's state
	peers         []*labrpc.ClientEnd // RPC end points of all peers
	persister     *Persister          // Object to hold this peer's persisted state
	me            int                 // This peer's index into peers[]
	dead          int32               // Set by Kill()
	enableLogging int32
	// Your code here (2A, 2B).
	state   State  //role "follower or leader", phase "prepare or accept"
	l       Ballot // ballot number of current leader
	r       int    // current heartbeat round, init 0
	b       Ballot // init (0, pid)
	delay   time.Duration
	ballots []Ballot

	//2B
	promisedRnd BallotRnd // double
	acceptedRnd BallotRnd // double
	decidedIdx  int 
	currentRnd  BallotRnd // double
	promises    []Promise
	maxProm     Promise
	log         []interface{}
	accepted    []int
	buffer      []interface{}

	//Recovery
	linkdrop	bool
	deadrounds 	int

	applyCh chan ApplyMsg
}

type Dummy struct {
}

// As each OmniPaxos peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// Ensure that all your fields for a RCP start with an Upper Case letter
type HBRequest struct {
	// Your code here (2A). Each server sends HBRequest to all other servers when startTimer expires
	// Don't send to yourself!!
	ServerSent int
	Rnd        int //round of this request
}

type HBReply struct {
	// Your code here (2A). A server that gets HBrequest responds with this.
	ServerReceived int
	Rnd            int    //Round reply was sent in
	BallotHBReply  Ballot //Ballot number of ServerReceived
	Q              bool   //qc of ServerReceived
}

func (op *OmniPaxos) suffix(idx int) []interface{} {
	if idx <= 0 || idx >= len(op.log) {
		return make([]interface{}, 0)
	} else {
		return op.log[idx:]
	}
}

func (op *OmniPaxos) prefix(idx int) []interface{} {
	if idx <= 0 || idx > len(op.log) {
		return make([]interface{}, 0)
	} else {
		return op.log[0:idx]
	}
}

func (op *OmniPaxos) HBRequestRPC(args *HBRequest, dummyresponse *Dummy) {

	op.mu.Lock()
	defer op.mu.Unlock()
	hbreply := new(HBReply)
	hbreply.ServerReceived = op.me
	hbreply.Rnd = args.Rnd
	hbreply.BallotHBReply = Ballot{}
	hbreply.BallotHBReply.Bno1 = op.b.Bno1
	hbreply.BallotHBReply.Bno2 = op.b.Bno2
	hbreply.BallotHBReply.Qc = op.b.Qc
	go op.peers[args.ServerSent].Call("OmniPaxos.HBReplyRPC", hbreply, dummyresponse)

}

func (op *OmniPaxos) HBReplyRPC(args *HBReply, dummyresponse *Dummy) {
	op.mu.Lock()
	defer op.mu.Unlock()
	if args.Rnd == op.r {
		tempballot := new(Ballot)
		tempballot.Bno1 = args.BallotHBReply.Bno1
		tempballot.Bno2 = args.BallotHBReply.Bno2
		tempballot.Qc = args.BallotHBReply.Qc
		op.ballots = append(op.ballots, *tempballot)
	}
}

// Prepare RPC, from leader to all peers
type PrepareStruct struct {
	ServerSent int //server sent
	N          BallotRnd //round of leader l
	AccRnd     BallotRnd //AcceptedRnd of l
	LogIdx     int //length of leader log
	DecIdx     int //decidedIdx of l
}

// is args1 > args2 lexicographically? 
func greaterthan(args1 BallotRnd, args2 BallotRnd) bool {
	if(args1.Bno1 > args2.Bno1 || (args1.Bno1 == args2.Bno1 && args1.Bno2 > args2.Bno2)) {
		return true;
	} else {
		return false;
	}
}

// is args1 < args2 lexicographically? 
func lesserthan(args1 BallotRnd, args2 BallotRnd) bool {
	if(args1.Bno1 < args2.Bno1 || (args1.Bno1 == args2.Bno1 && args1.Bno2 < args2.Bno2)) {
		return true;
	} else {
		return false;
	}
}

// is args1 == args2 lexicographically? 
func equalto(args1 BallotRnd, args2 BallotRnd) bool {
	if(args1.Bno1 == args2.Bno1 && args1.Bno2 == args2.Bno2) {
		return true;
	} else {
		return false;
	}
}

func (op *OmniPaxos) PrepareRPC(args *PrepareStruct, dummyresponse *Dummy) {
	op.mu.Lock()
	defer op.mu.Unlock()
	if greaterthan(op.promisedRnd,args.N) {
		return
	}
	//fmt.Println("Current log in Preparerpc beginning is ", op.log)
	op.state.role = "FOLLOWER"
	op.state.phase = "PREPARE"
	op.promisedRnd  = args.N 
	op.persist() //persist promisedRnd

	sfx := make([]interface{}, 0)
	//fmt.Printf("============= SFX %v\n", sfx)

	if greaterthan(op.acceptedRnd, args.AccRnd) {
		sfx = op.suffix(args.DecIdx)
		//fmt.Println("============= Goes into logidx, op.acceptedRnd, args.AccRnd and args.DecIdx is", op.acceptedRnd, args.AccRnd, args.DecIdx)
	} else if equalto(op.acceptedRnd, args.AccRnd) {
		sfx = op.suffix(args.LogIdx)
		//fmt.Println("============= Goes into logidx, op.acceptedRnd and args.LogIdx is", op.acceptedRnd, args.LogIdx)
	}
	//fmt.Println("Current log in preparerpc is ", op.log)
	//fmt.Printf("============= SFX NEW %v\n", sfx)
	// PromiseRPC to leader l, which is args.SeverSent
	promisestruct := new(PromiseStruct)
	promisestruct.ServerSent = op.me
	promisestruct.N = args.N
	promisestruct.AccRnd = op.acceptedRnd
	promisestruct.LogIdx = len(op.log) 
	promisestruct.DecIdx = op.decidedIdx
	promisestruct.Sfx = sfx

	go op.peers[args.ServerSent].Call("OmniPaxos.PromiseRPC", promisestruct, dummyresponse)

}

// PromiseRPC is sent from follower to leader
type PromiseStruct struct {
	ServerSent int
	N          BallotRnd
	AccRnd     BallotRnd
	LogIdx     int
	DecIdx     int
	Sfx        []interface{}
}

func (op *OmniPaxos) PromiseRPC(args *PromiseStruct, dummyresponse *Dummy) {
	op.mu.Lock()
	defer op.mu.Unlock()
	if !equalto(args.N, op.currentRnd) { 
		return
	}
	/*
	AcceptedRnd int
	LogIdx      int
	F           int
	DecIdx      int
	Sfx         []interface{}
	*/

	temppromise := *new(Promise)
	temppromise.AcceptedRnd = args.AccRnd
	temppromise.LogIdx = args.LogIdx 
	temppromise.F = args.ServerSent
	temppromise.DecIdx = args.DecIdx 
	temppromise.Sfx = args.Sfx
	//If args.ServerSent has a promise already in promises[], replace it with this one; else append it at the end
	
	flagAppend := true;
	for j:=0; j<len(op.promises); j++ {
		if(op.promises[j].F == temppromise.F) { //Time to replace that promise with the newest one
			op.promises[j] = temppromise
			flagAppend = false;
		}
	}
	if(flagAppend) { //Appending it at the end
		op.promises = append(op.promises, temppromise)
	}
	

	if op.state.role == "LEADER" {
		if op.state.phase == "PREPARE" {
			if len(op.promises) <= len(op.peers)/2 {
				return //not committing!
			}

			// Iterate through promises and find value with highest accrnd
			op.maxProm = op.promises[0] //op.promises[0] will def exist as it would have exited above otherwise
			for i := 1; i < len(op.promises); i++ {
				if greaterthan(op.promises[i].AcceptedRnd,op.maxProm.AcceptedRnd) {
					op.maxProm = op.promises[i]
				} else if (equalto(op.promises[i].AcceptedRnd,op.maxProm.AcceptedRnd) && op.promises[i].LogIdx > op.maxProm.LogIdx) {
					op.maxProm = op.promises[i]
				}
			}

			if !equalto(op.maxProm.AcceptedRnd,op.acceptedRnd) {
				op.log = op.prefix(op.decidedIdx)
			}

			op.log = append(op.log, op.maxProm.Sfx...)
			//fmt.Println("Current log in promiseRPC1 is ", op.log)

			// if stopped() logic. if yes, clear buffer, else append buffer to log
			// stopped() means: true if last entry in log is SS, else false
			if op.Stopped() {
				op.buffer = make([]interface{}, 0)
			} else {
				op.log = append(op.log, op.buffer...)
				//fmt.Println("Current log in promiseRPC2 is ", op.log)
			}

			op.acceptedRnd = op.currentRnd
			op.persist() //to persist op.log and op.acceptedRnd
			op.accepted[op.me] = len(op.log)
			op.state.role = "LEADER"
			op.state.phase = "ACCEPT"

			//for each p in promises, calculate a syncIdx out of it

			for _, p := range op.promises {
				if(p.F != op.me) {
					syncIdx := p.DecIdx
					// Index i, p is individual promise
					if equalto(p.AcceptedRnd,op.maxProm.AcceptedRnd) {
						syncIdx = p.LogIdx
					} else {
						syncIdx = p.DecIdx
					}
					// AcceptSync must be sent to all promised peers
					acceptsyncstruct := new(AcceptSyncStruct)
					acceptsyncstruct.ServerSent = op.me
					acceptsyncstruct.N = op.currentRnd

					acceptsyncstruct.Sfx = op.suffix(syncIdx)
					acceptsyncstruct.SyncIdx = syncIdx
					go op.peers[p.F].Call("OmniPaxos.AcceptSyncRPC", acceptsyncstruct, dummyresponse)
				}
			}

		} else if op.state.phase == "ACCEPT" {
			syncIdx := args.DecIdx
			if equalto(args.AccRnd, op.maxProm.AcceptedRnd) {
				syncIdx = op.maxProm.LogIdx
			} else {
				syncIdx = args.DecIdx
			}

			acceptsyncstruct := new(AcceptSyncStruct)
			acceptsyncstruct.ServerSent = op.me
			acceptsyncstruct.N = op.currentRnd

			acceptsyncstruct.Sfx = op.suffix(syncIdx)
			acceptsyncstruct.SyncIdx = syncIdx
			go op.peers[args.ServerSent].Call("OmniPaxos.AcceptSyncRPC", acceptsyncstruct, dummyresponse)

			if op.decidedIdx > args.DecIdx {
				decidestruct := new(DecideStruct)
				decidestruct.ServerSent = op.me
				decidestruct.N = op.currentRnd
				decidestruct.DecIdx = op.decidedIdx 
				go op.peers[args.ServerSent].Call("OmniPaxos.DecideRPC", decidestruct, dummyresponse)
			}

		}
	}
}

// AcceptSync RPC is sent from leader to follower
type AcceptSyncStruct struct {
	ServerSent int
	N          BallotRnd           //round of leader l
	Sfx        []interface{} //entries to append
	SyncIdx    int           //log position where Sfx should be appended
}

func (op *OmniPaxos) AcceptSyncRPC(args *AcceptSyncStruct, dummyresponse *Dummy) {
	op.mu.Lock()
	defer op.mu.Unlock()
	if !equalto(op.promisedRnd, args.N) || !(op.state.role == "FOLLOWER" && op.state.phase == "PREPARE") {
		return
	}
	op.acceptedRnd = args.N
	op.state.phase = "ACCEPT"
	op.log = op.prefix(args.SyncIdx)
	op.log = append(op.log, args.Sfx...)
	op.persist()
	//fmt.Println("Current log in AcceptSyncRPC is ", op.log)

	acceptedstruct := new(AcceptedStruct)
	acceptedstruct.ServerSent = op.me
	acceptedstruct.N = args.N
	acceptedstruct.LogIdx = len(op.log)
	go op.peers[args.ServerSent].Call("OmniPaxos.AcceptedRPC", acceptedstruct, dummyresponse)

}

// Accepted RPC sent from follower to leader
type AcceptedStruct struct {
	ServerSent int
	N          BallotRnd //promised round
	LogIdx     int // Position in log f has accepted up to
}

// divergent
func (op *OmniPaxos) AcceptedRPC(args *AcceptedStruct, dummyresponse *Dummy) {
	op.mu.Lock()
	defer op.mu.Unlock()
	if !equalto(op.currentRnd, args.N) || !(op.state.role == "LEADER" && op.state.phase == "ACCEPT") { //DOUBTLEX
		return
	}
	op.accepted[args.ServerSent] = args.LogIdx

	// Check if a majority has logIdx in accepted[] array
	countlogidx := 0
	for i := 0; i < len(op.accepted); i++ {
		if op.accepted[i] == args.LogIdx {
			countlogidx++
		}
	}

	if args.LogIdx >= op.decidedIdx && countlogidx > (len(op.peers)/2) { //a majority has accepted logIdx
		// there might be a todo here
		if op.decidedIdx != -1 {
			for i := op.decidedIdx; i < args.LogIdx; i++ {
				applymsg := *new(ApplyMsg)
				applymsg.CommandValid = true
				applymsg.Command = op.log[i] 
				applymsg.CommandIndex = i
				op.applyCh <- applymsg
				time.Sleep(10 * time.Millisecond)
			}
		}
		op.decidedIdx = args.LogIdx
		op.persist() 

		decidestruct := new(DecideStruct)
		decidestruct.ServerSent = op.me
		decidestruct.N = op.currentRnd
		decidestruct.DecIdx = op.decidedIdx

		// send to all promised followers.
		for i:=0; i<len(op.promises); i++ {
			idx := op.promises[i].F 
			if idx != op.me {
				go op.peers[idx].Call("OmniPaxos.DecideRPC", decidestruct, dummyresponse)
			}
		}
	}
	
}
	

// Decide RPC sent from leader l to followerS
type DecideStruct struct {
	ServerSent int
	N          BallotRnd //round of leader l
	DecIdx     int // Position in log that has been decided
}

//divergent
func (op *OmniPaxos) DecideRPC(args *DecideStruct, dummyresponse *Dummy) {
	op.mu.Lock()
	defer op.mu.Unlock()
	if equalto(op.promisedRnd,args.N) && op.state.role == "FOLLOWER" && op.state.phase == "ACCEPT" { //DOUBTLEX
		//might be a todo here
		//fmt.Println("DecideRPC follower ",op.me," follower accept")
		if op.decidedIdx != -1 {
			for i := op.decidedIdx; i < args.DecIdx; i++ {
				applymsg := *new(ApplyMsg)
				applymsg.CommandValid = true 
				applymsg.Command = op.log[i] 
				applymsg.CommandIndex = i
				op.applyCh <- applymsg
				time.Sleep(10 * time.Millisecond)
			}
		}

		op.decidedIdx = args.DecIdx
		op.persist() 
	}
}

// Accept RPC sent from leader l to follower
type AcceptStruct struct {
	ServerSent int
	N          BallotRnd         //round of leader l
	C          interface{} //client request
}

func (op *OmniPaxos) AcceptRPC(args *AcceptStruct, dummyresponse *Dummy) {
	op.mu.Lock()
	defer op.mu.Unlock()
	if !equalto(op.promisedRnd, args.N) || !(op.state.role == "FOLLOWER" && op.state.phase == "ACCEPT") { //DOUBTLEX
		return
	}
	fmt.Println("AcceptRPC called for server ", op.me, " by leader", args.ServerSent, "for command ", args.C);
	op.log = append(op.log, args.C)
	op.persist()
	//fmt.Println("Current log in AcceptRPC is ", op.log)

	acceptedstruct := new(AcceptedStruct)
	acceptedstruct.ServerSent = op.me
	acceptedstruct.N = args.N
	acceptedstruct.LogIdx = len(op.log)
	go op.peers[args.ServerSent].Call("OmniPaxos.AcceptedRPC", acceptedstruct, dummyresponse)

}

type PrepareReqStruct struct {
	ServerSent int 
}

func (op *OmniPaxos) PrepareReqRPC(args *PrepareReqStruct, dummyresponse *Dummy) {
	op.mu.Lock()
	defer op.mu.Unlock()

	if op.state.role!="LEADER" {
		return;
	}
	preparestruct := new(PrepareStruct)
	preparestruct.ServerSent = op.me
	preparestruct.N = op.currentRnd
	preparestruct.AccRnd = op.acceptedRnd
	preparestruct.LogIdx = len(op.log)
	preparestruct.DecIdx = op.decidedIdx
	go op.peers[args.ServerSent].Call("OmniPaxos.PrepareRPC", preparestruct, dummyresponse)
}

// Sample code for sending an RPC to another server
/*
func (op *OmniPaxos) sendHeartBeats(server int, args *HBRequest, reply *HBReply) bool {
	ok := op.peers[server].Call("OmniPaxos.HeartBeatHandler", args, reply)
	return ok
}
*/

/*
func (op *OmniPaxos) HeartBeatHandler(args *HBRequest, reply *HBReply) {
	// Your code here (2A).
}
*/

// TODO: if stopped(): true if last entry in log is SS, else false
func (op *OmniPaxos) Stopped() bool {
	return false
}

// GetState Return the current leader's ballot and whether this server "op"
// believes it is the leader.
func (op *OmniPaxos) GetState() (int, bool) {
	var ballot int
	var isleader bool = false

	// Your code here (2A).
	op.mu.Lock()
	ballot = op.l.Bno1
	/*
	if op.state.role == "Leader" {
		isleader = true
	}
		*/
	if op.me == op.l.Bno2 {
		isleader = true
	}
	op.mu.Unlock()

	return ballot, isleader
}

// Called by the tester to submit a log to your OmniPaxos server
// Implement this as described in Figure 3
func (op *OmniPaxos) Proposal(command interface{}) (int, int, bool) {
	op.mu.Lock()
	defer op.mu.Unlock()
	index := -1
	ballot := -1
	isLeader := false

	// Your code here (2B).
	if op.Stopped() {
		return index, ballot, isLeader
	}
	ballot = op.l.Bno1
	if op.me == op.l.Bno2 {
		isLeader = true
		//op.state.role = "LEADER"
		//op.state.phase = "PREPARE"
	}

	fmt.Println("Proposal called for server ", op.me, "which is ", op.state, "isLeader = ", isLeader)

	if op.state.role == "LEADER" && op.state.phase == "PREPARE" {
		index = len(op.log) + len(op.buffer)
		op.buffer = append(op.buffer, command)
	} else if op.state.role == "LEADER" && op.state.phase == "ACCEPT" {
		op.log = append(op.log, command)
		op.persist()
		//fmt.Println("Current log in Proposal is ", op.log)
		op.accepted[op.me] = len(op.log)
		dummyresponse := new(Dummy)
		acceptstruct := new(AcceptStruct)
		acceptstruct.ServerSent = op.me
		acceptstruct.N = op.currentRnd
		acceptstruct.C = command

		index = len(op.log) - 1

		// Send promises to all promised followers
		for i := 0; i<len(op.promises); i++ {
			idx := op.promises[i].F 
			if idx!= op.me { //Not to itself!
				go op.peers[idx].Call("OmniPaxos.AcceptRPC", acceptstruct, dummyresponse)
			}
		}
	}
	time.Sleep(20 * time.Millisecond)
	return index, ballot, isLeader
}

// s is elected server, n is round s got elected in
func (op *OmniPaxos) Leader(s Ballot, n BallotRnd) {

	if s.Bno2 == op.me && greaterthan(n, op.promisedRnd) {
		fmt.Println("Set as leader - server", op.me)
		// reset all volatile state of leader
		op.state.role = "LEADER"
		op.state.phase = "PREPARE"
		//op.r = n
		op.promisedRnd = n 
		op.persist()
		op.currentRnd = n
		op.maxProm = Promise{AcceptedRnd:BallotRnd{-1,op.me}} //DOUBTLEX
		op.promises = make([]Promise, 0)
		op.buffer = make([]interface{},0)
		op.accepted = make([]int, 0)
		for i:=0; i<len(op.peers); i++ {
			op.accepted = append(op.accepted, 0)
		}
		dummyresponse := new(Dummy)
		temppromise := Promise{}
		temppromise = Promise{op.acceptedRnd, len(op.log), op.me, op.decidedIdx, op.suffix(op.decidedIdx)}
		op.promises = append(op.promises, temppromise)
		preparestruct := new(PrepareStruct)
		preparestruct.ServerSent = op.me
		preparestruct.N = op.currentRnd
		preparestruct.AccRnd = op.acceptedRnd
		preparestruct.LogIdx = len(op.log)
		preparestruct.DecIdx = op.decidedIdx
		//to all peers except itself!
		for i := 0; i < len(op.peers); i++ {
			if i != op.me {
				go op.peers[i].Call("OmniPaxos.PrepareRPC", preparestruct, dummyresponse)
			}
		}

	} 
}

// The service using OmniPaxos (e.g. a k/v server) wants to start
// agreement on the next command to be appended to OmniPaxos's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the OmniPaxos log, since the leader
// may fail or lose an election. Even if the OmniPaxos instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// ballot. The third return value is true if this server believes it is
// the leader.

// The tester doesn't halt goroutines created by OmniPaxos after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (op *OmniPaxos) Kill() {
	atomic.StoreInt32(&op.dead, 1)
	// Your code here, if desired.
	// you may set a variable to false to
	// disable logs as soon as a server is killed
	atomic.StoreInt32(&op.enableLogging, 0)
}

func (op *OmniPaxos) killed() bool { //can be used to check if not killed
	z := atomic.LoadInt32(&op.dead)
	return z == 1
}

func (op *OmniPaxos) checkLeader() {
	if !op.killed() {
		if op.l.Bno1 == op.me && op.state. 
		follow recover op.l my state follower, reset op.l 
		


		candidates := []Ballot{}
		for i := 0; i < len(op.ballots); i++ {
			if op.ballots[i].Qc == true {
				candidates = append(candidates, op.ballots[i])
			}
		}
		maxCandidate := Ballot{-1, -1, true}

		if len(candidates) == 0 {
			//fmt.Println("length of candidates is 0")
			maxCandidate.Bno1 = -2 //-1
		} else {
			maxCandidate = candidates[0]
		}
		// check candidates. no1 highest, if there is a tie, then do no2 higher
		for j := 1; j < len(candidates); j++ {
			if maxCandidate.Bno1 < candidates[j].Bno1 {
				maxCandidate = candidates[j]
			} else if maxCandidate.Bno1 == candidates[j].Bno1 && maxCandidate.Bno2 < candidates[j].Bno2 {
				maxCandidate = candidates[j]
			}
		}

		//op.b.Qc = true
		//op.mu.Unlock()
		fmt.Println("MaxCandidate is ", maxCandidate, " for server ", op.me)
		if maxCandidate.Bno1 < op.l.Bno1 || (maxCandidate.Bno1 == op.l.Bno1 && maxCandidate.Bno2 < op.l.Bno2) { //max < l
			//op.mu.Lock()
			fmt.Println("NOT Trying to set as leader, ballot is", op.b, "ballots received is ", candidates, "maxCandidate is ", maxCandidate, "op.l is",op.l, "- server", op.me)

			op.b.Bno1 = op.l.Bno1 + 1
			op.b.Qc = true
			fmt.Println("Not setting as leader - server", op.me)
			//op.mu.Unlock()
		} else if maxCandidate.Bno1 > op.l.Bno1 || (maxCandidate.Bno1 == op.l.Bno1 && maxCandidate.Bno2 > op.l.Bno2) { //max > l
			//op.mu.Lock()
			fmt.Println("Trying to set as leader, ballot is ", op.b, "ballots received is ", candidates, "maxcandidate is", maxCandidate,"op.l is",op.l,  "- server", op.me)
			op.l.Bno1 = maxCandidate.Bno1
			op.l.Bno2 = maxCandidate.Bno2
			op.persist() 
			//op.b.Qc = true
			//op.mu.Unlock()
			// Leader function
			// TODO 2B: reset volatile state of leader

			op.Leader(op.l, BallotRnd{op.r,op.me})
		} else {
			fmt.Println("Nothing!, ballot is ", op.b, "- server", op.me)
		}

	}

}

// save OmniPaxos's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 3 &4 for a description of what should be persistent.
func (op *OmniPaxos) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(op.xxx)
	// e.Encode(op.yyy)
	// data := w.Bytes()
	// op.persister.SaveOmnipaxosState(data)

	// Persist: op.log, op.promisedRnd, op.acceptedRnd, op.decidedIdx, op.l (ballot)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(op.log)
	e.Encode(op.promisedRnd)
	e.Encode(op.acceptedRnd)
	e.Encode(op.decidedIdx)
	e.Encode(op.l)
	data := w.Bytes()
	op.persister.SaveOmnipaxosState(data)
}

// restore previously persisted state.
func (op *OmniPaxos) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	log := make([]interface{},0)
	promisedRnd := BallotRnd{} // double
	acceptedRnd := BallotRnd{}  // double
	decidedIdx := 0 
	l := Ballot{} // ballot number of current leader
	if d.Decode(&log) != nil ||
		d.Decode(&promisedRnd) != nil || 
		d.Decode(&acceptedRnd) != nil ||
		d.Decode(&decidedIdx) != nil ||
		d.Decode(&l) != nil {
		fmt.Println("Error reading persisted state in server", op.me)
	} else {
		op.log = log
		op.promisedRnd = promisedRnd
		op.acceptedRnd = acceptedRnd
		op.decidedIdx = decidedIdx
		op.l = l 
		op.l.Qc = false //since we don't want qc state to have been persisted, but my implementation is weird so 
		
	}
}

// The service or tester wants to create a OmniPaxos server. The ports
// of all the OmniPaxos servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects OmniPaxos to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *OmniPaxos {

	op := &OmniPaxos{}
	op.mu.Lock()
	op.applyCh = applyCh 
	op.peers = peers
	op.persister = persister
	op.me = me
	op.dead = 0
	op.promisedRnd = BallotRnd{-1,op.me}
	op.acceptedRnd = BallotRnd{-1,op.me}
	op.currentRnd = BallotRnd{0,op.me}
	op.decidedIdx = -1
	op.state = State{"FOLLOWER", "PREPARE"}
	dummyresponse := new(Dummy)
	op.enableLogging = 1
	op.accepted = make([]int, len(op.peers))
	op.promises = make([]Promise, 0)
	op.maxProm = Promise{AcceptedRnd: BallotRnd{0,op.me}}
	op.buffer = make([]interface{}, 0)
	op.log = make([]interface{}, 0)
	op.accepted = make([]int, 0)
	for i:=0; i<len(op.peers); i++ {
		op.accepted = append(op.accepted, 0) //an int array init to 0
	}

	op.l = Ballot{-1, -1, false}
	op.r = 0
	initballot := Ballot{0, op.me, true}
	op.b = initballot
	op.delay = 100 * time.Millisecond
	op.ballots = []Ballot{}

	bytearg := op.persister.ReadOmnipaxosState()
	op.readPersist(bytearg)
	op.persist() // initial persist, if needed? 
	// Redo any command commits
	if op.decidedIdx != -1 {
		for i := 0; i < op.decidedIdx; i++ {
			applymsg := *new(ApplyMsg)
			applymsg.CommandValid = true 
			applymsg.Command = op.log[i] 
			applymsg.CommandIndex = i
			op.applyCh <- applymsg
			time.Sleep(10 * time.Millisecond)
		}
	}
	fmt.Println("Server ", op.me, "is LIVE with op.log = ", op.log, "decidedIdx = ", op.decidedIdx, "ballotNum b", op.b, "op.l is", op.l);


	op.linkdrop = false
	op.deadrounds = 0
	op.mu.Unlock()

	/*
		mu            sync.Mutex          // Lock to protect shared access to this peer's state
		peers         []*labrpc.ClientEnd // RPC end points of all peers
		persister     *Persister          // Object to hold this peer's persisted state
		me            int                 // This peer's index into peers[]
		dead          int32               // Set by Kill()
		enableLogging int32
		// Your code here (2A, 2B).
		promisedRnd	  int
		acceptedRnd   int
		decidedIdx	  int
		state		  State  //role "follower or leader", phase "prepare or accept"
		l			  int // ballot number of current leader
		r 			  int // current heartbeat round, init 0
		b			  Ballot // init (0, pid)
		delay 		  time.Duration
		ballots		  []Ballot
		promisedRnd	  int
		acceptedRnd   int
		decidedIdx	  int
		currentRnd	  int
		promises	  []Promise
		maxProm 	  Promise
		log			  []interface{}
		accepted      []int
		buffer 		  []interface{}*/

	// Send go function HBRequests to all other servers
	// This also handles HBReplies and populating stuff

	go func(op *OmniPaxos) {
		for !op.killed() {
			// Go function for HBRequests
			go func() {
				for i := 0; i < len(peers); i++ {
					// Sending to all servers other than your own
					if i != op.me {
						op.mu.Lock()
						hbrequest := new(HBRequest)
						hbrequest.ServerSent = op.me
						hbrequest.Rnd = op.r
						//fmt.Println("Sending HBRequest from server ",i, " to ", op.me)
						op.mu.Unlock()
						go op.peers[i].Call("OmniPaxos.HBRequestRPC", hbrequest, dummyresponse) // We are sending it from server 'op.me' to server 'i'
						//fmt.Println("Received request back to ",i, " from ", op.me)
					}
					time.Sleep(op.delay / 5)
				}
			}()
			/*
			// Go function for 
			for i := 0; i < len(peers); i++ {
				// Sending to all servers other than your own
				if i != op.me {
					op.mu.Lock()
					hbrequest := new(HBRequest)
					hbrequest.ServerSent = op.me
					hbrequest.Rnd = op.r
					//fmt.Println("Sending HBRequest from server ",i, " to ", op.me)
					op.mu.Unlock()
					if (!op.killed()) {
						go op.peers[i].Call("OmniPaxos.HBRequestRPC", hbrequest, dummyresponse) // We are sending it from server 'op.me' to server 'i'
					}
					//fmt.Println("Received request back to ",i, " from ", op.me)
				}
				time.Sleep(100 * time.Millisecond)
			} */
			
			// Wait for op.delay
			time.Sleep(op.delay)
			//fmt.Println("Heartbeats done, timer expire")
			op.mu.Lock()

			// Check for recovery stuff
			if len(op.ballots) == 0 {
				op.deadrounds++
				fmt.Println("Server ", op.me, "deadrounds = ", op.deadrounds, "in HBround", op.r)
			}
			if(op.deadrounds >= 3) {
				fmt.Println("Server ", op.me, "is dead")
				op.linkdrop = true
				op.b.Qc = false
			}

			if(op.linkdrop==true && len(op.ballots)>0) { //server has come back from the dead
				go func(op *OmniPaxos) {
					op.mu.Lock()
					defer op.mu.Unlock()
					fmt.Println("Server ", op.me, "has come back from the dead")

					op.linkdrop = false 
					op.deadrounds = 0

					//bytearg := op.persister.ReadOmnipaxosState()
					//op.readPersist(bytearg)
					// 

					op.state.role = "FOLLOWER"
					op.state.phase = "RECOVER"
					// Send prepareReq to all reachable servers, in hopes you find the leader
					for i:=0; i<len(op.peers); i++ {
						if i!=op.me {
							preparereqstruct := new(PrepareReqStruct)
							preparereqstruct.ServerSent = op.me
							go op.peers[i].Call("OmniPaxos.PrepareReqRPC", preparereqstruct, dummyresponse)
						}
					}
				}(op)
			}

			// Upon timeout of startTimer
			tempballot := *new(Ballot)
			tempballot.Bno1 = op.b.Bno1
			tempballot.Bno2 = op.me 
			tempballot.Qc = op.b.Qc
			op.ballots = append(op.ballots, tempballot)
			//fmt.Println("Waiting to check majority - server ",op.me, "op.ballots and op.peers = ", len(op.ballots), len(op.peers))

			if len(op.ballots) > len(op.peers)/2 { // majority
				fmt.Println("Server" ,op.me, "Is a majority, going into checkLeader ",op.me, "current elected leader id is", op.l, "op.ballots and op.peers = ",len(op.ballots), len(op.peers))
				op.checkLeader()
			} else {
				fmt.Println("Server", op.me, "Setting QC as false, with op.ballots = ", op.ballots, "len op.ballots and (op.peers/2) = ", len(op.ballots), len(op.peers)/2, " - Server ", op.me)
				op.b.Qc = false
			}
			op.ballots = make([]Ballot, 0)
			//fmt.Println("Clearing ballots, op.ballots = ", op.ballots, " - Server ",op.me)
			op.r = op.r + 1
			//fmt.Println("===================== ROUND ", op.r)
			op.mu.Unlock()
		}
	}(op)

	// Your initialization code here (2A, 2B).
	// This is to create one specific omnipaxos server
	//
	// Make has to kick off leader election periodically by sending out HeartBeat RPCs when it hasn't heard from another peer for a while
	// ticker() goroutine should be used for periodic stuff

	return op
}


