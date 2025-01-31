package omnipaxos

//
// Support for Omnipaxos and kvomnipaxos to save persistent
// Omnipaxos state (log &c) and k/v server snapshots.
//
// We will use the original persister.go to test your code for grading.
// So, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import "sync"

type Persister struct {
	mu             sync.Mutex
	omnipaxosstate []byte
	snapshot       []byte
}

func MakePersister() *Persister {
	return &Persister{}
}

func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.omnipaxosstate = ps.omnipaxosstate
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) SaveOmnipaxosState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.omnipaxosstate = clone(state)
}

func (ps *Persister) ReadOmnipaxosState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.omnipaxosstate)
}

func (ps *Persister) OmnipaxosStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.omnipaxosstate)
}

// Save both Omnipaxos state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.omnipaxosstate = clone(state)
	ps.snapshot = clone(snapshot)
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}
