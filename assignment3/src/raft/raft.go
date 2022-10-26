package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "a3/labrpc"

// import "bytes"
// import "encoding/gob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	/* enum [follower, candidate, leader] */
	peerRole PeerRole

	/* latest term server has seen (initialized to 0 on first boot, increases monotonically) */
	currentTerm int

	/* K,V -> term, candidateId */
	votedForMap sync.Map

	/* K,V -> term, (K,V) -> peerId, voteInFavour */
	receivedVotesCounter sync.Map

	/* actual log */
	log []LogData

	/* send true on this channel to reset the election timeout
	To be used only when this peer is not a leader */
	resetElectionTimeoutChan chan bool

	/* send true on this channel to interrupt the heartbeat this leader will send in case there are no log replication requests
	To be used only when this peer is a leader */
	resetHeartBeatTimeoutChan chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here.

	term = rf.currentTerm
	isleader = rf.peerRole.role == LeaderRole().role

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	// TODO : Note that if using gob.encode, encode zero will get the previous value. It’s a feature not a bug.
	//w := new(bytes.Buffer)
	//e := gob.NewEncoder(w)
	//e.Encode(rf.currentTerm)
	//e.Encode(rf.votedForMap)
	//e.Encode(rf.log)
	//data := w.Bytes()
	//rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)

	// TODO : Note that if using gob.encode, encode zero will get the previous value. It’s a feature not a bug.
	//r := bytes.NewBuffer(data)
	//d := gob.NewDecoder(r)
	//d.Decode(&rf.currentTerm)
	//d.Decode(&rf.votedForMap)
	//d.Decode(&rf.log)
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here.
	RequestingPeerId   int
	RequestingPeerTerm int
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here.
	VotedInFavour      bool
	RespondingPeerTerm int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

	var voteDecision bool
	if args.RequestingPeerTerm < rf.currentTerm {
		// requesting peer is not on the latest term && desires to be leader
		// dont vote for it
		fmt.Printf("RequestVote early exit %d %d \n", args.RequestingPeerTerm, rf.currentTerm)
		voteDecision = false

	} else {
		votedFor, _ := rf.votedForMap.LoadOrStore(args.RequestingPeerTerm, args.RequestingPeerId)
		voteDecision = votedFor == args.RequestingPeerId
		//if !voteDecision {
		//	fmt.Printf("RequestVote %d %d\n", votedFor, args.RequestingPeerId)
		//}
	}

	reply.VotedInFavour = voteDecision
	reply.RespondingPeerTerm = rf.currentTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	return ok
}

// Start
// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	term, isLeader = rf.GetState()

	if isLeader {
		// TODO: write to log, maintaining the state of each item (commit or tentative)
	}

	return index, term, isLeader
}

// Kill
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// Make
// The service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd,
	me int,
	persister *Persister,
	applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.receivedVotesCounter = sync.Map{}
	rf.votedForMap = sync.Map{}
	rf.currentTerm = 0
	rf.log = []LogData{}
	rf.resetElectionTimeoutChan = make(chan bool)
	rf.resetHeartBeatTimeoutChan = make(chan bool)

	// just started peer, should be a follower
	rf.peerRole = FollowerRole()

	// initialize from state persisted before a crash
	//rf.readPersist(persister.ReadRaftState())

	rf.startElectionTimeoutBackgroundProcess()

	return rf
}

func (rf *Raft) startElectionTimeoutBackgroundProcess() {
	rand.Seed(time.Now().UnixNano())
	timeoutDuration := time.Duration(randInt(150, 300))

	go func() {
		// this background loop should run as long as peer is not leader
		for rf.peerRole.role != LeaderRole().role {

			timeoutChan := make(chan bool)
			go func(timeoutChan chan bool, timeoutDuration time.Duration) {
				time.Sleep(timeoutDuration)
				timeoutChan <- true
			}(timeoutChan, timeoutDuration)

			select {
			case <-timeoutChan:
				rf.tryTakingLeaderRole()
				break
			case <-rf.resetElectionTimeoutChan:
				break
			}
		}

	}()
}

func (rf *Raft) tryTakingLeaderRole() {

	// 1. switch to candidate role if not in that role already
	if rf.peerRole.role != CandidateRole().role {
		rf.peerRole = CandidateRole()
	}

	// 2. increment the term
	rf.currentTerm++

	// 3. request votes from peers
	for index, peer := range rf.peers {

		if index == rf.me {
			// mark self vote and ignore requesting vote from self
			receivedVotesForTerm_, _ := rf.receivedVotesCounter.LoadOrStore(rf.currentTerm, sync.Map{})
			receivedVotesForTerm := receivedVotesForTerm_.(sync.Map)
			receivedVotesForTerm.Store(index, true)
			rf.receivedVotesCounter.Store(rf.currentTerm, receivedVotesForTerm)
			continue
		}

		requestVoteArgs := RequestVoteArgs{rf.me, rf.currentTerm}
		requestVoteReply := RequestVoteReply{}
		peer := peer
		index := index
		go func() {
			ok := peer.Call("Raft.RequestVote", requestVoteArgs, &requestVoteReply)
			if ok {
				// got vote reply
				fmt.Println("tryTakingLeaderRole 2")
				receivedVotesCounter_, _ := rf.receivedVotesCounter.LoadOrStore(requestVoteReply.RespondingPeerTerm, sync.Map{})
				receivedVotesCounter := receivedVotesCounter_.(sync.Map)
				receivedVotesCounter.Store(index, requestVoteReply.VotedInFavour)

				fmt.Printf("tryTakingLeaderRole 2 done %v\n", requestVoteReply.VotedInFavour)
				if requestVoteReply.VotedInFavour {
					rf.transitionToLeaderIfSufficientVotes(requestVoteReply.RespondingPeerTerm)
				}
			}
		}()
	}
}

func (rf *Raft) transitionToLeaderIfSufficientVotes(respondingPeerTerm int) {
	if rf.peerRole.role == LeaderRole().role {
		// already a leader
		return
	}

	if rf.currentTerm > respondingPeerTerm {
		// ignore this vote as it was of previous term
		return
	}

	requiredVotes := len(rf.peers) / 2 // TODO: check threshold
	receivedVotes := 0

	receivedVotesCounter_, _ := rf.receivedVotesCounter.LoadOrStore(respondingPeerTerm, sync.Map{})
	receivedVotesCounter := receivedVotesCounter_.(sync.Map)
	receivedVotesCounter.Range(func(key, ifVotedInFavour any) bool {
		if ifVotedInFavour.(bool) {
			receivedVotes++
		}
		return true
	})

	if receivedVotes < requiredVotes {
		// insufficient votes
		return
	}

	// Make current peer leader
	rf.peerRole = LeaderRole()
	rf.startPeriodicBroadcastBackgroundProcess()
}

func (rf *Raft) startPeriodicBroadcastBackgroundProcess() {

	// TODO: This duration should be less than election timeout duration
	heartBeatDuration := time.Duration(100)

	// should run as long as peer is leader
	for rf.peerRole.role == LeaderRole().role {
		heartBeatChan := make(chan bool)

		go func(heartBeatChan chan bool, duration time.Duration) {
			time.Sleep(duration)
			heartBeatChan <- true
		}(heartBeatChan, heartBeatDuration)

		entryRequestArgs := EntryRequestArgs{rf.me, rf.currentTerm, []LogData{}}
		entryRequestReply := EntryRequestReply{}
		select {
		case <-heartBeatChan:

			for _, peer := range rf.peers {
				peer := peer
				go func() {
					ok := peer.Call("Raft.AppendEntriesRPC", entryRequestArgs, &entryRequestReply)
					if ok {
						// got reply
					}
				}()
			}

			break
		case <-rf.resetHeartBeatTimeoutChan:
			// called when there is an actual data in log thats to be replicated.
			// otherwise it sends just heartbeats to the followers
			break
		}
	}
}

func (rf *Raft) AppendEntriesRPC(args EntryRequestArgs, reply *EntryRequestReply) {
	// HeartBeat logic
	go func() {
		rf.resetElectionTimeoutChan <- true
	}()

	reply.Term = rf.currentTerm
	reply.Success = true // TODO: need to change for A4
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

/* STRUCTS AND ENUMS */

// PeerRole Enum
type PeerRole struct {
	role int
}

func FollowerRole() PeerRole  { return PeerRole{0} }
func CandidateRole() PeerRole { return PeerRole{1} }
func LeaderRole() PeerRole    { return PeerRole{2} }

// LogData TODO check if predefined struct needs to be used
type LogData struct {
	command interface{} // command for the state machine
	term    int         // term when the entry was received by the leader
}

type EntryRequestArgs struct {
	LeaderId   int
	LeaderTerm int
	LogEntries []LogData
}

type EntryRequestReply struct {
	Term    int
	Success bool
}
