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
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

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

// This duration should be less than election timeout duration
const heartBeatDuration = 100 * time.Millisecond

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

	/* send true on this channel to reset the election timeout
	To be used only when this peer is not a leader */
	resetElectionTimeoutChan chan bool

	/* send index of the log on this channel to interrupt the heartbeat (this leader will send) in case there are no log replication requests
	To be used only when this peer is a leader */
	//relayToPeerChan chan int

	//---------------------- fields used for Log Replication by Peers
	/* actual log */
	log *[]LogItem

	/* index where leader / peer will receive next entry from client / leader respectively */
	nextIndex int

	/* index till which entries are committed */
	commitIndex int

	//---------------------- fields used for Log Replication by Leaders

	/* for tentative & commit conformation */
	confirmationCountsMap map[int](map[int]ConfirmationStatus)

	/* match Indexes of peers. [Only applicable when this peer is leader]*/
	matchIndexesOf map[int]int
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

func (rf *Raft) UpdateState(newTerm int, newRole PeerRole) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	rf.currentTerm = newTerm
	rf.peerRole = newRole
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

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	votedFor, ok := rf.votedForMap.Load(rf.currentTerm)
	if !ok {
		votedFor = -1
	}
	e.Encode(votedFor)
	e.Encode(*rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)

	rf.votedForMap = sync.Map{}
	var votedFor int
	d.Decode(&votedFor)
	if votedFor != -1 {
		rf.votedForMap.Store(rf.currentTerm, votedFor)
	}

	var log []LogItem
	d.Decode(&log)
	*rf.log = log
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here.
	RequestingPeerId       int
	RequestingPeerTerm     int
	LogsCountInCurrentTerm int
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
	//fmt.Printf("[Requesting Vote by %v for term %v from server %v] \n", args.RequestingPeerId, args.RequestingPeerTerm, rf.me)

	reply.RespondingPeerTerm = args.RequestingPeerTerm

	// If already voted for someone else for this term
	votedFor, _ := rf.votedForMap.Load(args.RequestingPeerTerm)
	if votedFor != nil {
		reply.VotedInFavour = votedFor == args.RequestingPeerId

	} else {

		currentTerm, _ := rf.GetState()

		if args.RequestingPeerTerm < currentTerm {
			// requesting peer is not on the latest term && desires to be leader -> reject
			reply.VotedInFavour = false

		} else if args.LogsCountInCurrentTerm < getNumOfCommittedLogsForTerm(rf.currentTerm, rf.log) {
			// for the latest term, peer has more log entries than candidate -> reject
			reply.VotedInFavour = false

		}
	}

	if reply.VotedInFavour {
		rf.UpdateState(args.RequestingPeerTerm, FollowerRole())
		rf.resetElectionTimeoutChan <- true
	}

	//fmt.Printf("[RequestVote by %v for term %v] [voted %v by %v ] \n", args.RequestingPeerId, args.RequestingPeerTerm, voteDecision, rf.me)
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

func (rf *Raft) sendAppendEntries(peerId int, currentTerm int) bool {

	entryRequestArgs := EntryRequestArgs{
		rf.me,
		currentTerm,
		rf.getPayloadForPeer(peerId, currentTerm),
	}
	entryRequestReply := EntryRequestReply{}

	ok := rf.peers[peerId].Call("Raft.AppendEntriesOrHeartbeatRPC", entryRequestArgs, &entryRequestReply)
	if ok {
		fmt.Printf("[Leader %v][term %v] AppendEntriesRPC Response from %v [errorCode:%v updatedMatchIndex:%v] \n",
			rf.me, currentTerm, peerId, entryRequestReply.ErrorCode, entryRequestReply.UpdatedMatchIndex)

		switch entryRequestReply.ErrorCode {
		case _200_OK():
			rf.matchIndexesOf[peerId] = entryRequestReply.UpdatedMatchIndex
			break

		case _401_OlderTerm():
			rf.transitionBackToFollower(rf.currentTerm)
			break

		case _402_MoreNumOfCommittedLogsOfCurrentTerm():
			rf.transitionBackToFollower(rf.currentTerm)
			break

		case _403_UnequalLogsAtMatchIndex():
			rf.matchIndexesOf[peerId] = -1
			go rf.sendAppendEntries(peerId, currentTerm)
			break
		}
	}
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
	nextIndex := -1
	term, isLeader := rf.GetState()

	if isLeader {
		// TODO: write to log

		// 1. Init the logItem and metadata
		logItem := LogItem{
			Command:     command,
			Term:        term,
			IsCommitted: false,
		}
		confirmationMap := rf.generateInitialConfirmationStatusMap()
		confirmationMap[rf.me] = Accepted()

		//go func() {

		// 2. Write to own log [use Critical Section]
		*rf.log = append(*rf.log, logItem)
		nextIndex = rf.nextIndex
		rf.confirmationCountsMap[nextIndex] = confirmationMap
		rf.nextIndex++
	}

	return nextIndex, term, isLeader
}

// Kill
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.h
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
	//rand.Seed(time.Now().UnixNano())
	rf.receivedVotesCounter = sync.Map{}
	rf.votedForMap = sync.Map{}
	rf.log = &[]LogItem{}

	// Making this channels buffered as a value might be present on this chan when this peer goes down
	// and new value wont get added to chan when this peer comes up again
	rf.resetElectionTimeoutChan = make(chan bool, 20)
	//rf.relayToPeerChan = make(chan int, 20)

	// just started peer, should be a follower with term as 0
	rf.UpdateState(0, FollowerRole())

	// initialize from state persisted before a crash
	//rf.readPersist(persister.ReadRaftState())

	rf.startElectionTimeoutBackgroundProcess()

	return rf
}

func (rf *Raft) startElectionTimeoutBackgroundProcess() {

	go func() {
		// this background loop should run as long as peer is not leader

		for rf.peerRole.role != LeaderRole().role {

			random := randInt(150, 300)
			timeoutDuration := time.Duration(random) * time.Millisecond

			timeoutChan := make(chan bool)
			go func(timeoutChan chan bool, timeoutDuration time.Duration) {
				time.Sleep(timeoutDuration)
				timeoutChan <- true
			}(timeoutChan, timeoutDuration)

			select {
			case <-timeoutChan:
				if rf.peerRole.role != LeaderRole().role {
					rf.tryTakingLeaderRole()
				}
				break
			case <-rf.resetElectionTimeoutChan:
				//fmt.Printf("resetElectionTimeoutChan  %v\n", rf.me)
				break
			}
		}

	}()
}

func (rf *Raft) tryTakingLeaderRole() {

	// 1. switch to candidate role if not in that role already
	// 2. increment the term
	currentTerm, _ := rf.GetState()
	currentTerm++
	rf.UpdateState(currentTerm, CandidateRole())

	rf.mu.Lock()
	log := rf.log
	rf.mu.Unlock()

	logsCount := getNumOfCommittedLogsForTerm(currentTerm, log)

	// 3. request votes from peers
	//fmt.Printf("tryTakingLeaderRole %v for term %v\n", rf.me, currentTerm)
	for index, _ := range rf.peers {

		if index == rf.me {
			// mark self vote and ignore requesting vote from self
			receivedVotesForTerm_, _ := rf.receivedVotesCounter.LoadOrStore(currentTerm, sync.Map{})
			receivedVotesForTerm := receivedVotesForTerm_.(sync.Map)
			receivedVotesForTerm.Store(rf.me, true)
			rf.receivedVotesCounter.Store(currentTerm, receivedVotesForTerm)
			continue
		}

		requestVoteArgs := RequestVoteArgs{rf.me, currentTerm, logsCount}
		requestVoteReply := RequestVoteReply{}
		//peer := peer
		index := index
		go func() {
			ok := rf.sendRequestVote(index, requestVoteArgs, &requestVoteReply)
			if ok {
				// got vote reply
				//fmt.Printf("tryTakingLeaderRole %v -> got %v from %v\n", rf.me, requestVoteReply.VotedInFavour, index)
				receivedVotesCounter_, _ := rf.receivedVotesCounter.LoadOrStore(requestVoteReply.RespondingPeerTerm, sync.Map{})
				receivedVotesCounter := receivedVotesCounter_.(sync.Map)
				receivedVotesCounter.Store(index, requestVoteReply.VotedInFavour)

				if requestVoteReply.VotedInFavour {
					rf.transitionToLeaderIfSufficientVotes(requestVoteReply)

				} else if requestVoteReply.RespondingPeerTerm > currentTerm {
					// transition back to follower
					rf.transitionBackToFollower(rf.currentTerm)
				}
			}
		}()
	}
}

func (rf *Raft) transitionToLeaderIfSufficientVotes(requestVoteReply RequestVoteReply) {
	respondingPeerTerm := requestVoteReply.RespondingPeerTerm

	if rf.peerRole.role == LeaderRole().role {
		// already a leader
		//fmt.Printf("tryTakingLeaderRole %v for term %v -> already a leader\n", rf.me, rf.currentTerm)
		return
	}

	if rf.currentTerm > respondingPeerTerm {
		// ignore this vote as it was of previous term
		//fmt.Printf("tryTakingLeaderRole %v for term %v -> ignore this vote as it was of previous term %v\n", rf.me, rf.currentTerm, respondingPeerTerm)
		return
	}

	requiredVotes := (len(rf.peers) - 1) / 2
	receivedVotes := 0

	receivedVotesCounter_, _ := rf.receivedVotesCounter.LoadOrStore(respondingPeerTerm, sync.Map{})
	receivedVotesCounter := receivedVotesCounter_.(sync.Map)
	receivedVotesCounter.Range(func(key, ifVotedInFavour any) bool {
		if ifVotedInFavour.(bool) {
			receivedVotes++
		}
		return true
	})

	if receivedVotes < requiredVotes && receivedVotes > 1 {
		// insufficient votes
		//fmt.Printf("insufficient votes %v\n", rf.me)
		return
	}
	//fmt.Printf("TRANSITION To Leader %v for term %v\n", rf.me, rf.currentTerm)
	// Make current peer leader
	rf.peerRole = LeaderRole()
	rf.startPeriodicBroadcastBackgroundProcess()
}

// When Peer is leader
func (rf *Raft) startPeriodicBroadcastBackgroundProcess() {

	// should run as long as peer is leader
	for {

		// if not a leader, exit this background process
		currentTerm, isLeader := rf.GetState()
		if !isLeader {
			return
		}

		heartBeatChan := make(chan bool)

		go func(heartBeatChan chan bool, duration time.Duration) {
			time.Sleep(duration)
			heartBeatChan <- true
		}(heartBeatChan, heartBeatDuration)

		<-heartBeatChan
		rf.relayToAllPeers(currentTerm)

	}
}

func (rf *Raft) relayToAllPeers(currentTerm int) {

	if rf.peerRole.role != LeaderRole().role {
		return
	}

	for peerIndex, _ := range rf.peers {
		// ignore sending msg to self
		if peerIndex == rf.me {
			continue
		}

		go func(index int, currentTerm int) {
			//fmt.Printf("Leader heartBeat by %v to %v \n", rf.me, peerIndex)
			rf.sendAppendEntries(index, currentTerm)
		}(peerIndex, currentTerm)

	}
}

func (rf *Raft) getPayloadForPeer(peer int, currentTerm int) *EntryRequestPayload {
	payload := EntryRequestPayload{}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Get the uncommitted Logs to send to this peer
	matchIndex := rf.matchIndexesOf[peer]
	*payload.Logs = (*rf.log)[matchIndex:rf.nextIndex]
	payload.MatchIndex = matchIndex
	payload.NumOfCommittedLogsOfCurrentTerm = getNumOfCommittedLogsForTerm(currentTerm, rf.log)

	return &payload
}

// AppendEntriesOrHeartbeatRPC : PEER receiving the appendEntries or Heartbeat call
func (rf *Raft) AppendEntriesOrHeartbeatRPC(args EntryRequestArgs, reply *EntryRequestReply) {
	currentTerm, _ := rf.GetState()
	fmt.Printf("[peer %v][term %v] AppendEntriesRPC received from %v\n", rf.me, currentTerm, args.LeaderId)

	rf.mu.Lock()
	updatedMatchIndex, errorCode := reconcileLogs(args, rf.log, currentTerm, rf.me)
	rf.mu.Unlock()

	fmt.Printf("[peer %v][term %v] reconcileLogs with %v\n -> updatedMatchIndex %v, errorCode %v",
		rf.me, currentTerm, args.LeaderId, updatedMatchIndex, errorCode.Code)
	if errorCode == _200_OK() {
		// HeartBeat logic
		//fmt.Printf("[Leader %v] heartBeat received by %v\n", args.LeaderId, rf.me)
		if rf.peerRole != FollowerRole() {
			rf.peerRole = FollowerRole()
		}
		rf.resetElectionTimeoutChan <- true
	}

	reply.Term = currentTerm
	reply.UpdatedMatchIndex = updatedMatchIndex
	reply.ErrorCode = errorCode
}

func reconcileLogs(
	args EntryRequestArgs,
	peerLog *[]LogItem,
	peerTerm int,
	peerId int) (int, ErrorCode) {

	leaderLog := args.EntryRequestPayload.Logs
	leaderTerm := args.LeaderTerm
	leaderLogsCountOfCurrentTerm := args.EntryRequestPayload.NumOfCommittedLogsOfCurrentTerm
	matchIndex := args.EntryRequestPayload.MatchIndex
	updatedMatchIndex := matchIndex

	if peerTerm > leaderTerm {
		// reject heartbeat/appendEntries
		return -1, _401_OlderTerm()
	}

	leaderLogLen := len(*leaderLog)
	peerLogLen := len(*peerLog)

	// If Payload exists
	if leaderLog != nil && leaderLogLen != 0 {
		if leaderLogsCountOfCurrentTerm < getNumOfCommittedLogsForTerm(peerTerm, peerLog) {
			// reject
			return -1, _402_MoreNumOfCommittedLogsOfCurrentTerm()
		}

		if matchIndex != -1 &&
			(matchIndex >= peerLogLen ||
				(0 <= matchIndex && matchIndex < len(*peerLog) && (*leaderLog)[matchIndex] != (*peerLog)[matchIndex])) {
			// matchIndex log is not matching with leader -> reject
			return -1, _403_UnequalLogsAtMatchIndex()
		}

		var startIndex int
		var endIndex int

		if matchIndex == -1 {
			// leader is unaware till where its logs match with this peer
			startIndex = 0
			endIndex = leaderLogLen

		} else {
			startIndex = matchIndex + 1
			endIndex = startIndex + leaderLogLen

		}

		for i := startIndex; i < endIndex; i++ {
			if i == len(*peerLog) {
				*peerLog = append(*peerLog, (*leaderLog)[i])
			} else if i < len(*peerLog) {
				(*peerLog)[i] = (*leaderLog)[i]
			} else {
				// this case shouldnt reach
				fmt.Printf("[peer %v][term %v] reconcileLogs unreachable condition reached\n", peerId, peerTerm)
			}

			if (*leaderLog)[i].IsCommitted {
				updatedMatchIndex = i
			}
		}

		peerLen := len(*peerLog)
		if peerLen > leaderLogLen {
			*peerLog = (*peerLog)[:leaderLogLen]
		}
	}
	return updatedMatchIndex, _200_OK()
}

func (rf *Raft) transitionBackToFollower(currentTerm int) {
	fmt.Printf("[peer %v][term %v] transitionBackToFollower\n", rf.me, currentTerm)
	rf.UpdateState(currentTerm, FollowerRole())
}

/* Helper functions */

func getNumOfCommittedLogsForTerm(currentTerm int, logs *[]LogItem) int {
	count := 0
	for i := len(*logs) - 1; i >= 0; i-- {
		if (*logs)[i].Term == currentTerm {
			if (*logs)[i].IsCommitted {
				count++
			}
		} else {
			break
		}
	}
	return count
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func areLogsEqual(log1 *[]LogItem, log2 *[]LogItem) bool {
	if len(*log1) != len(*log2) {
		return false
	}

	n := len(*log1)
	for i := 0; i < n; i++ {
		if (*log1)[i] != (*log2)[i] {
			//fmt.Printf("areLogsEqual : %v %v\n", (*log1)[i], (*log2)[i])
			return false
		}
	}
	return true
}

func getLeaderLogsToSend(leaderLog *[]LogItem, matchIndex int) *[]LogItem {
	leaderLogToSend := &[]LogItem{}
	if matchIndex < 0 {
		*leaderLogToSend = *leaderLog
	} else {
		*leaderLogToSend = (*leaderLog)[matchIndex:]
	}
	return leaderLogToSend
}

/* STRUCTS AND ENUMS */

// PeerRole Enum
type PeerRole struct {
	role int
}

func FollowerRole() PeerRole  { return PeerRole{0} }
func CandidateRole() PeerRole { return PeerRole{1} }
func LeaderRole() PeerRole    { return PeerRole{2} }

type LogItem struct {
	Command     interface{} // command for the state machine
	Term        int         // term when the entry was received by the leader
	IsCommitted bool
}

type EntryRequestArgs struct {
	LeaderId            int
	LeaderTerm          int
	EntryRequestPayload *EntryRequestPayload
}

type EntryRequestReply struct {
	Term              int
	ErrorCode         ErrorCode
	UpdatedMatchIndex int
}

type EntryRequestPayload struct {
	Logs                            *[]LogItem
	MatchIndex                      int
	NumOfCommittedLogsOfCurrentTerm int
}

// ReconciliationResult Enum
type ReconciliationResult struct {
	result int
}

func LeaderAccurate() ReconciliationResult { return ReconciliationResult{0} }
func PeerAccurate() ReconciliationResult   { return ReconciliationResult{1} }

// ConfirmationStatus Enum
type ConfirmationStatus struct {
	status int
}

func Accepted() ConfirmationStatus   { return ConfirmationStatus{0} }
func Rejected() ConfirmationStatus   { return ConfirmationStatus{1} }
func NoResponse() ConfirmationStatus { return ConfirmationStatus{2} }

func (rf *Raft) generateInitialConfirmationStatusMap() map[int]ConfirmationStatus {
	confirmationMap := map[int]ConfirmationStatus{}
	for index, _ := range rf.peers {
		confirmationMap[index] = NoResponse()
	}
	return confirmationMap
}

type ErrorCode struct {
	Code int
}

func _200_OK() ErrorCode                                  { return ErrorCode{200} }
func _401_OlderTerm() ErrorCode                           { return ErrorCode{401} }
func _402_MoreNumOfCommittedLogsOfCurrentTerm() ErrorCode { return ErrorCode{402} }
func _403_UnequalLogsAtMatchIndex() ErrorCode             { return ErrorCode{403} }
