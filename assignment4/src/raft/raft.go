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
	"log"
	"math/rand"
	"os"
	"strconv"
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

	/* K,V -> term, (K,V) -> peerId, *voteInFavour */
	receivedVotesCounter sync.Map

	/* send true on this channel to reset the election timeout
	To be used only when this peer is not a leader */
	resetElectionTimeoutChan chan bool

	applyCh chan ApplyMsg

	transitionLeaderMu sync.Mutex

	//---------------------- fields used for Log Replication by Peers
	/* actual log */
	log *[]LogItem

	/* index where leader / peer will receive next entry from client / leader respectively */
	nextIndex int

	/* index till which entries are committed */
	commitIndex int

	//---------------------- fields used for Log Replication by Leaders

	/* for tentative & commit conformation */
	confirmationStatusMap   map[int]ConfirmationStatus // K:logIndex , V:ConfirmationStatus
	confirmationStatusMapMU sync.Mutex

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
	rf.log = &log
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here.
	RequestingPeerId   int
	RequestingPeerTerm int
	LastLogTerm        int
	LastLogIndex       int
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

	rf.mu.Lock()
	_log := rf.log
	rf.mu.Unlock()

	reply.RespondingPeerTerm = rf.currentTerm

	// If already voted for someone else for this term
	votedFor, _ := rf.votedForMap.Load(args.RequestingPeerTerm)
	if votedFor != nil {
		reply.VotedInFavour = votedFor == args.RequestingPeerId

	} else {
		peerLastLogIndex, peerLastLogTerm := getLastLogIndexAndTerm(_log)

		if args.RequestingPeerTerm < rf.currentTerm {
			// requesting peer is not on the latest term && desires to be leader -> reject
			reply.VotedInFavour = false

		} else if args.LastLogTerm < peerLastLogTerm ||
			(args.LastLogTerm == peerLastLogTerm && args.LastLogIndex < peerLastLogIndex) {
			// for the latest term, peer has more log entries than candidate -> reject
			reply.VotedInFavour = false

		} else {
			reply.VotedInFavour = true
		}
	}

	if reply.VotedInFavour {
		rf.UpdateState(args.RequestingPeerTerm, FollowerRole())
		rf.resetElectionTimeoutChan <- true
		reply.RespondingPeerTerm = args.RequestingPeerTerm
		rf.persist()
	}
	debugLog(rf.me, fmt.Sprintf("[Peer %v][term %v] RequestVote voted %v to %v] args[%v] peerLog[%v]\n",
		rf.me, rf.currentTerm, reply.VotedInFavour, args.RequestingPeerId, args, _log))

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

	// 1. Get the uncommitted Logs to send to this peer
	rf.mu.Lock()

	matchIndex := rf.matchIndexesOf[peerId]
	nextIndex := rf.nextIndex
	//debugLog(rf.me, fmt.Sprintf("[Leader %v][term %v] Leader sendAppendEntries [log: %v] [matchIndex: %v] [nextIndex: %v]\n",
	//	rf.me, currentTerm, , matchIndex, nextIndex))
	logsToReplicate, logItemAtMatchIndex := getLogsToSend(rf.log, matchIndex, nextIndex)
	//debugLog(rf.me, fmt.Sprintf("[Leader %v][term %v] Leader sendAppendEntries Success [log: %v] [matchIndex: %v] [nextIndex: %v]\n",
	//	rf.me, currentTerm, *rf.log, matchIndex, nextIndex))

	lastLogIndex, lastLogTerm := getLastLogIndexAndTerm(rf.log)

	payload := EntryRequestPayload{
		LogsToReplicate:                 logsToReplicate,
		MatchIndex:                      matchIndex,
		LogItemAtMatchIndex:             logItemAtMatchIndex,
		NumOfCommittedLogsOfCurrentTerm: getNumOfCommittedLogsForTerm(currentTerm, rf.log),
		LastLogIndex:                    lastLogIndex,
		LastLogTerm:                     lastLogTerm,
	}

	//if debug {
	//	fmt.Printf("[Leader %v][term %v] sendAppendEntries %v [%v:%v]\n", rf.me, currentTerm, payload.Logs, startIndex, nextIndex)
	//}
	rf.mu.Unlock()

	entryRequestArgs := EntryRequestArgs{
		rf.me,
		currentTerm,
		payload,
	}
	entryRequestReply := EntryRequestReply{}
	debugLog(rf.me, fmt.Sprintf("[Leader %v][term %v] sending AppendEntriesRPC to %v] payload[%v]  [leaderLog: %v]\n", rf.me, currentTerm, peerId, payload, *rf.log))

	ok := rf.peers[peerId].Call("Raft.AppendEntriesOrHeartbeatRPC", entryRequestArgs, &entryRequestReply)
	if ok {
		debugLog(rf.me, fmt.Sprintf("[Leader %v][term %v] AppendEntriesRPC Response from %v [errorCode:%v updatedMatchIndex:%v] \n",
			rf.me, currentTerm, peerId, entryRequestReply.ErrorCode, entryRequestReply.UpdatedMatchIndex))
	}

	switch entryRequestReply.ErrorCode {
	case _200_OK():
		rf.mu.Lock()
		rf.matchIndexesOf[peerId] = entryRequestReply.UpdatedMatchIndex
		rf.mu.Unlock()
		for i := matchIndex + 1; i < nextIndex; i++ {

			rf.markConfirmationStatusAccepted(i, peerId)

			rf.mu.Lock()
			//debugLog(rf.me, fmt.Sprintf("[Leader %v][term %v] marking Accepted of [%v] at index %v by %v [acceptedCount:%v] \n",
			//	rf.me, currentTerm, (*rf.log)[i].Command, i, peerId, rf.confirmationStatusMap[i].acceptedCount))
			if rf.confirmationStatusMap[i].acceptedCount >= (len(rf.peers)/2+1) && !(*rf.log)[i].IsCommitted {

				debugLog(rf.me, fmt.Sprintf("[Leader %v][term %v] Leader Committing [%v] at index:%v confirmationStatusMap:%v\n",
					rf.me, currentTerm, (*rf.log)[i].Command, i, rf.confirmationStatusMap[i].isAccepted))
				(*rf.log)[i].IsCommitted = true
				rf.notifyCommit(i)

			}
			rf.mu.Unlock()
		}
		break

	case _401_OlderTerm():
		debugLog(rf.me, fmt.Sprintf("[Candidate %v][term %v] transitionBackToFollower : Peer[%v] responded with _401_OlderTerm\n",
			rf.me, rf.currentTerm, peerId))
		rf.transitionBackToFollower(rf.currentTerm)
		break

	case _402_MoreNumOfCommittedLogsOfCurrentTerm():
		debugLog(rf.me, fmt.Sprintf("[Candidate %v][term %v] transitionBackToFollower : Peer[%v] responded with _402_MoreNumOfCommittedLogsOfCurrentTerm\n",
			rf.me, rf.currentTerm, peerId))
		rf.transitionBackToFollower(rf.currentTerm)
		break

	case _403_UnequalLogsAtMatchIndex():
		rf.mu.Lock()
		rf.matchIndexesOf[peerId] = -1
		rf.mu.Unlock()
		go rf.sendAppendEntries(peerId, currentTerm)
		rf.persist()
		break

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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if isLeader {
		// 1. Init the logItem and metadata
		logItem := LogItem{
			Command:     command,
			Term:        term,
			IsCommitted: false,
		}
		confirmationStatus := generateInitialConfirmationStatusMap(rf.me, len(rf.peers))

		// 2. Write to own log [use Critical Section]
		*rf.log = append(*rf.log, logItem)
		nextIndex = rf.nextIndex
		rf.confirmationStatusMap[nextIndex] = confirmationStatus
		rf.nextIndex++
		debugLog(rf.me, fmt.Sprintf("[Leader %v][term %v] Start : cmd %v saved at %v [cs %v]\n",
			rf.me, rf.currentTerm, command, nextIndex, confirmationStatus))

		rf.persist()
	}

	return nextIndex + 1, term, isLeader
}

// Kill
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.h
	debugLog(rf.me, fmt.Sprintf("[Peer %v][term %v] Killed....................\n", rf.me, rf.currentTerm))
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
	rf.applyCh = applyCh

	// Making this channels buffered as a value might be present on this chan when this peer goes down
	// and new value wont get added to chan when this peer comes up again
	rf.resetElectionTimeoutChan = make(chan bool, 900000)

	// just started peer, should be a follower with term as 0
	rf.UpdateState(0, FollowerRole())

	rf.log = &[]LogItem{}
	//initialize currentTerm, votedForMap, log from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.nextIndex = len(*rf.log)
	rf.matchIndexesOf = initMatchIndexesOf(len(peers))
	rf.commitIndex = getNumOfCommittedLogsForTerm(rf.currentTerm, rf.log)
	rf.confirmationStatusMap = map[int]ConfirmationStatus{}

	rf.startElectionTimeoutBackgroundProcess()
	debugLog(rf.me, fmt.Sprintf("[Peer %v][term %v] Make..............\n", rf.me, rf.currentTerm))
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
					debugLog(rf.me, fmt.Sprintf("[Peer %v][term %v] timed out..........\n", rf.me, rf.currentTerm))
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
	_log := rf.log
	rf.mu.Unlock()

	//logsCount := getNumOfCommittedLogsForTerm(currentTerm, _log)

	// 3. request votes from peers
	debugLog(rf.me, fmt.Sprintf("[Candidate %v][term %v] tryTakingLeaderRole\n", rf.me, currentTerm))

	for index, _ := range rf.peers {

		if index == rf.me {
			// mark self vote and ignore requesting vote from self
			receivedVotesForTerm_, _ := rf.receivedVotesCounter.LoadOrStore(currentTerm, &sync.Map{})
			receivedVotesForTerm := receivedVotesForTerm_.(*sync.Map)
			(*receivedVotesForTerm).Store(rf.me, true)
			rf.receivedVotesCounter.Store(currentTerm, receivedVotesForTerm)
			continue
		}

		lastLogIndex, lastLogTerm := getLastLogIndexAndTerm(_log)

		requestVoteArgs := RequestVoteArgs{
			rf.me,
			currentTerm,
			lastLogTerm,
			lastLogIndex,
		}

		requestVoteReply := RequestVoteReply{}

		index := index
		go func(peerId int, requestVoteArgs RequestVoteArgs, requestVoteReply RequestVoteReply) {
			ok := rf.sendRequestVote(index, requestVoteArgs, &requestVoteReply)
			//if debug {
			//	fmt.Printf("[Candidate %v][term %v] tryTakingLeaderRole [response:%v] from %v\n",
			//		rf.me, currentTerm, requestVoteReply, index)
			//}
			if ok {
				// got vote reply
				//fmt.Printf("tryTakingLeaderRole %v -> got %v from %v\n", rf.me, requestVoteReply.VotedInFavour, index)
				receivedVotesCounter_, _ := rf.receivedVotesCounter.LoadOrStore(requestVoteReply.RespondingPeerTerm, &sync.Map{})
				receivedVotesCounter := receivedVotesCounter_.(*sync.Map)
				(*receivedVotesCounter).Store(index, requestVoteReply.VotedInFavour)

				if requestVoteReply.VotedInFavour {
					rf.transitionToLeaderIfSufficientVotes(requestVoteReply)

				} else if requestVoteReply.RespondingPeerTerm > currentTerm {
					// transition back to follower
					debugLog(rf.me, fmt.Sprintf("[Candidate %v][term %v] transitionBackToFollower : Peer[%v] responded with Term[%v] > currentTerm[%v]\n",
						rf.me, rf.currentTerm, index, requestVoteReply.RespondingPeerTerm, currentTerm))
					rf.transitionBackToFollower(requestVoteReply.RespondingPeerTerm)
				}
				rf.persist()
			}
		}(index, requestVoteArgs, requestVoteReply)
	}
}

func (rf *Raft) transitionToLeaderIfSufficientVotes(requestVoteReply RequestVoteReply) {
	rf.transitionLeaderMu.Lock()
	defer rf.transitionLeaderMu.Unlock()

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

	requiredVotes := len(rf.peers)/2 + 1 //(len(rf.peers) - 1) / 2
	receivedVotes := 0

	receivedVotesCounter_, _ := rf.receivedVotesCounter.LoadOrStore(respondingPeerTerm, &sync.Map{})
	receivedVotesCounter := receivedVotesCounter_.(*sync.Map)
	(*receivedVotesCounter).Range(func(key, ifVotedInFavour any) bool {
		if ifVotedInFavour.(bool) {
			receivedVotes++
		}
		return true
	})

	if receivedVotes < requiredVotes {
		// insufficient votes
		debugLog(rf.me, fmt.Sprintf("[Candidate %v][term %v] insufficient votes\n", rf.me, rf.currentTerm))

		return
	}

	if receivedVotes > 1 && rf.peerRole != LeaderRole() {
		debugLog(rf.me, fmt.Sprintf("[Candidate %v][term %v] TRANSITION To Leader\n", rf.me, rf.currentTerm))
		// Make current peer leader
		rf.peerRole = LeaderRole()
		rf.matchIndexesOf = initMatchIndexesOf(len(rf.peers))
		rf.confirmationStatusMap = map[int]ConfirmationStatus{}
		rf.startPeriodicBroadcastBackgroundProcess()
	}
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

// AppendEntriesOrHeartbeatRPC : PEER receiving the appendEntries or Heartbeat call
func (rf *Raft) AppendEntriesOrHeartbeatRPC(args EntryRequestArgs, reply *EntryRequestReply) {
	currentTerm, _ := rf.GetState()
	debugLog(rf.me, fmt.Sprintf("[peer %v][term %v] AppendEntriesRPC received from %v data[%v]\n", rf.me, currentTerm, args.LeaderId, args.EntryRequestPayload))

	updatedMatchIndex, errorCode := rf.reconcileLogs(args, currentTerm, rf.me)

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
	debugLog(rf.me, fmt.Sprintf("[peer %v][term %v] AppendEntriesRPC responding %v with reply [%v]\n", rf.me, currentTerm, args.LeaderId, &reply))
}

func (rf *Raft) reconcileLogs(
	args EntryRequestArgs,
	peerTerm int,
	peerId int) (int, ErrorCode) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	leaderLogsToReplicate := args.EntryRequestPayload.LogsToReplicate
	//leaderTerm := args.LeaderTerm
	leaderLogsCountOfCurrentTerm := args.EntryRequestPayload.NumOfCommittedLogsOfCurrentTerm
	matchIndex := args.EntryRequestPayload.MatchIndex
	logItemAtMatchIndex := args.EntryRequestPayload.LogItemAtMatchIndex
	updatedMatchIndex := matchIndex

	peerLastLogIndex, peerLastLogTerm := getLastLogIndexAndTerm(rf.log)
	//debugLog(peerId, fmt.Sprintf("[peer %v][term %v] reconcileLogs ENTRY [LeaderLastLogTerm %v] [peerLastLogTerm %v] ; [LeaderLastLogIndex %v] [PeerLastLogIndex: %v] [PeerLog %v]\n",
	//	peerId, peerTerm, args.EntryRequestPayload.LastLogTerm, peerLastLogTerm, args.EntryRequestPayload.LastLogIndex, peerLastLogIndex, rf.log))

	if args.EntryRequestPayload.LastLogTerm < peerLastLogTerm || (args.EntryRequestPayload.LastLogTerm == peerLastLogTerm && args.EntryRequestPayload.LastLogIndex < peerLastLogIndex) {
		// reject heartbeat/appendEntries
		debugLog(peerId, fmt.Sprintf("[peer %v][term %v] reconcileLogs reject_401  [LeaderLastLogTerm %v] [peerLastLogTerm %v] ; [LeaderLastLogIndex %v] [PeerLastLogIndex: %v]\n",
			peerId, peerTerm, args.EntryRequestPayload.LastLogTerm, peerLastLogTerm, args.EntryRequestPayload.LastLogIndex, peerLastLogIndex))
		return -1, _401_OlderTerm()
	}

	leaderLogLen := len(leaderLogsToReplicate)

	// If Payload exists
	if leaderLogsToReplicate != nil && leaderLogLen != 0 {
		//if debug {
		//	fmt.Printf("[peer %v][term %v] reconcileLogs payload exists\n", peerId, peerTerm)
		//}
		if leaderLogsCountOfCurrentTerm < getNumOfCommittedLogsForTerm(peerTerm, rf.log) {
			// reject
			debugLog(peerId, fmt.Sprintf("[peer %v][term %v] reconcileLogs reject_402\n", peerId, peerTerm))
			return -1, _402_MoreNumOfCommittedLogsOfCurrentTerm()
		}

		if matchIndex != -1 &&
			(matchIndex >= len(*rf.log) ||
				(0 <= matchIndex && matchIndex < len(*rf.log) && logItemAtMatchIndex != (*rf.log)[matchIndex])) {
			// matchIndex log is not matching with leader -> reject

			debugLog(peerId, fmt.Sprintf("[peer %v][term %v] reconcileLogs reject_403 [matchIndex:%v] [leaderLog:%v] [peerLog:%v] [logItemAtMatchIndex:%v]\n",
				peerId, peerTerm, matchIndex, leaderLogsToReplicate, *rf.log, logItemAtMatchIndex))

			return -1, _403_UnequalLogsAtMatchIndex()
		}

		rf.currentTerm = args.LeaderTerm
		for i := 0; i < len(leaderLogsToReplicate); i++ {
			peerIndex := matchIndex + 1 + i
			if peerIndex == len(*rf.log) {
				*rf.log = append(*rf.log, leaderLogsToReplicate[i])

			} else if peerIndex < len(*rf.log) {
				(*rf.log)[peerIndex] = leaderLogsToReplicate[i]

			} else {
				// peerIndex > peerLogLen
				// this case shouldn't reach
				debugLog(peerIndex, fmt.Sprintf("[peer %v][term %v] reconcileLogs unreachable condition reached\n", peerId, peerTerm))

			}

			if (leaderLogsToReplicate)[i].IsCommitted {
				debugLog(peerId, fmt.Sprintf("[peer %v][term %v] reconcileLogs: peer committing [%v] at index:%v [peerLog:%v]\n",
					peerId, peerTerm, (leaderLogsToReplicate)[i].Command, peerIndex, *rf.log))
				rf.notifyCommit(peerIndex)
				updatedMatchIndex = peerIndex
			}
		}

		//debugLog(peerId, fmt.Sprintf("[peer %v][term %v] reconcileLogs: [peerLog before trimming :%v]\n", peerId, peerTerm, *rf.log))
		// trimming unwanted entries from peerlog
		trimLength := leaderLogLen
		if matchIndex != -1 {
			trimLength += updatedMatchIndex
		}
		if len(*rf.log) > trimLength {
			*rf.log = (*rf.log)[:trimLength]
		}

		rf.nextIndex = len(*rf.log)
	}
	rf.persist()
	return updatedMatchIndex, _200_OK()
}

func (rf *Raft) transitionBackToFollower(currentTerm int) {
	if rf.peerRole == FollowerRole() {
		// already a follower
		return
	}

	debugLog(rf.me, fmt.Sprintf("[peer %v][term %v] transitionBackToFollower\n", rf.me, currentTerm))
	rf.UpdateState(currentTerm, FollowerRole())
	rf.resetElectionTimeoutChan <- true
	rf.persist()
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

func getLastLogIndexAndTerm(log *[]LogItem) (int, int) {
	lastLogIndex := len(*log) - 1
	lastLogTerm := -1
	if lastLogIndex != -1 {
		lastLogTerm = (*log)[lastLogIndex].Term
	}
	return lastLogIndex, lastLogTerm
}

func initMatchIndexesOf(peersCount int) map[int]int {
	matchIndexesOf := map[int]int{}
	for peerId := 0; peerId < peersCount; peerId++ {
		matchIndexesOf[peerId] = -1
	}
	return matchIndexesOf
}

func getLogsToSend(log *[]LogItem, matchIndex int, nextIndex int) ([]LogItem, LogItem) {
	var logsToReplicate []LogItem
	var logItemAtMatchIndex LogItem
	if matchIndex == -1 {
		logsToReplicate = *log
		logItemAtMatchIndex = LogItem{} // dummy

	} else {
		logsToReplicate = (*log)[matchIndex+1 : nextIndex]
		logItemAtMatchIndex = (*log)[matchIndex]
	}

	return logsToReplicate, logItemAtMatchIndex
}

func (rf *Raft) notifyCommit(index int) {
	if rf.applyCh == nil {
		// nil when called from test suite.
		// No need of null check in actual logic
		return
	}
	rf.applyCh <- ApplyMsg{
		Index:       index + 1, // verification code has 1 based-indexing
		Command:     (*rf.log)[index].Command,
		UseSnapshot: false,
		Snapshot:    nil,
	}
	debugLog(rf.me, fmt.Sprintf("[=======][term %v] commit notified %v %v\n",
		rf.currentTerm, index+1, (*rf.log)[index].Command))
}

func printLog(log []LogItem, name string) {
	logStr := name + "-> "
	fmt.Printf("\n")
	for i := 0; i < len(log); i++ {
		logStr += fmt.Sprintf("%v, ", log[i])
	}
	logStr += "\n"
	fmt.Printf(logStr)
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
	EntryRequestPayload EntryRequestPayload
}

type EntryRequestReply struct {
	Term              int
	ErrorCode         ErrorCode
	UpdatedMatchIndex int
}

type EntryRequestPayload struct {
	LogsToReplicate                 []LogItem
	MatchIndex                      int
	LogItemAtMatchIndex             LogItem
	NumOfCommittedLogsOfCurrentTerm int
	LastLogTerm                     int
	LastLogIndex                    int
}

// ConfirmationStatus Enum
type ConfirmationStatus struct {
	acceptedCount int
	isAccepted    []bool
}

func (rf *Raft) markConfirmationStatusAccepted(logIndex int, peerId int) {
	rf.confirmationStatusMapMU.Lock()
	defer rf.confirmationStatusMapMU.Unlock()

	if _, ok := rf.confirmationStatusMap[logIndex]; !ok {
		rf.confirmationStatusMap[logIndex] = generateInitialConfirmationStatusMap(rf.me, len(rf.peers))
	}

	if !rf.confirmationStatusMap[logIndex].isAccepted[peerId] {
		confirmationStatus, _ := rf.confirmationStatusMap[logIndex]
		confirmationStatus.isAccepted[peerId] = true
		confirmationStatus.acceptedCount++
		rf.confirmationStatusMap[logIndex] = confirmationStatus
	}
}

func generateInitialConfirmationStatusMap(self int, peersCount int) ConfirmationStatus {
	confirmationStatus := ConfirmationStatus{
		acceptedCount: 0,
		isAccepted:    make([]bool, peersCount),
	}

	for peerId := 0; peerId < peersCount; peerId++ {
		confirmationStatus.isAccepted[peerId] = self == peerId
	}
	confirmationStatus.acceptedCount = 1
	return confirmationStatus
}

type ErrorCode struct {
	Code int
}

func _200_OK() ErrorCode                                  { return ErrorCode{200} }
func _401_OlderTerm() ErrorCode                           { return ErrorCode{401} }
func _402_MoreNumOfCommittedLogsOfCurrentTerm() ErrorCode { return ErrorCode{402} }
func _403_UnequalLogsAtMatchIndex() ErrorCode             { return ErrorCode{403} }

// LOGGER
func debugLog(peerId int, logString string) {
	if false {
		f, err := os.OpenFile("debug-"+strconv.Itoa(peerId)+".log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Println(err)
		}
		defer f.Close()
		timestamp := time.Now().Format("15:04:05.000000")
		if _, err := f.WriteString("[" + timestamp + "]   " + logString); err != nil {
			log.Println(err)
		}
	}
}
