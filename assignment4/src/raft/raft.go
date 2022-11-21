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

const debug = true

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

	applyCh chan ApplyMsg

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
	RequestingPeerId           int
	RequestingPeerTerm         int
	CommittedLogsInCurrentTerm int
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

		} else if args.CommittedLogsInCurrentTerm < getNumOfCommittedLogsForTerm(rf.currentTerm, rf.log) {
			// for the latest term, peer has more log entries than candidate -> reject
			reply.VotedInFavour = false

		} else {
			reply.VotedInFavour = true
		}
	}

	if reply.VotedInFavour {
		rf.UpdateState(args.RequestingPeerTerm, FollowerRole())
		rf.resetElectionTimeoutChan <- true
	}
	if debug {
		fmt.Printf("[Peer %v][term %v] RequestVote voted %v to %v] \n",
			rf.me, rf.currentTerm, reply.VotedInFavour, args.RequestingPeerId)
	}
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

	payload := EntryRequestPayload{
		Logs: []LogItem{},
	}

	// 1. Get the uncommitted Logs to send to this peer
	rf.mu.Lock()
	log := rf.log
	nextIndex := rf.nextIndex
	matchIndex := rf.matchIndexesOf[peerId]
	payload.MatchIndex = matchIndex
	startIndex := matchIndex
	if startIndex == -1 {
		startIndex = 0
	}
	payload.Logs = (*log)[startIndex:nextIndex]
	if debug {
		fmt.Printf("[Leader %v][term %v] sendAppendEntries %v [%v:%v]\n", rf.me, currentTerm, payload.Logs, startIndex, nextIndex)
	}

	payload.NumOfCommittedLogsOfCurrentTerm = getNumOfCommittedLogsForTerm(currentTerm, log)
	rf.mu.Unlock()

	entryRequestArgs := EntryRequestArgs{
		rf.me,
		currentTerm,
		payload,
	}
	entryRequestReply := EntryRequestReply{}
	if debug {
		fmt.Printf("[Leader %v][term %v] sending AppendEntriesRPC to %v] \n", rf.me, currentTerm, peerId)
	}
	ok := rf.peers[peerId].Call("Raft.AppendEntriesOrHeartbeatRPC", entryRequestArgs, &entryRequestReply)
	if ok {
		if debug {
			fmt.Printf("[Leader %v][term %v] AppendEntriesRPC Response from %v [errorCode:%v updatedMatchIndex:%v] \n",
				rf.me, currentTerm, peerId, entryRequestReply.ErrorCode, entryRequestReply.UpdatedMatchIndex)
		}

		switch entryRequestReply.ErrorCode {
		case _200_OK():
			rf.mu.Lock()
			rf.matchIndexesOf[peerId] = entryRequestReply.UpdatedMatchIndex
			rf.mu.Unlock()
			for i := matchIndex + 1; i < nextIndex; i++ {

				rf.markConfirmationStatusAccepted(i, peerId)
				if debug {
					fmt.Printf("[Leader %v][term %v] marking Accepted of %v by %v [acceptedCount:%v] \n", rf.me, currentTerm, i, peerId, rf.confirmationStatusMap[i].acceptedCount)
				}
				if rf.confirmationStatusMap[i].acceptedCount > (len(rf.peers)-1)/2 && !(*rf.log)[i].IsCommitted {

					if debug {
						fmt.Printf("[Leader %v][term %v] Leader Committing %v \n", rf.me, currentTerm, i)
					}

					(*rf.log)[i].IsCommitted = true
					rf.applyCh <- ApplyMsg{
						Index:       i,
						Command:     (*rf.log)[i].Command,
						UseSnapshot: false,
						Snapshot:    nil,
					}
				}
			}
			break

		case _401_OlderTerm():
			rf.transitionBackToFollower(rf.currentTerm)
			break

		case _402_MoreNumOfCommittedLogsOfCurrentTerm():
			rf.transitionBackToFollower(rf.currentTerm)
			break

		case _403_UnequalLogsAtMatchIndex():
			rf.mu.Lock()
			rf.matchIndexesOf[peerId] = -1
			rf.mu.Unlock()
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

		if debug {
			fmt.Printf("[Leader %v][term %v] Start : cmd saved at %v [cs %v]\n", rf.me, rf.currentTerm, nextIndex, confirmationStatus)
		}
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
	rf.applyCh = applyCh

	// Making this channels buffered as a value might be present on this chan when this peer goes down
	// and new value wont get added to chan when this peer comes up again
	rf.resetElectionTimeoutChan = make(chan bool, 20)

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
					if debug {
						fmt.Printf("[Peer %v][term %v] timed out..........\n", rf.me, rf.currentTerm)
					}
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
	if debug {
		fmt.Printf("[Candidate %v][term %v] tryTakingLeaderRole\n", rf.me, currentTerm)
	}
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
		}(index, requestVoteArgs, requestVoteReply)
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

	if receivedVotes < requiredVotes {
		// insufficient votes
		if debug {
			fmt.Printf("[Candidate %v][term %v] insufficient votes\n", rf.me, rf.currentTerm)
		}
		return
	}

	if receivedVotes > 1 && rf.peerRole != LeaderRole() {
		if debug {
			fmt.Printf("[Candidate %v][term %v] TRANSITION To Leader\n", rf.me, rf.currentTerm)
		}
		// Make current peer leader
		rf.peerRole = LeaderRole()
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
	if debug {
		fmt.Printf("[peer %v][term %v] AppendEntriesRPC received from %v data[%v]\n", rf.me, currentTerm, args.LeaderId, args.EntryRequestPayload)
	}

	rf.mu.Lock()
	updatedMatchIndex, errorCode := reconcileLogs(args, rf.log, currentTerm, rf.me)
	rf.mu.Unlock()

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

	//fmt.Printf("[peer %v][term %v] reconcileLogs begin\n", peerId, peerTerm)

	leaderLog := args.EntryRequestPayload.Logs
	leaderTerm := args.LeaderTerm
	leaderLogsCountOfCurrentTerm := args.EntryRequestPayload.NumOfCommittedLogsOfCurrentTerm
	matchIndex := args.EntryRequestPayload.MatchIndex
	updatedMatchIndex := matchIndex

	if peerTerm > leaderTerm {
		// reject heartbeat/appendEntries
		//fmt.Printf("[peer %v][term %v] reconcileLogs reject_401\n", peerId, peerTerm)
		return -1, _401_OlderTerm()
	}

	leaderLogLen := len(leaderLog)
	peerLogLen := len(*peerLog)

	// If Payload exists
	if leaderLog != nil && leaderLogLen != 0 {
		//fmt.Printf("[peer %v][term %v] reconcileLogs payload exists\n", peerId, peerTerm)
		if leaderLogsCountOfCurrentTerm < getNumOfCommittedLogsForTerm(peerTerm, peerLog) {
			// reject
			if debug {
				fmt.Printf("[peer %v][term %v] reconcileLogs reject_402\n", peerId, peerTerm)
			}
			return -1, _402_MoreNumOfCommittedLogsOfCurrentTerm()
		}

		if matchIndex != -1 &&
			(matchIndex >= peerLogLen ||
				(0 <= matchIndex && matchIndex < len(*peerLog) && (leaderLog)[matchIndex] != (*peerLog)[matchIndex])) {
			// matchIndex log is not matching with leader -> reject
			if debug {
				fmt.Printf("[peer %v][term %v] reconcileLogs reject_403 [matchIndex:%v]\n", peerId, peerTerm, matchIndex)
				printLog(leaderLog, "leaderLog")
				printLog(*peerLog, "peerLog")
			}
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
			endIndex = matchIndex + leaderLogLen

		}

		for i := startIndex; i < endIndex; i++ {
			if i == len(*peerLog) {
				if debug {
					fmt.Printf("DEBUG [peer %v][term %v] [matchIndex:%v] [endIndex:%v] [leaderLogLen:%v] [peerLogLen:%v]\n",
						peerId, peerTerm, matchIndex, endIndex, leaderLogLen, peerLogLen)
				}
				*peerLog = append(*peerLog, (leaderLog)[i])
			} else if i < len(*peerLog) {
				(*peerLog)[i] = (leaderLog)[i]
			} else {
				// this case shouldnt reach
				if debug {
					fmt.Printf("[peer %v][term %v] reconcileLogs unreachable condition reached\n", peerId, peerTerm)
				}
			}

			if (leaderLog)[i].IsCommitted {
				if debug {
					fmt.Printf("[peer %v][term %v] reconcileLogs: peer committing %v \n", peerId, peerTerm, i)
				}
				updatedMatchIndex = i
			}
		}
		//fmt.Printf("[peer %v][term %v] reconcileLogs reconcilling part1 done\n", peerId, peerTerm)

		peerLen := len(*peerLog)
		if peerLen > leaderLogLen {
			*peerLog = (*peerLog)[:leaderLogLen]
		}
		//fmt.Printf("[peer %v][term %v] reconcileLogs reconcilling part2 done\n", peerId, peerTerm)
	}
	return updatedMatchIndex, _200_OK()
}

func (rf *Raft) transitionBackToFollower(currentTerm int) {
	if debug {
		fmt.Printf("[peer %v][term %v] transitionBackToFollower\n", rf.me, currentTerm)
	}
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

func getLeaderLogsToSend(leaderLog *[]LogItem, matchIndex int) []LogItem {
	leaderLogToSend := []LogItem{}
	if matchIndex < 0 {
		leaderLogToSend = *leaderLog
	} else {
		leaderLogToSend = (*leaderLog)[matchIndex:]
		if len(leaderLogToSend) == 1 {
			leaderLogToSend = []LogItem{}
		}
	}
	return leaderLogToSend
}

func initMatchIndexesOf(peersCount int) map[int]int {
	matchIndexesOf := map[int]int{}
	for peerId := 0; peerId < peersCount; peerId++ {
		matchIndexesOf[peerId] = -1
	}
	return matchIndexesOf
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
	Logs                            []LogItem
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
