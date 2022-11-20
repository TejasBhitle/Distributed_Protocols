package raft

import (
	"fmt"
	"sync"
	"testing"
)

func TestPersistence0(t *testing.T) {
	currentTerm := 3

	votedForMap := sync.Map{}
	votedForMap.Store(currentTerm, 4)

	log := []LogItem{}

	rf := getEmptyRaft()
	rf.currentTerm = currentTerm
	rf.votedForMap = votedForMap
	rf.log = &log
	rf.persist()

	data := rf.persister.ReadRaftState()

	rf2 := getEmptyRaft()
	rf2.readPersist(data)

	if rf.currentTerm != rf2.currentTerm {
		fmt.Printf("mismatch currentTerm %v %v \n", rf.currentTerm, rf2.currentTerm)
	}

	v1, ok1 := rf.votedForMap.Load(currentTerm)
	v2, ok2 := rf2.votedForMap.Load(currentTerm)
	if ok1 != ok2 || v1 != v2 {
		fmt.Printf("mismatch votedForMap %v %v %v %v \n", ok1, ok2, v1, v2)
	}

	if !areLogsEqual(rf.log, rf2.log) {
		fmt.Printf("mismatch areLogsEqual %v %v\n", len(*rf.log), len(*rf2.log))
	}
}

func TestPersistence1(t *testing.T) {
	currentTerm := 0

	votedForMap := sync.Map{}

	log := []LogItem{}
	log = append(log, LogItem{
		IsCommitted: false,
		Command:     "cmd1",
		Term:        5,
	})

	rf := getEmptyRaft()
	rf.currentTerm = currentTerm
	rf.votedForMap = votedForMap
	rf.log = &log
	rf.persist()

	data := rf.persister.ReadRaftState()

	rf2 := getEmptyRaft()
	rf2.readPersist(data)

	if rf.currentTerm != rf2.currentTerm {
		fmt.Printf("mismatch currentTerm %v %v \n", rf.currentTerm, rf2.currentTerm)
	}

	v1, ok1 := rf.votedForMap.Load(currentTerm)
	v2, ok2 := rf2.votedForMap.Load(currentTerm)
	if ok1 != ok2 || v1 != v2 {
		fmt.Printf("mismatch votedForMap %v %v %v %v \n", ok1, ok2, v1, v2)
	}

	if !areLogsEqual(rf.log, rf2.log) {
		fmt.Printf("mismatch areLogsEqual %v %v\n", len(*rf.log), len(*rf2.log))
	}
}

func getEmptyRaft() Raft {
	return Raft{
		mu:                       sync.Mutex{},
		peers:                    nil,
		persister:                MakePersister(),
		me:                       0,
		peerRole:                 FollowerRole(),
		currentTerm:              1,
		votedForMap:              sync.Map{},
		receivedVotesCounter:     sync.Map{},
		resetElectionTimeoutChan: nil,
		applyCh:                  nil,
		log:                      nil,
		nextIndex:                0,
		commitIndex:              0,
		confirmationStatusMap:    nil,
		matchIndexesOf:           nil,
	}
}
