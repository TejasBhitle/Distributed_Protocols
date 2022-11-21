package raft

import (
	"fmt"
	"testing"
)

func Test0(t *testing.T) {
	// input
	rf := Raft{}
	leaderLog := &[]LogItem{}
	*leaderLog = append(*leaderLog, LogItem{"cmd0", 0, false})
	matchIndex := -1
	peerLog := &[]LogItem{}

	numOfLogs := getNumOfCommittedLogsForTerm(0, leaderLog)
	if numOfLogs != 0 {
		t.Fatal("numOfLogs mismatch")
	}

	// common logic
	logsToReplicate, logItemAtMatchIndex := getLogsToSend(leaderLog, matchIndex, len(*leaderLog))
	fmt.Printf("%v  %v\n", logsToReplicate, logItemAtMatchIndex)

	args := EntryRequestArgs{
		LeaderId:   0,
		LeaderTerm: 0,
		EntryRequestPayload: EntryRequestPayload{
			LogsToReplicate:                 logsToReplicate,
			LogItemAtMatchIndex:             logItemAtMatchIndex,
			MatchIndex:                      matchIndex,
			NumOfCommittedLogsOfCurrentTerm: numOfLogs,
		},
	}

	updatedMatchIndex, errorCode := rf.reconcileLogs(args, peerLog, 0, 0)
	fmt.Printf("updatedMatchIndex:%v  errorCode:%v\n", updatedMatchIndex, errorCode)
	// assertions
	if !areLogsEqual(leaderLog, peerLog) {
		fmt.Printf("%v %v\n", len(*leaderLog), len(*peerLog))
		t.Fatal("unequal logs")
	}
	if updatedMatchIndex != -1 {
		t.Fatal("wrong updatedMatchIndex")
	}
	if errorCode != _200_OK() {
		t.Fatal("wrong errorCode")
	}

	t.Log("All OK..")
}

func Test1(t *testing.T) {
	rf := Raft{}
	// input
	leaderLog := &[]LogItem{}
	*leaderLog = append(*leaderLog, LogItem{"cmd0", 0, false})
	*leaderLog = append(*leaderLog, LogItem{"cmd1", 0, false})
	matchIndex := -1
	peerLog := &[]LogItem{}
	*peerLog = append(*peerLog, LogItem{"cmd5", 0, false})
	*peerLog = append(*peerLog, LogItem{"cmd6", 0, false})
	*peerLog = append(*peerLog, LogItem{"cmd7", 0, false})

	numOfLogs := getNumOfCommittedLogsForTerm(0, leaderLog)

	// common logic
	logsToReplicate, logItemAtMatchIndex := getLogsToSend(leaderLog, matchIndex, len(*leaderLog))

	args := EntryRequestArgs{
		LeaderId:   0,
		LeaderTerm: 0,
		EntryRequestPayload: EntryRequestPayload{
			LogsToReplicate:                 logsToReplicate,
			LogItemAtMatchIndex:             logItemAtMatchIndex,
			MatchIndex:                      matchIndex,
			NumOfCommittedLogsOfCurrentTerm: numOfLogs,
		},
	}

	updatedMatchIndex, errorCode := rf.reconcileLogs(args, peerLog, 0, 0)
	fmt.Printf("%v %v\n", updatedMatchIndex, errorCode)
	// assertions
	if !areLogsEqual(leaderLog, peerLog) {
		fmt.Printf("%v %v\n", len(*leaderLog), len(*peerLog))
		t.Fatal("unequal logs")
	}
	if updatedMatchIndex != -1 {
		t.Fatal("wrong updatedMatchIndex")
	}
	if errorCode != _200_OK() {
		t.Fatal("wrong errorCode")
	}

	t.Log("All OK..")
}

func Test2(t *testing.T) {
	rf := Raft{}
	// input
	leaderLog := &[]LogItem{}
	*leaderLog = append(*leaderLog, LogItem{"cmd0", 0, true})
	*leaderLog = append(*leaderLog, LogItem{"cmd1", 0, false})
	matchIndex := -1
	peerLog := &[]LogItem{}
	*peerLog = append(*peerLog, LogItem{"cmd5", 0, false})
	*peerLog = append(*peerLog, LogItem{"cmd6", 0, false})
	*peerLog = append(*peerLog, LogItem{"cmd7", 0, false})

	numOfLogs := getNumOfCommittedLogsForTerm(0, leaderLog)

	// common logic
	logsToReplicate, logItemAtMatchIndex := getLogsToSend(leaderLog, matchIndex, len(*leaderLog))
	args := EntryRequestArgs{
		LeaderId:   0,
		LeaderTerm: 0,
		EntryRequestPayload: EntryRequestPayload{
			LogsToReplicate:                 logsToReplicate,
			LogItemAtMatchIndex:             logItemAtMatchIndex,
			MatchIndex:                      matchIndex,
			NumOfCommittedLogsOfCurrentTerm: numOfLogs,
		},
	}

	updatedMatchIndex, errorCode := rf.reconcileLogs(args, peerLog, 0, 0)
	fmt.Printf("%v %v\n", updatedMatchIndex, errorCode)
	// assertions
	if !areLogsEqual(leaderLog, peerLog) {
		fmt.Printf("%v %v\n", len(*leaderLog), len(*peerLog))
		t.Fatal("unequal logs")
	}
	if updatedMatchIndex != 0 {
		t.Fatal("wrong updatedMatchIndex")
	}
	if errorCode != _200_OK() {
		t.Fatal("wrong errorCode")
	}

	t.Log("All OK..")
}

func Test3(t *testing.T) {
	rf := Raft{}
	// input
	leaderLog := &[]LogItem{}
	*leaderLog = append(*leaderLog, LogItem{"cmd0", 0, false})
	*leaderLog = append(*leaderLog, LogItem{"cmd1", 0, false})
	matchIndex := -1

	peerLog := &[]LogItem{}
	*peerLog = append(*peerLog, LogItem{"cmd5", 0, true})
	*peerLog = append(*peerLog, LogItem{"cmd6", 0, false})
	*peerLog = append(*peerLog, LogItem{"cmd7", 0, false})

	numOfLogs := getNumOfCommittedLogsForTerm(0, leaderLog)

	// common logic
	logsToReplicate, logItemAtMatchIndex := getLogsToSend(leaderLog, matchIndex, len(*leaderLog))

	args := EntryRequestArgs{
		LeaderId:   0,
		LeaderTerm: 0,
		EntryRequestPayload: EntryRequestPayload{
			LogsToReplicate:                 logsToReplicate,
			LogItemAtMatchIndex:             logItemAtMatchIndex,
			MatchIndex:                      matchIndex,
			NumOfCommittedLogsOfCurrentTerm: numOfLogs,
		},
	}

	updatedMatchIndex, errorCode := rf.reconcileLogs(args, peerLog, 0, 0)
	fmt.Printf("%v %v\n", updatedMatchIndex, errorCode)
	// assertions
	if errorCode != _402_MoreNumOfCommittedLogsOfCurrentTerm() {
		t.Fatal("wrong errorCode")
	}
	if updatedMatchIndex != -1 {
		t.Fatal("wrong updatedMatchIndex")
	}

	t.Log("All OK..")
}

func Test4(t *testing.T) {
	rf := Raft{}
	// input
	leaderLog := &[]LogItem{}
	*leaderLog = append(*leaderLog, LogItem{"cmd0", 0, true})
	*leaderLog = append(*leaderLog, LogItem{"cmd1", 0, true})
	*leaderLog = append(*leaderLog, LogItem{"cmd2", 1, true})
	*leaderLog = append(*leaderLog, LogItem{"cmd3", 1, false})
	*leaderLog = append(*leaderLog, LogItem{"cmd4", 1, false})
	matchIndex := -1
	peerLog := &[]LogItem{}
	*peerLog = append(*peerLog, LogItem{"cmd0", 0, true})
	*peerLog = append(*peerLog, LogItem{"cmd1", 0, true})
	*peerLog = append(*peerLog, LogItem{"cmd2", 1, true})

	numOfLogs := getNumOfCommittedLogsForTerm(1, leaderLog)

	// common logic
	logsToReplicate, logItemAtMatchIndex := getLogsToSend(leaderLog, matchIndex, len(*leaderLog))

	args := EntryRequestArgs{
		LeaderId:   0,
		LeaderTerm: 1,
		EntryRequestPayload: EntryRequestPayload{
			LogsToReplicate:                 logsToReplicate,
			LogItemAtMatchIndex:             logItemAtMatchIndex,
			MatchIndex:                      matchIndex,
			NumOfCommittedLogsOfCurrentTerm: numOfLogs,
		},
	}

	updatedMatchIndex, errorCode := rf.reconcileLogs(args, peerLog, 1, 0)
	fmt.Printf("%v %v\n", updatedMatchIndex, errorCode)
	// assertions
	if !areLogsEqual(leaderLog, peerLog) {
		fmt.Printf("%v %v\n", len(*leaderLog), len(*peerLog))
		t.Fatal("unequal logs")
	}
	if updatedMatchIndex != 2 {
		t.Fatal("wrong updatedMatchIndex")
	}
	if errorCode != _200_OK() {
		t.Fatal("wrong errorCode")
	}

	t.Log("All OK..")
}

func Test5(t *testing.T) {
	rf := Raft{}
	// input
	leaderLog := &[]LogItem{}
	*leaderLog = append(*leaderLog, LogItem{"cmd0", 0, true})
	matchIndex := 0
	peerLog := &[]LogItem{}
	*peerLog = append(*peerLog, LogItem{"cmd0", 0, true})

	numOfLogs := getNumOfCommittedLogsForTerm(0, leaderLog)

	// common logic
	logsToReplicate, logItemAtMatchIndex := getLogsToSend(leaderLog, matchIndex, len(*leaderLog))

	if len(logsToReplicate) > 0 {
		fmt.Printf("logsToReplicate unexpected %v \n", len(logsToReplicate))
		t.Fatal("")
	}

	args := EntryRequestArgs{
		LeaderId:   0,
		LeaderTerm: 1,
		EntryRequestPayload: EntryRequestPayload{
			LogsToReplicate:                 logsToReplicate,
			LogItemAtMatchIndex:             logItemAtMatchIndex,
			MatchIndex:                      matchIndex,
			NumOfCommittedLogsOfCurrentTerm: numOfLogs,
		},
	}

	updatedMatchIndex, errorCode := rf.reconcileLogs(args, peerLog, 1, 0)
	fmt.Printf("%v %v\n", updatedMatchIndex, errorCode)
	// assertions
	if !areLogsEqual(leaderLog, peerLog) {
		fmt.Printf("%v %v\n", len(*leaderLog), len(*peerLog))
		t.Fatal("unequal logs")
	}
	if updatedMatchIndex != 0 {
		t.Fatal("wrong updatedMatchIndex")
	}
	if errorCode != _200_OK() {
		t.Fatal("wrong errorCode")
	}

	t.Log("All OK..")
}

//func printLog(log *[]LogItem) {
//	fmt.Printf("printling log")
//	for i := 0; i < len(*log); i++ {
//		fmt.Printf("%v ", (*log)[i])
//	}
//	fmt.Printf("\n")
//}
