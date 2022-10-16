package chandy_lamport

import (
	"fmt"
	"log"
	"sync"
)

// The main participant of the distributed snapshot protocol.
// Servers exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one server to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.
type Server struct {
	Id            string
	Tokens        int
	sim           *Simulator
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src
	// TODO: ADD MORE FIELDS HERE

	snapshotsMap sync.Map // K,V -> snapshotId, *SnapshotStatus (because of multiple snapshots)
	//startSnapShotMutex sync.Mutex // to make startSnapShot() thread safe

	numOfOnGoingSnapshots      int // Access this using the below sync methods only
	numOfOnGoingSnapshotsMutex sync.RWMutex
}

type SnapshotStatus struct {
	isOngoing     bool
	snapshotState SnapshotState
	//isMarkerReceivedBackFromServer map[string]bool // K,V -> neighbourServerId, boolean that says whether marker was returned from that server
	//inboundMarkerResponseWaitGroup sync.WaitGroup
	markerReceivedCount int
}

// A unidirectional communication channel between two servers
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src    string
	dest   string
	events *Queue
}

func NewServer(id string, tokens int, sim *Simulator) *Server {
	return &Server{
		id,
		tokens,
		sim,
		make(map[string]*Link),
		make(map[string]*Link),
		sync.Map{},
		//sync.Mutex{},
		0,
		sync.RWMutex{},
	}
}

// Add a unidirectional link to the destination server
func (server *Server) AddOutboundLink(dest *Server) {
	if server == dest {
		return
	}
	l := Link{server.Id, dest.Id, NewQueue()}
	server.outboundLinks[dest.Id] = &l
	dest.inboundLinks[server.Id] = &l
}

// Send a message on all of the server's outbound links
func (server *Server) SendToNeighbors(message interface{}) {
	for _, serverId := range getSortedKeys(server.outboundLinks) {
		link := server.outboundLinks[serverId]
		server.sim.logger.RecordEvent(
			server,
			SentMessageEvent{server.Id, link.dest, message})
		link.events.Push(SendMessageEvent{
			server.Id,
			link.dest,
			message,
			server.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this server
func (server *Server) SendTokens(numTokens int, dest string) {
	if server.Tokens < numTokens {
		log.Fatalf("Server %v attempted to send %v tokens when it only has %v\n",
			server.Id, numTokens, server.Tokens)
	}
	message := TokenMessage{numTokens}
	server.sim.logger.RecordEvent(server, SentMessageEvent{server.Id, dest, message})
	// Update local state before sending the tokens
	server.Tokens -= numTokens
	link, ok := server.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from server %v\n", dest, server.Id)
	}
	link.events.Push(SendMessageEvent{
		server.Id,
		dest,
		message,
		server.sim.GetReceiveTime()})
}

// Callback for when a message is received on this server.
// When the snapshot algorithm completes on this server, this function
// should notify the simulator by calling `sim.NotifySnapshotComplete`.
func (server *Server) HandlePacket(src string, message interface{}) {
	// TODO: IMPLEMENT ME

	/*
		write switch cases on message type
	*/

	switch message.(type) {
	case TokenMessage:

		//fmt.Println("[server "+server.Id+"]  HandlePacket got Token %d = %v", message.(TokenMessage).numTokens, server.snapshotsMap)
		fmt.Printf("[Server %s] -> HandlePacket got Token %d = %v\n", server.Id, message.(TokenMessage).numTokens, server.snapshotsMap)
		// Basic Token update (independent of chandy-lamport algo)
		// This update should not be affected by our snapshotting code
		server.Tokens += message.(TokenMessage).numTokens

		// if no ongoing snapshots -> do nothing further
		numOfOnGoingSnapshots := server.getNumOfOnGoingSnapshots()
		fmt.Printf("[server %s]  HandlePacket : ongoing snapshots: %d = %v\n", server.Id, numOfOnGoingSnapshots, server.snapshotsMap)
		if numOfOnGoingSnapshots == 0 {
			// nothing to handle
			return
		}

		// forEach ongoing snapshots -> add the incoming token as tracked token from that inbound channel
		server.snapshotsMap.Range(func(snapshotId, _snapshotStatus interface{}) bool {

			snapshotStatus := _snapshotStatus.(*SnapshotStatus)
			if snapshotStatus.isOngoing {
				fmt.Printf("[server %s]  HandlePacket : Tracking %v\n", server.Id, server.snapshotsMap)
				snapshotMsg := SnapshotMessage{
					src:     src,
					dest:    server.Id,
					message: message,
				}
				snapshotStatus.snapshotState.messages = append(snapshotStatus.snapshotState.messages, &snapshotMsg)
			}
			return true // Range method expects a true; otherwise it stops iterating further.
		})
		break

	case MarkerMessage:

		fmt.Printf("[server %s]  HandlePacket got Marker from %v %v\n", server.Id, src, server.snapshotsMap)
		snapshotId := message.(MarkerMessage).snapshotId

		snapshotAlreadyStarted := !server.startSnapshotOnlyIfNotAlreadyStarted(snapshotId)

		if !snapshotAlreadyStarted {
			// This is the first time, the marker has arrived.
			fmt.Printf("[server  %s ]  HandlePacket starting snapshot  %v\n ", server.Id, server.snapshotsMap)
			server.StartSnapshot(snapshotId)
		}

		// mark done from this inbound channel
		_snapshotStatus, _ := server.snapshotsMap.Load(snapshotId)
		snapshotStatus := _snapshotStatus.(*SnapshotStatus)
		snapshotStatus.markerReceivedCount++

		if snapshotStatus.markerReceivedCount == len(server.inboundLinks) {
			// all markers received from inbound channels -> snapshot complete for this server
			fmt.Println("[server " + server.Id + "]  notifySnapshotComplete: all markers have been received from inbound channels. Notifying...")
			snapshotStatus.isOngoing = false
			server.addToNumOfOnGoingSnapshots(-1)
			// notify this event to the simulator
			server.sim.NotifySnapshotComplete(server.Id, snapshotId)
		}

		break
	default:
		fmt.Printf("[server  %s ]  Uknown msg type received \n", server.Id)
	}

}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	fmt.Println("[server " + server.Id + "]  startSnapshot")
	// TODO: IMPLEMENT ME

	/**
	1. set the current snapshot as ongoing for this server
	2. take the snapshot of the tokens
	3. send markers to all neighbours
	*/
	snapshotStatus := SnapshotStatus{}
	snapshotStatus.isOngoing = true
	server.addToNumOfOnGoingSnapshots(1)

	tokensSnap := map[string]int{}
	tokensSnap[server.Id] = server.Tokens
	snapshotStatus.snapshotState.tokens = tokensSnap
	snapshotStatus.markerReceivedCount = 0
	server.snapshotsMap.Store(snapshotId, &snapshotStatus)

	marker := MarkerMessage{}
	marker.snapshotId = snapshotId
	server.SendToNeighbors(marker)
}

//func (server *Server) notifySnapshotComplete(snapshotId int) {
//	fmt.Println("[server " + server.Id + "]  notifySnapshotComplete: waiting for all markers")
//	_snapshotStatus, _ := server.snapshotsMap.Load(snapshotId)
//	snapshotStatus := _snapshotStatus.(*SnapshotStatus)
//	//snapshotStatus.inboundMarkerResponseWaitGroup.Wait()
//	//for i := 0; i < len(server.inboundLinks); i++ {
//	//	x := <-snapshotStatus.markerReceivedChan
//	//	x = !x // dummy operation to use x
//	//}
//
//
//}

func (server *Server) addToNumOfOnGoingSnapshots(numToAdd int) {
	server.numOfOnGoingSnapshotsMutex.Lock()
	defer server.numOfOnGoingSnapshotsMutex.Unlock()
	server.numOfOnGoingSnapshots += numToAdd
}

func (server *Server) getNumOfOnGoingSnapshots() int {
	server.numOfOnGoingSnapshotsMutex.RLock()
	defer server.numOfOnGoingSnapshotsMutex.RUnlock()
	return server.numOfOnGoingSnapshots
}

/*
Returns false if snapshot is already started,
if not then, this method creates an entry in the snapshotsMap to mark its starting, and returns true
*/
func (server *Server) startSnapshotOnlyIfNotAlreadyStarted(snapshotId int) bool {
	//fmt.Println("[server " + server.Id + "]  startSnapshotOnlyIfNotAlreadyStarted")
	_, ok := server.snapshotsMap.Load(snapshotId)
	if ok {
		//fmt.Println("[server " + server.Id + "]  startSnapshotOnlyIfNotAlreadyStarted returning false")
		return false
	}
	//fmt.Println("[server " + server.Id + "]  creating SnapshotStatus")
	//server.startSnapShotMutex.Lock()
	//{
	//	// double check if snapshot already started; first check is to avoid locking and checking
	//	_, ok := server.snapshotsMap.Load(snapshotId)
	//	if ok {
	//		//server.startSnapShotMutex.Unlock()
	//		return false
	//	}
	//
	//	// store the empty snapshot first to avoid starting same snapshotId again
	//	server.snapshotsMap.Store(snapshotId, &SnapshotStatus{})
	//}
	//server.startSnapShotMutex.Unlock()
	return true
}

// creates marker response tracker with all values as false.
// These values will be marked true when marker is received the second time
//
//	func (server *Server) initIsMarkerReceivedBackFromServer() map[string]bool {
//		isMarkerReceivedBackFromServer := make(map[string]bool)
//		for _, serverId := range getSortedKeys(server.inboundLinks) {
//			isMarkerReceivedBackFromServer[serverId] = false
//		}
//		return isMarkerReceivedBackFromServer
//	}
func (server *Server) collectSnapshotState(snapshotId int) SnapshotState {
	fmt.Println("[server " + server.Id + "]  collectSnapshotState")
	_snapshotStatus, _ := server.snapshotsMap.Load(snapshotId)
	snapshotStatus := _snapshotStatus.(*SnapshotStatus)

	fmt.Println("====================" + server.Id + "===========================")
	fmt.Println(snapshotStatus.snapshotState.tokens)

	msgs := snapshotStatus.snapshotState.messages
	for _, msg := range msgs {
		token := msg.message.(TokenMessage).numTokens
		fmt.Println("("+msg.src+" "+msg.dest+" token", token)
	}

	fmt.Println("===============================================")

	return snapshotStatus.snapshotState
}
