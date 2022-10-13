package chandy_lamport

import (
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

	snapshotsMap       sync.Map   // K,V -> snapshotId, SnapshotStatus (because of multiple snapshots)
	startSnapShotMutex sync.Mutex // to make startSnapShot() thread safe

	numOfOnGoingSnapshots      int // Access this using the below mutex only
	numOfOnGoingSnapshotsMutex sync.RWMutex
}

type SnapshotStatus struct {
	isOngoing     bool
	snapshotState SnapshotState
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
		sync.Mutex{},
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
		write switch case on message type
	*/

}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func (server *Server) StartSnapshot(snapshotId int) {
	// TODO: IMPLEMENT ME

	// Critical Section Impl for [This should be called only once per server.]
	_, ok := server.snapshotsMap.Load(snapshotId)
	if ok {
		return
	}
	server.startSnapShotMutex.Lock()
	{
		// double check if snapshot already started;  if yes then return
		_, ok := server.snapshotsMap.Load(snapshotId)
		if ok {
			server.startSnapShotMutex.Unlock()
			return
		}

		// store the empty snapshot first to avoid starting same snapshotId again
		server.snapshotsMap.Store(snapshotId, &SnapshotStatus{})
	}
	server.startSnapShotMutex.Unlock()

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
	server.snapshotsMap.Store(snapshotId, &snapshotStatus)

	marker := MarkerMessage{}
	marker.snapshotId = snapshotId
	go server.SendToNeighbors(marker) //TODO: need to confirm if go routine is needed here
}

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
