package impl

import (
	"errors"
	"sync"
	"time"

	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

// store information for messaging insterface
type messaging struct {
	lockedRoutingTable lockedRoutingTable
	rumorSeq           safeCounter
	rumorsCollection   rumorsCollection
	antiAnthropySig    *time.Ticker
	heartbeatSig       *time.Ticker
	heartbeatCheck     *time.Ticker
	waitedAck          ackChanMap
	lockedCatalog      lockedCatalog
	waitedDataReply    dataChanMap
	waitedSearchReply  searchChanMap
	searchesReceived   searchesReceived
	backupNodes        BackupMap
	missedHeartBeats   MissedHeartBeatCounter
	socketMutex        sync.Mutex
}

func initMessaging(conf peer.Configuration) *messaging {
	// here you must return a struct that implements the peer.Peer functions.
	// Therefore, you are free to rename and change it as you want.

	m := messaging{}
	m.lockedRoutingTable.routingTable = make(peer.RoutingTable)
	// Arbitratry length queue, should avoid to much goroutine spinning
	m.rumorsCollection.lockedRumors = make(map[string][]types.Rumor)
	m.waitedAck.waitingChan = make(map[string](chan struct{}))
	m.lockedCatalog.catalog = make(peer.Catalog)
	m.waitedDataReply.waitingChan = make(map[string](chan []byte))
	m.waitedSearchReply.waitingChan = make(map[string](chan []types.FileInfo))
	m.searchesReceived.searches = make(map[string]struct{})
	m.backupNodes.backups = make(map[string][]string)
	m.missedHeartBeats.counters = make(map[string]uint)
	return &m
}

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {
	n.checkAndwaitReconnection()
	selfAddr := n.address.getAddress()
	header := transport.NewHeader(selfAddr, selfAddr, dest, 0)
	pkt := transport.Packet{Header: &header, Msg: &msg}
	neighbor, ok := n.lockedRoutingTable.get(dest)
	if !ok {
		return xerrors.Errorf("failed to send unicast message: unknown neighbor")
	}
	err := n.sendPacketWithoutRoutingTable(neighbor, pkt, time.Second*1)
	if err != nil {
		return xerrors.Errorf("failed to send unicast message: %v", err)
	}
	return nil
}

func (n *node) Broadcast(msg transport.Message) error {
	n.checkAndwaitReconnection()
	selfAddr := n.address.getAddress()
	// Create RumorsMessage pkt and send it
	seq := n.rumorSeq.incr()
	rumor := types.Rumor{
		Origin:   selfAddr,
		Sequence: seq,
		Msg:      &msg,
	}
	rumors := make([]types.Rumor, 1)
	rumors[0] = rumor
	n.rumorsCollection.addRumors(rumors)
	rumorsMessage := types.RumorsMessage{Rumors: rumors}
	rumorsTransport, err := n.conf.MessageRegistry.MarshalMessage(&rumorsMessage)
	if err != nil {
		return xerrors.Errorf("failed to marshald rumors message: %v", err)
	}

	// Get a random neighbhor
	randNeighbor := getRandomNeighbor(n.GetRoutingTable(), selfAddr)
	if randNeighbor != "" {
		neighborHeader := transport.NewHeader(selfAddr, selfAddr, randNeighbor, 0)
		neighborPkt := transport.Packet{
			Header: &neighborHeader,
			Msg:    &rumorsTransport,
		}
		err := n.sendPacket(neighborPkt)
		if err != nil {
			return err
		}
		go n.waitForAck(neighborPkt)
	}
	// Process the message for oneself
	selfHeader := transport.NewHeader(selfAddr, selfAddr, selfAddr, 0)
	selfPkt := transport.Packet{
		Header: &selfHeader,
		Msg:    &msg,
	}
	err = n.conf.MessageRegistry.ProcessPacket(selfPkt)
	if err != nil {
		return xerrors.Errorf("failed to process broadcast message: %v", err)
	}

	return nil
}

// AddPeer implements peer.Service
func (n *node) AddPeer(addr ...string) {
	for _, a := range addr {
		n.handleNewNode(a, a)
	}
}

// GetRoutingTable implements peer.Service
func (n *node) GetRoutingTable() peer.RoutingTable {
	n.lockedRoutingTable.Lock()
	defer n.lockedRoutingTable.Unlock()
	copy := make(map[string]string, len(n.lockedRoutingTable.routingTable))
	for k, v := range n.lockedRoutingTable.routingTable {
		copy[k] = v
	}
	return copy
}

// SetRoutingEntry implements peer.Service
func (n *node) SetRoutingEntry(origin, relayAddr string) {
	// Make sure to keep our own address in the routing table
	if origin != n.address.getAddress() {
		n.handleNewNode(origin, relayAddr)
	}
}

func (n *node) handleNewNode(origin string, relayAddr string) {
	toRetransmit := true
	//don't retransmit if this is not new
	if n.GetRoutingTable()[origin] == relayAddr {
		toRetransmit = false
	}
	n.lockedRoutingTable.add(origin, relayAddr)
	if origin == relayAddr && toRetransmit {
		//keeping the neighbors updated of our list of neighbors
		n.sendNewNeighborsToPeers()
	}
	//initializes the counter of missed heart beats
	if n.conf.NumberOfMissedHeartbeatsBeforeDisconnection > 0 {
		n.missedHeartBeats.setCounter(origin, 0)
	}
}

// Generate status message and send it
func (n *node) sendAntiAnthropy() error {
	selfAddr := n.address.getAddress()
	statusMsg := n.rumorsCollection.generateStatusMessage()
	msg, err := n.conf.MessageRegistry.MarshalMessage(&statusMsg)
	if err != nil {
		return xerrors.Errorf("failed to marshal rumors forward message: %v", err)
	}
	randNeighbor := getRandomNeighbor(n.GetRoutingTable(), selfAddr)
	if randNeighbor != "" {
		header := transport.NewHeader(selfAddr, selfAddr, randNeighbor, 0)
		pkt := transport.Packet{
			Header: &header,
			Msg:    &msg,
		}
		err = n.sendPacket(pkt)
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *node) waitForAck(pkt transport.Packet) error {
	waitChan := n.waitedAck.createChan(pkt.Header.PacketID)
	if n.conf.AckTimeout <= 0 {
		<-waitChan
		n.waitedAck.deleteChan(pkt.Header.PacketID)
		return nil
	}
	t := time.NewTimer(n.conf.AckTimeout)
	for {
		select {
		case <-waitChan:
			// Stop timer
			n.waitedAck.deleteChan(pkt.Header.PacketID)
			t.Stop()
			return nil
		case <-t.C:
			// Send to another
			pkt.Header.Destination = getRandomNeighbor(n.GetRoutingTable(), n.address.getAddress(), pkt.Header.Destination)
			if pkt.Header.Destination != "" {
				err := n.sendPacket(pkt)
				if err != nil {
					return err
				}
				t.Reset(n.conf.AckTimeout)
			}
		}
	}
}

func (n *node) sendPacket(pkt transport.Packet) error {
	dest, ok := n.lockedRoutingTable.get(pkt.Header.Destination)
	if ok {
		n.socketMutex.Lock()
		err := n.conf.Socket.Send(dest, pkt, time.Second*1)
		n.socketMutex.Unlock()
		if errors.Is(err, transport.TimeoutErr(0)) {
			return xerrors.Errorf("failed to send packet (timeout): %v", err)
		} else if err != nil {
			return xerrors.Errorf("failed to send packet: %v", err)
		}
	} else {
		return xerrors.Errorf("failed to resolve route to destination: %s", pkt.Header.Destination)
	}
	return nil
}

func (n *node) sendPacketWithoutRoutingTable(dest string, pkt transport.Packet, timeout time.Duration) error {
	n.socketMutex.Lock()
	defer n.socketMutex.Unlock()
	err := n.conf.Socket.Send(dest, pkt, timeout)
	if errors.Is(err, transport.TimeoutErr(0)) {
		return xerrors.Errorf("failed to send packet (timeout): %v", err)
	} else if err != nil {
		return xerrors.Errorf("failed to send packet: %v", err)
	}
	return nil
}
