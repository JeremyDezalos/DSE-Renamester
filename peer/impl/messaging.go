package impl

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"time"

	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

// store information for messaging insterface
type messaging struct {
	lockedRoutingTable lockedRoutingTable
	waitedIdReplies    chan string
	rumorSeq           safeCounter
	rumorsCollection   rumorsCollection
	antiAnthropySig    *time.Ticker
	heartbeatSig       *time.Ticker
	waitedAck          ackChanMap
	lockedCatalog      lockedCatalog
	waitedDataReply    dataChanMap
	waitedSearchReply  searchChanMap
	searchesReceived
}

func initMessaging(conf peer.Configuration) *messaging {
	// here you must return a struct that implements the peer.Peer functions.
	// Therefore, you are free to rename and change it as you want.

	m := messaging{}
	m.lockedRoutingTable.routingTable = make(map[string]routingTableEntry)
	m.waitedIdReplies = make(chan string, 5)
	// Arbitratry length queue, should avoid to much goroutine spinning
	m.rumorsCollection.lockedRumors = make(map[string][]types.Rumor)
	m.waitedAck.waitingChan = make(map[string](chan struct{}))
	m.lockedCatalog.catalog = make(peer.Catalog)
	m.waitedDataReply.waitingChan = make(map[string](chan []byte))
	m.waitedSearchReply.waitingChan = make(map[string](chan []types.FileInfo))
	m.searchesReceived.searches = make(map[string]struct{})
	return &m
}

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {
	header := transport.NewHeader(n.id, n.id, dest, 0)
	pkt := transport.Packet{Header: &header, Msg: &msg}
	err := n.sendPacket(pkt)
	if err != nil {
		return xerrors.Errorf("failed to send unicast message: %v", err)
	}
	return nil
}

func (n *node) Broadcast(msg transport.Message) error {

	// Create RumorsMessage pkt and send it
	seq := n.rumorSeq.incr()
	rumor := types.Rumor{
		Origin:   n.id,
		Sequence: seq,
		Msg:      &msg,
	}
	// Signing self emited packets
	sig := ed25519.Sign(n.privateKey, msg.Payload)
	msg.Signature = sig

	rumors := make([]types.Rumor, 1)
	rumors[0] = rumor
	n.rumorsCollection.addRumors(rumors)
	rumorsMessage := types.RumorsMessage{Rumors: rumors}
	rumorsTransport, err := n.conf.MessageRegistry.MarshalMessage(&rumorsMessage)
	if err != nil {
		return xerrors.Errorf("failed to marshald rumors message: %v", err)
	}

	// Get a random neighbhor
	randNeighbor := getRandomNeighbor(n.GetRoutingTable(), n.id)
	if randNeighbor != "" {
		neighborHeader := transport.NewHeader(n.id, n.id, randNeighbor, 0)
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
	selfHeader := transport.NewHeader(n.id, n.id, n.id, 0)
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
func (n *node) AddPeer(addr ...string) error {

	// for _, a := range addr {
	// 	n.lockedRoutingTable.add(a, a)
	// }
	// TODO: Send IdRequest and wait for IdReply
	// Send identity request (with empty destination because we do not know id yet)
	requestHeader := transport.NewHeader(n.id, n.id, "", 0)
	request := types.IdRequestMessage{
		Ip: n.conf.Socket.GetAddress(),
	}
	requestMsg, err := n.conf.MessageRegistry.MarshalMessage(&request)
	if err != nil {
		// AddPeer should be able to return an error now!
		return xerrors.Errorf("failed to marshal id request message: %v", err)
	}
	requestPkt := transport.Packet{
		Header: &requestHeader,
		Msg:    &requestMsg,
	}
	// Direct send (because the routing table is yet to be populated)
	remainingAddr := make(map[string]struct{})
	var aggregateErr error
	for _, ip := range addr {

		err := n.conf.Socket.Send(ip, requestPkt, 1*time.Second)
		if errors.Is(err, transport.TimeoutErr(0)) {
			aggregateErr = fmt.Errorf("%v\n - failed to send packet for ip %s (timeout): %v", aggregateErr, ip, err)
		} else if err != nil {
			aggregateErr = fmt.Errorf("%v\n - failed to send packet for ip %s: %v", aggregateErr, ip, err)
		} else {
			remainingAddr[ip] = struct{}{}
		}
	}
	// and wait for IdReply (with multiple attempts)
	t := time.NewTicker(n.conf.AckTimeout)
	retry := 0
	for len(remainingAddr) > 0 && retry < 3 {
		select {
		case replyId := <-n.waitedIdReplies:
			delete(remainingAddr, replyId)
		case <-t.C:
			// Retry for the missing addresses
			for ip := range remainingAddr {
				err := n.conf.Socket.Send(ip, requestPkt, 1*time.Second)
				if errors.Is(err, transport.TimeoutErr(0)) {
					aggregateErr = fmt.Errorf("%v\n - failed to send packet for ip %s (timeout): %v", aggregateErr, ip, err)
				} else if err != nil {
					aggregateErr = fmt.Errorf("%v\n - failed to send packet for ip %s: %v", aggregateErr, ip, err)
				}
			}
			retry = retry + 1
			// Send to another
			t.Reset(n.conf.AckTimeout)
		}
	}
	t.Stop()

	if len(remainingAddr) > 0 {
		aggregateErr = fmt.Errorf("%v\nfollowing peers could not be added: %s", aggregateErr, remainingAddr)
	}

	//keeping the neighbors updated
	n.sendNewNeighborsToPeers()
	return aggregateErr
}

// GetRoutingTable implements peer.Service
func (n *node) GetRoutingTable() peer.RoutingTable {
	n.lockedRoutingTable.Lock()
	defer n.lockedRoutingTable.Unlock()
	copy := make(map[string]string, len(n.lockedRoutingTable.routingTable))
	for k, v := range n.lockedRoutingTable.routingTable {
		copy[k] = v.nextHop
	}
	return copy
}

// SetRoutingEntry implements peer.Service
func (n *node) SetRoutingEntry(origin, relayAddr string) {
	if relayAddr == "" {
		n.lockedRoutingTable.delete(origin)
	} else {
		n.lockedRoutingTable.add(origin, relayAddr)
		if origin == relayAddr {
			n.sendNewNeighborsToPeers()
		}
	}
}

func (n *node) GetNeighborsTable() map[string]string {
	n.lockedRoutingTable.Lock()
	defer n.lockedRoutingTable.Unlock()
	copy := make(map[string]string, len(n.lockedRoutingTable.routingTable))
	for k, v := range n.lockedRoutingTable.routingTable {
		if v.address != "" {
			copy[k] = v.address
		}
	}
	return copy
}

func (n *node) GetAliasTable() map[string]string {
	n.lockedRoutingTable.Lock()
	defer n.lockedRoutingTable.Unlock()
	copy := make(map[string]string, len(n.lockedRoutingTable.routingTable))
	for k, v := range n.lockedRoutingTable.routingTable {
		copy[k] = v.alias
	}
	return copy
}

// Generate status message and send it
func (n *node) sendAntiAnthropy() error {
	statusMsg := n.rumorsCollection.generateStatusMessage()
	msg, err := n.conf.MessageRegistry.MarshalMessage(&statusMsg)
	if err != nil {
		return xerrors.Errorf("failed to marshal rumors forward message: %v", err)
	}
	randNeighbor := getRandomNeighbor(n.GetRoutingTable(), n.id)
	if randNeighbor != "" {
		header := transport.NewHeader(n.id, n.id, randNeighbor, 0)
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
			pkt.Header.Destination = getRandomNeighbor(n.GetRoutingTable(), n.conf.Socket.GetAddress(), pkt.Header.Destination)
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
	nextHop, ok := n.lockedRoutingTable.get(pkt.Header.Destination)
	var dest string
	if ok {
		dest, ok = n.lockedRoutingTable.resolveNeighbor(nextHop)
	}
	if ok {
		if pkt.Header.Source == n.id {
			// Signing self emited packets
			sig := ed25519.Sign(n.privateKey, pkt.Msg.Payload)
			pkt.Msg.Signature = sig
		}

		if ok {
			err := n.conf.Socket.Send(dest, pkt, time.Second*1)
			if errors.Is(err, transport.TimeoutErr(0)) {
				return xerrors.Errorf("failed to send packet (timeout): %v", err)
			} else if err != nil {
				return xerrors.Errorf("failed to send packet: %v", err)
			}
		}
	} else {
		return xerrors.Errorf("failed to resolve route to destination: %s", pkt.Header.Destination)
	}
	return nil
}
