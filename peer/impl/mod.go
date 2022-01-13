package impl

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

var logout = zerolog.ConsoleWriter{
	Out:        os.Stdout,
	TimeFormat: time.RFC3339,
}
var log = zerolog.New(logout).Level(zerolog.ErrorLevel).
	With().Timestamp().Logger().
	With().Caller().Logger()

// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {
	// here you must return a struct that implements the peer.Peer functions.
	// Therefore, you are free to rename and change it as you want.

	n := node{conf: conf}
	n.messaging = initMessaging(n.conf)
	n.lockedRoutingTable.routingTable = make(peer.RoutingTable)
	// Add ourself to the list of peers
	n.started = false
	n.stoped = make(chan struct{})
	n.decoReco.isConnected = true
	n.decoReco.waiting = make(map[chan struct{}]struct{})
	n.socketMutex.Lock()
	n.address.setAddress(n.conf.Socket.GetAddress())
	n.socketMutex.Unlock()

	// Add ourself to the list of peers
	n.AddPeer(n.address.getAddress())

	proposer := PaxosProposer{
		Retry:        make(chan types.PaxosValue),
		AcceptCount:  make(map[string]uint),
		MsgsPrepared: make(map[string]struct{}),
		TagIsDone:    make(chan bool),
	}

	accepter := PaxosAccepter{
		AcceptStatus: PaxosAcceptStatus{status: false},
	}

	n.paxos = Paxos{
		proposer:            &proposer,
		accepter:            accepter,
		Step:                0,
		MaxID:               0,
		TLCMessagesReceived: make(map[uint]uint),
		HasBroadcastedTLC:   make(map[uint]bool),
		blocksReceived:      make(map[uint]types.BlockchainBlock),
		prepareMsg:          make(chan transport.Message),
	}
	return &n
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer
	// You probably want to keep the peer.Configuration on this struct:
	conf peer.Configuration
	*messaging
	started  bool
	stoped   chan struct{}
	paxos    Paxos
	decoReco DecoReco
	address  Address
}

type safeCounter struct {
	sync.Mutex
	v uint
}

func (c *safeCounter) reset() {
	c.Lock()
	defer c.Unlock()
	c.v = 0
}

func (c *safeCounter) incr() uint {
	c.Lock()
	defer c.Unlock()
	c.v++
	return c.v
}

func (c *safeCounter) get() uint {
	c.Lock()
	defer c.Unlock()
	return c.v
}

// Start implements peer.Service
func (n *node) Start() error {
	n.started = true
	// Register callbacks
	n.conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, n.ExecChatMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.RumorsMessage{}, n.ExecRumorsMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.StatusMessage{}, n.ExecStatusMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.AckMessage{}, n.ExecAckMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.EmptyMessage{}, n.ExecEmptyMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.PrivateMessage{}, n.ExecPrivateMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.DataRequestMessage{}, n.ExecDataRequestMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.DataReplyMessage{}, n.ExecDataReplyMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.SearchRequestMessage{}, n.ExecSearchRequestMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.SearchReplyMessage{}, n.ExecSearchReplyMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.PaxosPrepareMessage{}, n.ExecPaxosPrepareMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.PaxosPromiseMessage{}, n.ExecPaxosPromiseMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.PaxosProposeMessage{}, n.ExecPaxosProposeMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.PaxosAcceptMessage{}, n.ExecPaxosAcceptMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.TLCMessage{}, n.ExecTLCMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.NeighborsMessage{}, n.ExecNeighborsMessage)
	n.conf.MessageRegistry.RegisterMessageCallback(types.DisconnectMessage{}, n.ExecDisconnectMessage)

	if n.conf.AntiEntropyInterval > 0 {
		n.antiAnthropySig = time.NewTicker(n.conf.AntiEntropyInterval)
		go func() {
			for {
				n.checkAndwaitReconnection()
				<-n.antiAnthropySig.C
				err := n.sendAntiAnthropy()
				if err != nil {
					log.Warn().Msgf("failed to send anti anthropy message: %v", err)
				}
			}
		}()
	}
	if n.conf.HeartbeatInterval > 0 {
		n.heartbeatSig = time.NewTicker(n.conf.HeartbeatInterval)
		msg, err := n.conf.MessageRegistry.MarshalMessage(types.EmptyMessage{})
		if err != nil {
			log.Warn().Msgf("failed to marshal heartbeat packet: %v", err)
		}
		n.Broadcast(msg)
		go func() {
			for {
				n.checkAndwaitReconnection()
				<-n.heartbeatSig.C
				msg, err := n.conf.MessageRegistry.MarshalMessage(types.EmptyMessage{})
				if err != nil {
					log.Warn().Msgf("failed to marshal heartbeat packet: %v", err)
				}
				n.Broadcast(msg)
			}
		}()
	}
	if n.conf.NumberOfMissedHeartbeatsBeforeDisconnection > 0 {
		n.heartbeatCheck = time.NewTicker(n.conf.HeartbeatInterval)
		go func() {
			for {
				n.checkAndwaitReconnection()
				<-n.heartbeatCheck.C
				for node, counter := range n.missedHeartBeats.getCounters() {
					n.missedHeartBeats.setCounter(node, counter+1)
					if counter >= n.conf.NumberOfMissedHeartbeatsBeforeDisconnection {
						fmt.Println(n.address.getAddress() + ": sus")
						n.handleDisconnection(node)
					}
				}
			}
		}()
	}

	go func() {
		for {
			select {
			case <-n.stoped:
				return
			case prepare := <-n.paxos.prepareMsg:
				n.Broadcast(prepare)
			case retry := <-n.paxos.proposer.Retry:
				n.paxos.proposer.MsgsPreparedMutex.Lock()
				id := uint(len(n.paxos.proposer.MsgsPrepared))*n.conf.TotalPeers + n.conf.PaxosID
				n.paxos.proposer.MsgsPreparedMutex.Unlock()

				prepare := types.PaxosPrepareMessage{
					Step:   n.paxos.Step,
					ID:     id,
					Source: n.address.getAddress(),
				}
				msg, err := n.conf.MessageRegistry.MarshalMessage(prepare)
				if err != nil {
					continue
				}
				n.paxos.proposer.PhaseLock.Lock()
				n.paxos.proposer.Phase = 1
				n.paxos.proposer.PhaseLock.Unlock()
				n.paxos.proposer.PromisesLock.Lock()
				n.paxos.proposer.PromisesReceived = 0
				n.paxos.proposer.PromisesLock.Unlock()
				n.paxos.proposer.AcceptedValue = PaxosAcceptStatus{
					acceptedID: id,
					acceptedValue: types.PaxosValue{
						UniqID:   retry.UniqID,
						Filename: retry.Filename,
						Metahash: retry.Metahash,
					},
				}
				n.paxos.proposer.MsgsPreparedMutex.Lock()
				n.paxos.proposer.MsgsPrepared[retry.UniqID] = struct{}{}
				n.paxos.proposer.MsgsPreparedMutex.Unlock()

				err = n.Broadcast(msg)
				n.RetryTag(retry.Filename, retry.Metahash)
				if err != nil {
					fmt.Println(err)
				}
			default:
				n.checkAndwaitReconnection()
				n.socketMutex.Lock()
				pkt, err := n.conf.Socket.Recv(time.Second * 1 / 5)
				n.socketMutex.Unlock()
				if errors.Is(err, transport.TimeoutErr(0)) {
					log.Warn().Msgf("failed to receive packet (timeout): %v", err)
					continue
				} else if err != nil {
					log.Warn().Msgf("failed to receive packet: %v", err)
					continue
				} else {

					// do something with the packet and the err
					if n.address.getAddress() == pkt.Header.Destination {
						err = n.conf.MessageRegistry.ProcessPacket(pkt)
						if err != nil {
							log.Warn().Msgf("failed to process packet: %v", err)
							continue
						}

					} else {
						pkt.Header.RelayedBy = n.address.getAddress()
						// Probably not how to relay
						nextHop, ok := n.lockedRoutingTable.get(pkt.Header.Destination)
						if !ok {
							log.Warn().Msgf("failed to relay packet: unknown address")
						} else {
							err = n.sendPacketWithoutRoutingTable(nextHop, pkt, time.Second*1)
							if err != nil {
								log.Warn().Msgf("failed to relay packet: %v", err)
								continue
							}
						}

					}
				}
			}

		}
	}()

	return nil
}

// Stop implements peer.Service
func (n *node) Stop() error {
	if n.started {
		if !n.decoReco.getStatus() {
			n.NotifyWaitingGoroutines()
		}
		n.stoped <- struct{}{}
		if n.conf.AntiEntropyInterval > 0 {
			n.antiAnthropySig.Stop()
		}
		if n.conf.HeartbeatInterval > 0 {
			n.heartbeatSig.Stop()
		}
		if n.conf.NumberOfMissedHeartbeatsBeforeDisconnection > 0 {
			n.heartbeatCheck.Stop()
		}
		if n.decoReco.getStatus() {
			n.NotifyWaitingGoroutines()
		}
	}
	return nil
}

func (n *node) checkAndwaitReconnection() {
	if !n.decoReco.getStatus() {
		fmt.Println("waiting " + n.address.getAddress())
		channel := make(chan struct{})
		n.decoReco.setChannel(channel)
		<-channel
		fmt.Println("resuming " + n.address.getAddress())
		n.decoReco.deleteChannel(channel)
	}
}
