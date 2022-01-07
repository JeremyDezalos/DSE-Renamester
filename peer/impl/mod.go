package impl

import (
	"crypto/ed25519"
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

	if conf.PrivateKey == nil {
		var err error
		var pubKey []byte
		pubKey, n.privateKey, err = ed25519.GenerateKey(nil)
		if err != nil {
			panic("failed to generate keypairs, cannot recover")
		}
		n.id = string(pubKey)
	} else {
		n.privateKey = conf.PrivateKey
		pubKey, ok := conf.PrivateKey.Public().(ed25519.PublicKey)
		if !ok {
			panic("failed to generate public key from given private key, cannot recover")
		}
		n.id = string(pubKey)
	}

	n.messaging = initMessaging(n.conf)
	n.lockedRoutingTable.routingTable = make(peer.RoutingTable)
	// Add ourself to the list of peers
	n.started = false
	n.stoped = make(chan struct{})

	// Add ourself to the list of peers
	n.AddPeer(conf.Socket.GetAddress())

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
	id   string // string type for compatibility with previous routing table
	// And ease of use in UI
	privateKey ed25519.PrivateKey
	*messaging
	started bool
	stoped  chan struct{}
	paxos   Paxos
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

	if n.conf.AntiEntropyInterval > 0 {
		n.antiAnthropySig = time.NewTicker(n.conf.AntiEntropyInterval)
		go func() {
			for {
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
				<-n.heartbeatSig.C
				msg, err := n.conf.MessageRegistry.MarshalMessage(types.EmptyMessage{})
				if err != nil {
					log.Warn().Msgf("failed to marshal heartbeat packet: %v", err)
				}
				n.Broadcast(msg)
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
					Source: n.conf.Socket.GetAddress(),
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
				pkt, err := n.conf.Socket.Recv(time.Second * 1 / 5)
				if errors.Is(err, transport.TimeoutErr(0)) {
					log.Warn().Msgf("failed to receive packet (timeout): %v", err)
					continue
				} else if err != nil {
					log.Warn().Msgf("failed to receive packet: %v", err)
					continue
				} else {

					// do something with the packet and the err
					if n.conf.Socket.GetAddress() == pkt.Header.Destination {
						err = n.conf.MessageRegistry.ProcessPacket(pkt)
						if err != nil {
							log.Warn().Msgf("failed to process packet: %v", err)
							continue
						}

					} else {
						pkt.Header.RelayedBy = n.conf.Socket.GetAddress()
						// Probably not how to relay
						nextHop, ok := n.lockedRoutingTable.get(pkt.Header.Destination)
						if !ok {
							log.Warn().Msgf("failed to relay packet: unknown address")
						} else {

							err = n.conf.Socket.Send(nextHop, pkt, time.Second*1)
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
		n.stoped <- struct{}{}
		if n.conf.AntiEntropyInterval > 0 {
			n.antiAnthropySig.Stop()
		}
		if n.conf.HeartbeatInterval > 0 {
			n.heartbeatSig.Stop()
		}
	}
	return nil
}
