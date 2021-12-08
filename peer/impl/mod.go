package impl

import (
	"errors"
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

	// Add ourself to the list of peers
	n.AddPeer(conf.Socket.GetAddress())
	n.paxos = initMultiPaxos(&n)

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
	paxos   *multiPaxos
	started bool
	stoped  chan struct{}
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

			default:
				pkt, err := n.conf.Socket.Recv(time.Second * 1)
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
