package impl

import (
	"crypto/ed25519"
	"encoding/base64"

	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

// Would need to implement a peer with unicast and broadcast not signing functions

// func NoSignPeer(conf peer.Configuration) peer.Peer {
// 	// here you must return a struct that implements the peer.Peer functions.
// 	// Therefore, you are free to rename and change it as you want.

// 	n := node{conf: conf}

// 	n.id = "I-nO-sIGn-MEsSAgeS"

// 	n.messaging = initMessaging(n.conf)
// 	n.lockedRoutingTable.routingTable = make(map[string]RoutingTableEntry)
// 	// Add ourself to the list of peers
// 	n.started = false
// 	n.stoped = make(chan struct{})

// 	// Add ourself to the list of peers
// 	// Should use SetRouting, this call block the node and it can't actually
// 	// receive the id req/rep during call (although it's not an issue with http node)
// 	n.lockedRoutingTable.setEntry(n.id, n.id, n.conf.Socket.GetAddress(), "That's you")

// 	proposer := PaxosProposer{
// 		Retry:        make(chan types.PaxosValue),
// 		AcceptCount:  make(map[string]uint),
// 		MsgsPrepared: make(map[string]struct{}),
// 		TagIsDone:    make(chan bool),
// 	}

// 	accepter := PaxosAccepter{
// 		AcceptStatus: PaxosAcceptStatus{status: false},
// 	}

// 	n.paxos = Paxos{
// 		proposer:            &proposer,
// 		accepter:            accepter,
// 		Step:                0,
// 		MaxID:               0,
// 		TLCMessagesReceived: make(map[uint]uint),
// 		HasBroadcastedTLC:   make(map[uint]bool),
// 		blocksReceived:      make(map[uint]types.BlockchainBlock),
// 		prepareMsg:          make(chan transport.Message),
// 	}
// 	return &n
// }

// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NoWrongSignPeer(conf peer.Configuration) peer.Peer {
	// here you must return a struct that implements the peer.Peer functions.
	// Therefore, you are free to rename and change it as you want.

	n := node{conf: conf}

	if conf.PrivateKey == nil {
		var err error
		var pubKey ed25519.PublicKey
		var privateKey ed25519.PrivateKey
		pubKey, privateKey, err = ed25519.GenerateKey(nil)
		if err != nil {
			panic("failed to generate keypairs, cannot recover")
		}
		n.privateKey = privateKey
		n.id = base64.StdEncoding.EncodeToString(pubKey)
	} else {
		n.privateKey = conf.PrivateKey
		pubKey, ok := conf.PrivateKey.Public().(ed25519.PublicKey)
		if !ok {
			panic("failed to generate public key from given private key, cannot recover")
		}

		n.id = base64.StdEncoding.EncodeToString(pubKey)
	}
	n.id = "lolWHOCaresAboutAuthenticity"

	n.messaging = initMessaging(n.conf)
	n.lockedRoutingTable.routingTable = make(map[string]types.RoutingTableEntry)
	// Add ourself to the list of peers
	n.started = false
	n.stoped = make(chan struct{})

	// Add ourself to the list of peers
	// Should use SetRouting, this call block the node and it can't actually
	// receive the id req/rep during call (although it's not an issue with http node)
	n.lockedRoutingTable.setEntry(n.id, n.id, n.conf.Socket.GetAddress(), "That's you")

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
