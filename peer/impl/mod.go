package impl

import (
	"crypto"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/xid"
	"go.dedis.ch/cs438/peer"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

// NewPeer creates a new peer. You can change the content and location of this
// function but you MUST NOT change its signature and package location.
func NewPeer(conf peer.Configuration) peer.Peer {
	// here you must return a struct that implements the peer.Peer functions.
	// Therefore, you are free to rename and change it as you want.
	myAddr := conf.Socket.GetAddress()
	proposer := PaxosProposer{
		Retry:        make(chan types.PaxosValue),
		Phase2Start:  make(chan types.PaxosProposeMessage),
		Consensus:    make(chan types.PaxosAcceptMessage),
		AcceptCount:  make(map[string]uint),
		MsgsPrepared: make(map[string]struct{}),
		TagIsDone:    make(chan bool),
	}

	accepter := PaxosAccepter{
		AcceptStatus:       PaxosAcceptStatus{status: false},
		PromiseToBroadcast: make(chan types.PrivateMessage),
		AcceptToBroadcast:  make(chan types.PaxosAcceptMessage),
	}

	paxos := Paxos{
		proposer:            &proposer,
		accepter:            accepter,
		Step:                0,
		MaxID:               0,
		TLCMessagesReceived: make(map[uint]uint),
		HasBroadcastedTLC:   make(map[uint]bool),
		blocksReceived:      make(map[uint]types.BlockchainBlock),
		TLCToForward:        make(chan types.TLCMessage),
	}

	messaging := Messaging{
		conf:                      conf,
		lastMessageReceivedByNode: types.StatusMessage{},
		queueRumors:               make(chan transport.Packet),
		queueRoutingEntry:         make(chan RoutingEntry),
		queueStatus:               make(chan transport.Packet),
		timeout:                   make(chan RumorWaitingForAck),
		waitingAckList:            make(map[RumorWaitingForAck]transport.Packet),
		rumors:                    make(map[RumorMissing]types.Rumor),
		dataToReply:               make(chan transport.Packet),
		dataReply:                 make(chan DataReply),
		checkDataReplyStatus:      make(chan string),
		dataReceived:              make(map[string]DataKeyValue),
		searchRequest:             make(chan SearchRequest),
		searchResult:              make(map[string][]types.FileInfo),
		searchReply:               make(chan SearchReply),
		paxos:                     paxos,
	}

	conf.MessageRegistry.RegisterMessageCallback(types.ChatMessage{}, messaging.ExecChatMessage)
	conf.MessageRegistry.RegisterMessageCallback(types.RumorsMessage{}, messaging.ExecRumorsMessage)
	conf.MessageRegistry.RegisterMessageCallback(types.StatusMessage{}, messaging.ExecStatusMessage)
	conf.MessageRegistry.RegisterMessageCallback(types.AckMessage{}, messaging.ExecAckMessage)
	conf.MessageRegistry.RegisterMessageCallback(types.EmptyMessage{}, messaging.ExecEmptyMessage)
	conf.MessageRegistry.RegisterMessageCallback(types.PrivateMessage{}, messaging.ExecPrivateMessage)
	conf.MessageRegistry.RegisterMessageCallback(types.DataRequestMessage{}, messaging.ExecDataRequestMessage)
	conf.MessageRegistry.RegisterMessageCallback(types.DataReplyMessage{}, messaging.ExecDataReplyMessage)
	conf.MessageRegistry.RegisterMessageCallback(types.SearchRequestMessage{}, messaging.ExecSearchRequestMessage)
	conf.MessageRegistry.RegisterMessageCallback(types.SearchReplyMessage{}, messaging.ExecSearchReplyMessage)
	conf.MessageRegistry.RegisterMessageCallback(types.PaxosPromiseMessage{}, messaging.ExecPaxosPromiseMessage)
	conf.MessageRegistry.RegisterMessageCallback(types.PaxosAcceptMessage{}, messaging.ExecPaxosAcceptMessage)
	conf.MessageRegistry.RegisterMessageCallback(types.PaxosPrepareMessage{}, messaging.ExecPaxosPrepareMessage)
	conf.MessageRegistry.RegisterMessageCallback(types.PaxosProposeMessage{}, messaging.ExecPaxosProposeMessage)
	conf.MessageRegistry.RegisterMessageCallback(types.TLCMessage{}, messaging.ExecTLCMessage)

	return &node{
		conf:                 conf,
		myAddr:               myAddr,
		rTable:               peer.RoutingTable{myAddr: myAddr},
		started:              false,
		finished:             make(chan bool),
		counter:              1,
		messaging:            &messaging,
		neighbors:            make(map[string]struct{}),
		catalog:              make(peer.Catalog),
		searchAllGetResult:   make(chan string),
		searchAllResult:      make(chan []types.FileInfo),
		searchFirstGetResult: make(chan string),
		searchFirstResult:    make(chan []types.FileInfo),
	}
}

// node implements a peer to build a Peerster system
//
// - implements peer.Peer
type node struct {
	peer.Peer
	conf peer.Configuration

	// the node has started
	started bool
	// finished: used to communicate between goroutine and stop function
	finished chan bool
	// myAddr: addresss of the node
	myAddr string
	// rTable: routing table of the node
	rTable peer.RoutingTable
	// mapMutex: mutex of the routing table
	mapMutex sync.Mutex
	// counter for the sequence numbers of the packets
	counter uint
	// list of nodes connected to this node
	neighbors map[string]struct{}
	// mutex for the list of neighbors
	neighborsMutex sync.Mutex

	messaging *Messaging
	// ticker used in anti entropy mechanism
	tickerAntiEntropy *time.Ticker

	tickerHeartBeat *time.Ticker

	catalogMutex sync.Mutex

	catalog peer.Catalog

	fileNamesSliceTemp []string

	searchAllGetResult chan string

	searchAllResult chan []types.FileInfo

	searchFirstGetResult chan string

	searchFirstResult chan []types.FileInfo
}

//types created so that the handlers can access to everything they need
type Messaging struct {
	conf peer.Configuration
	// packets to be sent
	queueRumors chan transport.Packet
	// packets to be sent
	queueStatus chan transport.Packet

	queueRoutingEntry chan RoutingEntry
	// mapMutex: mutex of the status message
	lastMessageReceivedByNodeMutex sync.Mutex
	// expected sequence number of each node
	lastMessageReceivedByNode types.StatusMessage
	// mutex of the map of the rumors received
	rumorsMutex sync.Mutex
	// rumors received
	rumors map[RumorMissing]types.Rumor
	// mutex of the map of waiting acks
	waitingAckListMutex sync.Mutex
	// map from the acks we are waiting for
	waitingAckList map[RumorWaitingForAck]transport.Packet
	// notif timeout ack ot be sent to the node
	timeout chan RumorWaitingForAck
	// used when the node stops waiting the reply during Download, asks if the reply is here
	checkDataReplyStatus chan string
	// says if the reply is here or not
	dataReply chan DataReply
	// DataReplyMessage to send
	dataToReply chan transport.Packet
	// data received from the network
	dataReceived map[string]DataKeyValue
	// mutex of the map of the data received
	dataReceivedMutex sync.Mutex

	searchRequest chan SearchRequest

	searchResultMutex sync.Mutex

	searchResult map[string][]types.FileInfo

	searchReply chan SearchReply

	paxos Paxos
}

type Paxos struct {
	proposer *PaxosProposer
	accepter PaxosAccepter

	Step  uint
	MaxID uint

	TLCMessagesReceived map[uint]uint
	HasBroadcastedTLC   map[uint]bool
	blocksReceived      map[uint]types.BlockchainBlock
	TLCToForward        chan types.TLCMessage
}

type PaxosProposer struct {
	PhaseLock         sync.Mutex
	Phase             uint
	PromisesReceived  int
	PromisesLock      sync.Mutex
	AcceptedValue     PaxosAcceptStatus
	Retry             chan types.PaxosValue
	Phase2Start       chan types.PaxosProposeMessage
	CountLock         sync.Mutex
	AcceptCount       map[string]uint
	Consensus         chan types.PaxosAcceptMessage
	MsgsPrepared      map[string]struct{}
	MsgsPreparedMutex sync.Mutex
	TagIsDone         chan bool
}

type PaxosAccepter struct {
	AcceptStatus       PaxosAcceptStatus
	PromiseToBroadcast chan types.PrivateMessage
	AcceptToBroadcast  chan types.PaxosAcceptMessage
}

type RumorMissing struct {
	Address  string
	Sequence uint
}

type RumorWaitingForAck struct {
	Address  string
	PacketID string
}

type RoutingEntry struct {
	Origin string
	Relay  string
}

type DataReply struct {
	requestID string
	status    bool
	value     []byte
}

type DataKeyValue struct {
	Key   string
	Value []byte
}

type SearchReply struct {
	Msg types.SearchReplyMessage

	origin string
}

type SearchRequest struct {
	Msg types.SearchRequestMessage

	Relay string
}

type PaxosAcceptStatus struct {
	status        bool
	acceptedID    uint
	acceptedValue types.PaxosValue
}

func (m *Messaging) ExecChatMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	_, ok := msg.(*types.ChatMessage)

	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	return nil
}

func (m *Messaging) ExecRumorsMessage(msg types.Message, pkt transport.Packet) error {
	m.lastMessageReceivedByNodeMutex.Lock()
	defer m.lastMessageReceivedByNodeMutex.Unlock()
	m.rumorsMutex.Lock()
	defer m.rumorsMutex.Unlock()
	// cast the message to its actual type. You assume it is the right type.
	rumorsMsg, ok := msg.(*types.RumorsMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	toForward := false
	for _, rumor := range rumorsMsg.Rumors {
		origin := rumor.Origin
		expectedSequenceNumber, ok := m.lastMessageReceivedByNode[origin]
		theMessageIsExpected := false
		if !ok {
			m.lastMessageReceivedByNode[origin] = 1
			theMessageIsExpected = true
			expectedSequenceNumber = 1
		} else {
			expectedSequenceNumber += 1
			if expectedSequenceNumber == rumor.Sequence {
				theMessageIsExpected = true
				m.lastMessageReceivedByNode[origin] = expectedSequenceNumber
			}
		}
		if theMessageIsExpected {
			go func() {
				m.queueRoutingEntry <- RoutingEntry{
					Origin: origin,
					Relay:  pkt.Header.RelayedBy,
				}
			}()
			toForward = true
			m.rumors[RumorMissing{rumor.Origin, rumor.Sequence}] = rumor

			newPkt := transport.Packet{
				Header: pkt.Header,
				Msg:    rumor.Msg,
			}
			err := m.conf.MessageRegistry.ProcessPacket(newPkt)
			if err != nil {
				return xerrors.Errorf("error while processing the new packet")
			}
		}
	}

	ack := types.AckMessage{
		AckedPacketID: pkt.Header.PacketID,
		Status:        m.lastMessageReceivedByNode,
	}
	header := transport.NewHeader(m.conf.Socket.GetAddress(), m.conf.Socket.GetAddress(), pkt.Header.Source, 0)
	ackMsg, err := m.conf.MessageRegistry.MarshalMessage(ack)

	if err != nil {

		return xerrors.Errorf("error while marshaling packet")
	}
	ackPkt := transport.Packet{
		Header: &header,
		Msg:    &ackMsg,
	}
	m.conf.Socket.Send(pkt.Header.RelayedBy, ackPkt, time.Second*1)

	go func() {
		if toForward {
			m.queueRumors <- pkt
		}
	}()

	return err
}

func (m *Messaging) ExecStatusMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	statusPtr, ok := msg.(*types.StatusMessage)
	m.lastMessageReceivedByNodeMutex.Lock()
	defer m.lastMessageReceivedByNodeMutex.Unlock()

	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	status := *statusPtr
	myNewRumors := m.MapDifference(status, m.lastMessageReceivedByNode)
	hisNewRumors := m.MapDifference(m.lastMessageReceivedByNode, status)
	go func() {
		m.queueRoutingEntry <- RoutingEntry{
			Origin: pkt.Header.Source,
			Relay:  pkt.Header.RelayedBy,
		}
	}()
	iHaveNewRumors := len(myNewRumors) != 0
	heHasNewRumors := len(hisNewRumors) != 0
	if iHaveNewRumors {
		//send a status message
		var newPkt transport.Packet
		header := transport.NewHeader(m.conf.Socket.GetAddress(), m.conf.Socket.GetAddress(), pkt.Header.Source, 0)
		statusMsg, err := m.conf.MessageRegistry.MarshalMessage(m.lastMessageReceivedByNode)
		if err != nil {
			return xerrors.Errorf("error while marshaling packet")
		}
		newPkt = transport.Packet{
			Header: &header,
			Msg:    &statusMsg,
		}

		m.conf.Socket.Send(pkt.Header.RelayedBy, newPkt, time.Second*1)
	}

	if heHasNewRumors {
		//send new rumors
		var rumorsMsg types.RumorsMessage
		m.rumorsMutex.Lock()
		for _, rumorMissing := range hisNewRumors {
			rumorsMsg.Rumors = append(rumorsMsg.Rumors, m.rumors[rumorMissing])
		}
		m.rumorsMutex.Unlock()
		header := transport.NewHeader(m.conf.Socket.GetAddress(), m.conf.Socket.GetAddress(), pkt.Header.Source, 0)
		rumorsyMsg, err := m.conf.MessageRegistry.MarshalMessage(rumorsMsg)
		if err != nil {
			return xerrors.Errorf("error while marshaling packet")
		}
		newPkt := transport.Packet{
			Header: &header,
			Msg:    &rumorsyMsg,
		}

		m.SendRumorsMsg(m.conf, pkt.Header.Source, newPkt, time.Second*1)
	}

	if !iHaveNewRumors && !heHasNewRumors {
		if m.conf.ContinueMongering > rand.Float64() {
			go func() {
				m.queueStatus <- pkt
			}()
		}
	}

	return nil
}

func (m *Messaging) ExecAckMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	ack, ok := msg.(*types.AckMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	addrAndPacketID := RumorWaitingForAck{
		Address:  pkt.Header.Source,
		PacketID: ack.AckedPacketID,
	}
	m.waitingAckListMutex.Lock()
	_, ok = m.waitingAckList[addrAndPacketID]
	if !ok {
		m.waitingAckListMutex.Unlock()
		//the ack is too late
	} else {
		// the ack is on time
		delete(m.waitingAckList, addrAndPacketID)
		m.waitingAckListMutex.Unlock()
		newMsg, err := m.conf.MessageRegistry.MarshalMessage(ack.Status)
		if err != nil {
			return xerrors.Errorf("error while marshaling status after ack")
		}
		newPkt := transport.Packet{
			Header: pkt.Header,
			Msg:    &newMsg,
		}
		m.conf.MessageRegistry.ProcessPacket(newPkt)
	}
	return nil
}

func (m *Messaging) ExecEmptyMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	_, ok := msg.(*types.EmptyMessage)

	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	return nil
}

func (m *Messaging) ExecPrivateMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	privateMsg, ok := msg.(*types.PrivateMessage)

	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	_, ok = privateMsg.Recipients[m.conf.Socket.GetAddress()]
	if ok {
		newPkt := transport.Packet{
			Header: pkt.Header,
			Msg:    privateMsg.Msg,
		}
		m.conf.MessageRegistry.ProcessPacket(newPkt)
	}

	return nil
}

func (m *Messaging) ExecDataRequestMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	dataRequestMsg, ok := msg.(*types.DataRequestMessage)

	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	store := m.conf.Storage.GetDataBlobStore()
	value := store.Get(dataRequestMsg.Key)
	dataReply := types.DataReplyMessage{
		RequestID: dataRequestMsg.RequestID,
		Key:       dataRequestMsg.Key,
		Value:     value,
	}
	header := transport.NewHeader(m.conf.Socket.GetAddress(), m.conf.Socket.GetAddress(), pkt.Header.Source, 0)
	dataReplyMsg, err := m.conf.MessageRegistry.MarshalMessage(dataReply)

	if err != nil {

		return xerrors.Errorf("error while marshaling packet")
	}
	reply := transport.Packet{
		Header: &header,
		Msg:    &dataReplyMsg,
	}

	go func() {
		m.dataToReply <- reply
	}()

	return nil
}

func (m *Messaging) ExecDataReplyMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	dataReplyMsg, ok := msg.(*types.DataReplyMessage)
	m.dataReceivedMutex.Lock()
	defer m.dataReceivedMutex.Unlock()
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	m.dataReceived[dataReplyMsg.RequestID] = DataKeyValue{
		Key:   dataReplyMsg.Key,
		Value: dataReplyMsg.Value,
	}

	return nil
}

func (m *Messaging) ExecSearchRequestMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	searchReqMsg, ok := msg.(*types.SearchRequestMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	go func() {
		searchRequest := SearchRequest{
			Msg: *searchReqMsg,

			Relay: pkt.Header.RelayedBy,
		}
		m.searchRequest <- searchRequest
	}()
	return nil
}

func (m *Messaging) ExecSearchReplyMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	searchReplyMsg, ok := msg.(*types.SearchReplyMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	m.searchResultMutex.Lock()

	old := m.searchResult[searchReplyMsg.RequestID]
	m.searchResult[searchReplyMsg.RequestID] = append(old, searchReplyMsg.Responses...)
	searchReply := SearchReply{
		Msg:    *searchReplyMsg,
		origin: pkt.Header.Source,
	}
	m.searchResultMutex.Unlock()
	go func() {
		m.searchReply <- searchReply
	}()

	return nil
}

func (m *Messaging) ExecPaxosPrepareMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	prepareMsg, ok := msg.(*types.PaxosPrepareMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	if prepareMsg.Step != m.paxos.Step {
		return nil
	}

	if prepareMsg.ID <= m.paxos.MaxID {
		return nil
	}

	var promiseMsg types.PaxosPromiseMessage
	if m.paxos.accepter.AcceptStatus.status {
		promiseMsg = types.PaxosPromiseMessage{
			Step: prepareMsg.Step,
			ID:   prepareMsg.ID,

			AcceptedID:    m.paxos.accepter.AcceptStatus.acceptedID,
			AcceptedValue: &(m.paxos.accepter.AcceptStatus.acceptedValue),
		}
	} else {
		promiseMsg = types.PaxosPromiseMessage{
			Step: prepareMsg.Step,
			ID:   prepareMsg.ID,
		}
	}

	msgToBroadcast, err := m.conf.MessageRegistry.MarshalMessage(promiseMsg)
	if err != nil {
		return err
	}

	m.paxos.MaxID = prepareMsg.ID
	recipients := make(map[string]struct{})
	recipients[prepareMsg.Source] = struct{}{}
	privateMsg := types.PrivateMessage{
		Recipients: recipients,
		Msg:        &msgToBroadcast,
	}

	go func() {
		m.paxos.accepter.PromiseToBroadcast <- privateMsg
	}()

	return nil
}

func (m *Messaging) ExecPaxosPromiseMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	promiseMsg, ok := msg.(*types.PaxosPromiseMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	if promiseMsg.Step != m.paxos.Step {
		return nil
	}
	m.paxos.proposer.PhaseLock.Lock()
	defer m.paxos.proposer.PhaseLock.Unlock()

	if m.paxos.proposer.Phase != 1 {
		return nil
	}

	m.paxos.proposer.PromisesLock.Lock()
	defer m.paxos.proposer.PromisesLock.Unlock()
	m.paxos.proposer.PromisesReceived++

	if promiseMsg.AcceptedValue != nil {
		if promiseMsg.AcceptedID >= m.paxos.proposer.AcceptedValue.acceptedID {
			m.paxos.proposer.AcceptedValue = PaxosAcceptStatus{
				acceptedID:    promiseMsg.AcceptedID,
				acceptedValue: *promiseMsg.AcceptedValue,
			}
		}
	}

	if m.paxos.proposer.PromisesReceived >= m.conf.PaxosThreshold(m.conf.TotalPeers) {
		m.paxos.proposer.Phase = 2
		m.paxos.proposer.CountLock.Lock()
		m.paxos.proposer.AcceptCount = make(map[string]uint)
		m.paxos.proposer.CountLock.Unlock()
		propose := types.PaxosProposeMessage{
			Step:  m.paxos.Step,
			ID:    m.paxos.proposer.AcceptedValue.acceptedID,
			Value: m.paxos.proposer.AcceptedValue.acceptedValue,
		}
		go func() {
			m.paxos.proposer.Phase2Start <- propose
		}()
	}
	return nil
}

func (m *Messaging) ExecPaxosProposeMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	proposeMsg, ok := msg.(*types.PaxosProposeMessage)
	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	if proposeMsg.Step != m.paxos.Step {
		return nil
	}

	if proposeMsg.ID < m.paxos.MaxID {
		return nil
	}

	var accept types.PaxosAcceptMessage
	if proposeMsg.ID != m.paxos.MaxID {
		return nil
	} else {
		accept = types.PaxosAcceptMessage{
			Step:  proposeMsg.Step,
			ID:    proposeMsg.ID,
			Value: proposeMsg.Value,
		}

		m.paxos.accepter.AcceptStatus = PaxosAcceptStatus{
			status:        true,
			acceptedID:    proposeMsg.ID,
			acceptedValue: proposeMsg.Value,
		}
	}

	go func() {
		m.paxos.accepter.AcceptToBroadcast <- accept
	}()

	return nil
}

func (m *Messaging) ExecPaxosAcceptMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	acceptMsg, ok := msg.(*types.PaxosAcceptMessage)

	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}

	if acceptMsg.Step != m.paxos.Step {
		return nil
	}
	m.paxos.proposer.MsgsPreparedMutex.Lock()
	defer m.paxos.proposer.MsgsPreparedMutex.Unlock()
	_, ok = m.paxos.proposer.MsgsPrepared[acceptMsg.Value.UniqID]
	if m.paxos.proposer.Phase != 2 && ok {
		return nil
	}
	m.paxos.proposer.CountLock.Lock()
	defer m.paxos.proposer.CountLock.Unlock()
	_, ok = m.paxos.proposer.AcceptCount[acceptMsg.Value.UniqID]
	if !ok {
		m.paxos.proposer.AcceptCount[acceptMsg.Value.UniqID] = 1
	} else {
		m.paxos.proposer.AcceptCount[acceptMsg.Value.UniqID]++
	}

	if m.paxos.proposer.AcceptCount[acceptMsg.Value.UniqID] >= uint(m.conf.PaxosThreshold(m.conf.TotalPeers)) {
		go func() {
			m.paxos.proposer.Consensus <- *acceptMsg
		}()
	}
	return nil
}

func (m *Messaging) ExecTLCMessage(msg types.Message, pkt transport.Packet) error {
	// cast the message to its actual type. You assume it is the right type.
	tlc, ok := msg.(*types.TLCMessage)

	if !ok {
		return xerrors.Errorf("wrong type: %T", msg)
	}
	_, ok = m.paxos.blocksReceived[tlc.Step]
	if !ok {
		m.paxos.blocksReceived[tlc.Step] = tlc.Block
	}

	_, ok = m.paxos.TLCMessagesReceived[tlc.Step]
	if !ok {
		m.paxos.TLCMessagesReceived[tlc.Step] = 1
	} else {
		m.paxos.TLCMessagesReceived[tlc.Step]++
	}

	if m.paxos.TLCMessagesReceived[m.paxos.Step] >= uint(m.conf.PaxosThreshold(m.conf.TotalPeers)) {
		err := m.AddBlock(tlc.Block)
		if err != nil {
			fmt.Println(err)
			return err
		}
		store := m.conf.Storage.GetNamingStore()
		store.Set(tlc.Block.Value.Filename, []byte(tlc.Block.Value.Metahash))

		if !m.paxos.HasBroadcastedTLC[tlc.Step] {
			m.paxos.HasBroadcastedTLC[tlc.Step] = true
			go func() {
				m.paxos.TLCToForward <- *tlc
			}()
		}
		m.paxos.proposer.MsgsPreparedMutex.Lock()
		_, ok := m.paxos.proposer.MsgsPrepared[tlc.Block.Value.UniqID]
		m.paxos.proposer.MsgsPreparedMutex.Unlock()
		if ok {
			go func() {
				m.paxos.proposer.TagIsDone <- true
			}()
		}
		m.IncrementStep()

		if m.paxos.TLCMessagesReceived[m.paxos.Step] >= uint(m.conf.PaxosThreshold(m.conf.TotalPeers)) {
			err := m.TLCCatchup()
			if err != nil {
				fmt.Println(err)
				return err
			}
		}
	}

	return nil
}

func (m *Messaging) AddBlock(block types.BlockchainBlock) error {
	store := m.conf.Storage.GetBlockchainStore()

	buf, err := block.Marshal()
	if err != nil {
		return err
	}

	blockHashHex := hex.EncodeToString(block.Hash)
	store.Set(blockHashHex, buf)

	store.Set(storage.LastBlockKey, block.Hash)

	return nil
}

func (m *Messaging) IncrementStep() {
	m.paxos.Step++
	m.paxos.HasBroadcastedTLC = make(map[uint]bool)
	m.paxos.proposer.CountLock.Lock()
	m.paxos.proposer.AcceptCount = make(map[string]uint)
	m.paxos.proposer.CountLock.Unlock()
	m.paxos.proposer.MsgsPreparedMutex.Lock()
	m.paxos.proposer.MsgsPrepared = make(map[string]struct{})
	m.paxos.proposer.MsgsPreparedMutex.Unlock()
	m.paxos.accepter.AcceptStatus = PaxosAcceptStatus{status: false}
	m.paxos.MaxID = 0
}

func (m *Messaging) TLCCatchup() error {
	err := m.AddBlock(m.paxos.blocksReceived[m.paxos.Step])
	if err != nil {
		return err
	}
	store := m.conf.Storage.GetNamingStore()
	store.Set(m.paxos.blocksReceived[m.paxos.Step].Value.Filename, []byte(m.paxos.blocksReceived[m.paxos.Step].Value.Metahash))
	m.IncrementStep()
	if m.paxos.TLCMessagesReceived[m.paxos.Step] >= uint(m.conf.PaxosThreshold(m.conf.TotalPeers)) {
		m.TLCCatchup()
	}
	return nil
}

//find the rumors that status1 has and status2 has not received
func (m *Messaging) MapDifference(status1 types.StatusMessage, status2 types.StatusMessage) []RumorMissing {
	var diff []RumorMissing
	for addr1, seq1 := range status1 {
		found := false
		for addr2, seq2 := range status2 {
			if addr1 == addr2 {
				if seq1 > seq2 {
					for i := seq2 + 1; i <= seq1; i++ {
						diff = append(diff, RumorMissing{addr1, i})
					}
				}
				found = true
			}
		}
		if !found {
			for i := uint(1); i <= seq1; i++ {
				diff = append(diff, RumorMissing{addr1, i})
			}
		}
	}
	return diff
}

func (m *Messaging) SendRumorsMsg(conf peer.Configuration, dest string, pkt transport.Packet, timeout time.Duration) error {
	m.waitingAckListMutex.Lock()
	defer m.waitingAckListMutex.Unlock()
	rumorWaitingForAck := RumorWaitingForAck{
		Address:  dest,
		PacketID: pkt.Header.PacketID,
	}

	m.waitingAckList[rumorWaitingForAck] = pkt

	go func(rumorWaitingForAck RumorWaitingForAck) {
		time.Sleep(conf.AckTimeout)
		m.timeout <- rumorWaitingForAck
	}(rumorWaitingForAck)
	return conf.Socket.Send(dest, pkt, timeout)
}

// Start implements peer.Service
func (n *node) Start() error {
	var pkt transport.Packet
	var err error
	n.started = true
	if n.conf.HeartbeatInterval > 0 {
		n.tickerHeartBeat = time.NewTicker(n.conf.HeartbeatInterval)

		empty := types.EmptyMessage{}
		msg, err := n.conf.MessageRegistry.MarshalMessage(empty)
		if err != nil {
			fmt.Println(err)
		}
		n.Broadcast(msg)

		go func() {
			for {
				select {
				case <-n.finished:
					return
				case <-n.tickerHeartBeat.C:
					n.Broadcast(msg)
				}
			}
		}()
	}

	go func() {
		if n.conf.AntiEntropyInterval > 0 {
			n.tickerAntiEntropy = time.NewTicker(n.conf.AntiEntropyInterval)
		} else {
			n.tickerAntiEntropy = time.NewTicker(time.Second * 1)
			n.tickerAntiEntropy.C = nil
		}
		for {
			select {
			case <-n.finished:
				return
			case tlc := <-n.messaging.paxos.TLCToForward:
				msg, err := n.conf.MessageRegistry.MarshalMessage(tlc)
				if err != nil {
					fmt.Println(err)
					continue
				}
				n.Broadcast(msg)
				continue
			case acceptMsg := <-n.messaging.paxos.proposer.Consensus:
				consensusValue := acceptMsg.Value
				var prev []byte
				if n.messaging.paxos.Step == 0 {
					prev = make([]byte, 32)
				} else {
					blockStore := n.conf.Storage.GetBlockchainStore()
					lastBlock := hex.EncodeToString(blockStore.Get(storage.LastBlockKey))
					prevLastBlock := blockStore.Get(string(lastBlock))
					var unmarshaledBlock types.BlockchainBlock
					unmarshaledBlock.Unmarshal(prevLastBlock)
					prev = unmarshaledBlock.Hash
				}
				toHash := []byte(strconv.Itoa(int(acceptMsg.Step)))
				toHash = append(toHash, []byte(consensusValue.UniqID)...)
				toHash = append(toHash, []byte(consensusValue.Filename)...)
				toHash = append(toHash, []byte(consensusValue.Metahash)...)
				toHash = append(toHash, prev...)

				h := crypto.SHA256.New()
				h.Write([]byte(toHash))
				hash := h.Sum(nil)

				tlc := types.TLCMessage{
					Step: acceptMsg.Step,
					Block: types.BlockchainBlock{
						Index:    acceptMsg.Step,
						Hash:     hash,
						Value:    consensusValue,
						PrevHash: prev,
					},
				}
				msg, err := n.conf.MessageRegistry.MarshalMessage(tlc)
				if err != nil {
					fmt.Println(err)
					continue
				}
				n.messaging.paxos.HasBroadcastedTLC[n.messaging.paxos.Step] = true
				err = n.Broadcast(msg)
				if err != nil {
					fmt.Println(err)
				}
				continue
			case promise := <-n.messaging.paxos.accepter.PromiseToBroadcast:
				promiseMsg, err := n.conf.MessageRegistry.MarshalMessage(promise)
				if err != nil {
					fmt.Println(err)
					continue
				}
				err = n.Broadcast(promiseMsg)
				if err != nil {
					fmt.Println(err)
				}
				continue
			case propose := <-n.messaging.paxos.proposer.Phase2Start:
				proposeMsg, err := n.conf.MessageRegistry.MarshalMessage(propose)
				if err != nil {
					fmt.Println(err)
					continue
				}
				err = n.Broadcast(proposeMsg)
				if err != nil {
					fmt.Println(err)
					continue
				}
				//n.RetryTag(propose.Value.Filename, propose.Value.Metahash)
				continue
			case retry := <-n.messaging.paxos.proposer.Retry:
				n.messaging.paxos.proposer.MsgsPreparedMutex.Lock()
				id := uint(len(n.messaging.paxos.proposer.MsgsPrepared))*n.conf.TotalPeers + n.conf.PaxosID
				n.messaging.paxos.proposer.MsgsPreparedMutex.Unlock()

				prepare := types.PaxosPrepareMessage{
					Step:   n.messaging.paxos.Step,
					ID:     id,
					Source: n.myAddr,
				}
				msg, err := n.conf.MessageRegistry.MarshalMessage(prepare)
				if err != nil {
					continue
				}
				n.messaging.paxos.proposer.PhaseLock.Lock()
				n.messaging.paxos.proposer.Phase = 1
				n.messaging.paxos.proposer.PhaseLock.Unlock()
				n.messaging.paxos.proposer.PromisesLock.Lock()
				n.messaging.paxos.proposer.PromisesReceived = 0
				n.messaging.paxos.proposer.PromisesLock.Unlock()
				n.messaging.paxos.proposer.AcceptedValue = PaxosAcceptStatus{
					acceptedID: id,
					acceptedValue: types.PaxosValue{
						UniqID:   retry.UniqID,
						Filename: retry.Filename,
						Metahash: retry.Metahash,
					},
				}
				n.messaging.paxos.proposer.MsgsPreparedMutex.Lock()
				n.messaging.paxos.proposer.MsgsPrepared[retry.UniqID] = struct{}{}
				n.messaging.paxos.proposer.MsgsPreparedMutex.Unlock()
				//fmt.Println("retrying")
				err = n.Broadcast(msg)
				n.RetryTag(retry.Filename, retry.Metahash)
				if err != nil {
					fmt.Println(err)
					continue
				}
				continue
			case accept := <-n.messaging.paxos.accepter.AcceptToBroadcast:
				acceptMsg, err := n.conf.MessageRegistry.MarshalMessage(accept)
				if err != nil {
					fmt.Println(err)
					continue
				}
				err = n.Broadcast(acceptMsg)
				if err != nil {
					fmt.Println(err)
				}
				continue
			case searchReply := <-n.messaging.searchReply:
				for _, fileInfo := range searchReply.Msg.Responses {
					n.UpdateCatalog(fileInfo.Metahash, searchReply.origin)
					for _, chunk := range fileInfo.Chunks {
						if len(chunk) > 0 {
							n.UpdateCatalog(string(chunk), searchReply.origin)
						}
					}
				}
				continue
			case requestId := <-n.searchAllGetResult:
				n.messaging.searchResultMutex.Lock()
				n.searchAllResult <- n.messaging.searchResult[requestId]
				n.messaging.searchResultMutex.Unlock()
				continue
			case requestId := <-n.searchFirstGetResult:
				n.messaging.searchResultMutex.Lock()
				n.searchFirstResult <- n.messaging.searchResult[requestId]
				n.messaging.searchResultMutex.Unlock()
			case pkt := <-n.messaging.dataToReply:
				n.mapMutex.Lock()
				relay, ok := n.rTable[pkt.Header.Destination]
				n.mapMutex.Unlock()
				if ok {
					n.conf.Socket.Send(relay, pkt, time.Second*1)
				}
				continue
			case searchRequestt := <-n.messaging.searchRequest:
				relay := searchRequestt.Relay
				searchRequest := searchRequestt.Msg
				reg := regexp.MustCompile(searchRequest.Pattern)
				budget := searchRequest.Budget - 1
				matches, err := n.SearchAllLocally(*reg)

				if err != nil {
					fmt.Println(err)
					continue
				}
				if budget > 0 && len(n.neighbors) > 0 {
					_, ok := n.neighbors[searchRequest.Origin]
					var budgets []uint
					if ok {
						budgets = DivideBudgets(len(n.neighbors)-1, budget)
					} else {
						budgets = DivideBudgets(len(n.neighbors), budget)
					}

					neighborNumber := 0
					for dest := range n.neighbors {
						if dest == searchRequest.Origin {
							continue
						}
						if len(budgets) > 0 {
							if budgets[neighborNumber] > 0 {
								go func(neighborNumber int, dest string) {
									searchRequestMsg := types.SearchRequestMessage{
										RequestID: searchRequest.RequestID,
										Origin:    searchRequest.Origin,
										Pattern:   reg.String(),
										Budget:    budgets[neighborNumber],
									}
									msg, err := n.conf.MessageRegistry.MarshalMessage(searchRequestMsg)
									if err != nil {
										fmt.Println(err)
										return
									}
									header := transport.NewHeader(n.myAddr, n.myAddr, dest, 0)
									pkt := transport.Packet{
										Header: &header,
										Msg:    &msg,
									}
									n.conf.Socket.Send(dest, pkt, time.Second*1)
								}(neighborNumber, dest)
							}
						}

						neighborNumber++
					}

				}
				var matchSlice []string

				for key := range matches {
					matchSlice = append(matchSlice, key)
				}
				var reply types.SearchReplyMessage
				if len(matches) != 0 {
					var fileInfos []types.FileInfo
					for _, match := range matchSlice {
						nameStore := n.conf.Storage.GetNamingStore()
						metahash := nameStore.Get(match)
						blobStore := n.conf.Storage.GetDataBlobStore()
						metaFileValue := blobStore.Get(string(metahash))
						var data [][]byte
						if len(metaFileValue) > 0 {
							chunkHexKeys := strings.Split(string(metaFileValue), peer.MetafileSep)
							for _, key := range chunkHexKeys {
								if len(blobStore.Get(key)) != 0 {
									data = append(data, []byte(key))
								} else {
									data = append(data, nil)
								}
							}
						} else {
							continue
						}
						fileInfo := types.FileInfo{
							Name:     match,
							Metahash: string(metahash),
							Chunks:   data,
						}
						fileInfos = append(fileInfos, fileInfo)
					}
					reply = types.SearchReplyMessage{
						RequestID: searchRequest.RequestID,
						Responses: fileInfos,
					}
				} else {
					reply = types.SearchReplyMessage{
						RequestID: searchRequest.RequestID,
						Responses: nil,
					}
				}
				msg, err := n.conf.MessageRegistry.MarshalMessage(reply)
				if err != nil {
					fmt.Println(err)
				}
				header := transport.NewHeader(n.myAddr, n.myAddr, searchRequest.Origin, 0)
				pkt := transport.Packet{
					Header: &header,
					Msg:    &msg,
				}
				n.conf.Socket.Send(relay, pkt, time.Second*1)

				continue
			case requestId := <-n.messaging.checkDataReplyStatus:
				n.messaging.dataReceivedMutex.Lock()
				dataReceived, ok := n.messaging.dataReceived[requestId]
				n.messaging.dataReceivedMutex.Unlock()
				dataReply := DataReply{
					requestID: "",
					status:    false,
					value:     nil,
				}
				if ok {
					dataReply.status = true
					dataReply.requestID = requestId
					dataReply.value = dataReceived.Value
				}
				n.messaging.dataReply <- dataReply
				continue
			case routingEntry := <-n.messaging.queueRoutingEntry:
				n.SetRoutingEntry(routingEntry.Origin, routingEntry.Relay)
			case ackTimeout := <-n.messaging.timeout:

				n.messaging.waitingAckListMutex.Lock()
				pkt, ok := n.messaging.waitingAckList[ackTimeout]
				n.messaging.waitingAckListMutex.Unlock()
				if ok {
					// ack not received
					// retransmitting to someone else
					delete(n.messaging.waitingAckList, ackTimeout)
					n.neighborsMutex.Lock()
					if n.PreconditionGetRandomNeighbor(ackTimeout.Address) {
						dest := n.getRandomNeighbor(ackTimeout.Address)
						newHeader := transport.NewHeader(n.myAddr, n.myAddr, dest, 0)
						newPacket := transport.Packet{
							Header: &newHeader,
							Msg:    pkt.Msg,
						}
						err = n.messaging.SendRumorsMsg(n.conf, n.rTable[dest], newPacket, time.Second*1)
						if err != nil {
							fmt.Println(err)
						}
					}
					n.neighborsMutex.Unlock()
				}
				continue
			// propagating rumors
			case packet := <-n.messaging.queueRumors:
				n.neighborsMutex.Lock()
				if n.PreconditionGetRandomNeighbor(packet.Header.Source) {
					dest := n.getRandomNeighbor(packet.Header.Source)
					newHeader := transport.NewHeader(packet.Header.Source, n.myAddr, dest, 0)
					newPacket := transport.Packet{
						Header: &newHeader,
						Msg:    packet.Msg,
					}
					err = n.messaging.SendRumorsMsg(n.conf, n.rTable[dest], newPacket, time.Second*1)
					if err != nil {
						fmt.Println(err)
					}
				}
				n.neighborsMutex.Unlock()
				continue
			// continue mongering status message
			case packet := <-n.messaging.queueStatus:
				n.mapMutex.Lock()
				n.neighborsMutex.Lock()
				if n.PreconditionGetRandomNeighbor(packet.Header.Source) {
					dest := n.getRandomNeighbor(packet.Header.Source)
					newHeader := transport.NewHeader(n.myAddr, n.myAddr, dest, 0)
					newPacket := transport.Packet{
						Header: &newHeader,
						Msg:    packet.Msg,
					}
					err = n.conf.Socket.Send(n.rTable[dest], newPacket, time.Second*1)
					if err != nil {
						fmt.Println(err)
					}
				}
				n.neighborsMutex.Unlock()
				n.mapMutex.Unlock()
				continue
			// anti entropy mechanism
			case <-n.tickerAntiEntropy.C:
				err := n.AntiEntropy()
				if err != nil {
					fmt.Println(err)
				}
				continue
			// listening
			default:
				pkt, err = n.conf.Socket.Recv(time.Second / 5)
				if errors.Is(err, transport.TimeoutErr(0)) {
					continue
				}
				if err != nil {
					fmt.Println(err)
					continue
				}

				if pkt.Header.Destination == n.myAddr {
					err = n.conf.MessageRegistry.ProcessPacket(pkt)
					if err != nil {
						fmt.Println(err)
					}
				} else {
					// else, forward the packet
					n.mapMutex.Lock()
					dest := pkt.Header.Destination
					relay, ok := n.rTable[dest]
					if !ok {
						fmt.Println(err)
						n.mapMutex.Unlock()
						continue
					}
					header := transport.NewHeader(pkt.Header.Source, n.myAddr, pkt.Header.Destination, pkt.Header.TTL)
					var packet = transport.Packet{Header: &header, Msg: pkt.Msg}
					err = n.conf.Socket.Send(relay, packet, time.Second*1)
					if err != nil {
						fmt.Println(err)
					}
					n.mapMutex.Unlock()
				}
			}
		}
	}()

	return nil
}

func (n *node) PreconditionGetRandomNeighbor(forbiddenReturn string) bool {
	_, ok := n.neighbors[forbiddenReturn]
	if !(len(n.neighbors) == 1 && ok) && (len(n.neighbors) > 0) {
		return true
	}
	return false
}

func (n *node) AntiEntropy() error {
	n.messaging.lastMessageReceivedByNodeMutex.Lock()
	defer n.messaging.lastMessageReceivedByNodeMutex.Unlock()
	n.mapMutex.Lock()
	defer n.mapMutex.Unlock()
	n.neighborsMutex.Lock()
	defer n.neighborsMutex.Unlock()

	if len(n.neighbors) > 0 {
		dest := n.getRandomNeighbor("iwanteverone")
		status := n.messaging.lastMessageReceivedByNode
		header := transport.NewHeader(n.myAddr, n.myAddr, dest, 0)
		msg, err := n.conf.MessageRegistry.MarshalMessage(status)
		if err != nil {
			return xerrors.Errorf("error while marshaling")
		}
		pkt := transport.Packet{
			Header: &header,
			Msg:    &msg,
		}
		return n.conf.Socket.Send(n.rTable[dest], pkt, time.Second*1)
	}
	return nil
}

// Stop implements peer.Service
func (n *node) Stop() error {
	if n.started {
		n.finished <- true
	}
	if n.conf.AntiEntropyInterval > 0 {
		n.tickerAntiEntropy.Stop()
	}
	if n.conf.HeartbeatInterval > 0 {
		n.tickerHeartBeat.Stop()
	}
	return nil
}

// Unicast implements peer.Messaging
func (n *node) Unicast(dest string, msg transport.Message) error {
	n.mapMutex.Lock()
	defer n.mapMutex.Unlock()
	relay, ok := n.rTable[dest]
	if !ok {
		return xerrors.Errorf("destination not found")
	}
	var header = transport.NewHeader(n.myAddr, n.myAddr, dest, 0)
	var packet = transport.Packet{Header: &header, Msg: &msg}
	err := n.conf.Socket.Send(relay, packet, time.Second*1)
	return err
}

func (n *node) Broadcast(msg transport.Message) error {
	n.mapMutex.Lock()
	defer n.mapMutex.Unlock()
	n.messaging.rumorsMutex.Lock()
	defer n.messaging.rumorsMutex.Unlock()
	n.messaging.lastMessageReceivedByNodeMutex.Lock()
	defer n.messaging.lastMessageReceivedByNodeMutex.Unlock()
	n.neighborsMutex.Lock()
	defer n.neighborsMutex.Unlock()
	localHeader := transport.NewHeader(n.myAddr, n.myAddr, n.myAddr, 0)
	localPkt := transport.Packet{
		Header: &localHeader,
		Msg:    &msg,
	}
	err := n.conf.MessageRegistry.ProcessPacket(localPkt)
	if err != nil {
		return xerrors.Errorf("error while processing packet")
	}
	rumor := types.Rumor{Origin: n.myAddr, Sequence: n.counter, Msg: &msg}
	missing := RumorMissing{
		Address:  n.myAddr,
		Sequence: n.counter,
	}
	n.messaging.rumors[missing] = rumor
	n.counter++

	counter, ok := n.messaging.lastMessageReceivedByNode[n.messaging.conf.Socket.GetAddress()]
	if !ok {
		n.messaging.lastMessageReceivedByNode[n.messaging.conf.Socket.GetAddress()] = 1
	} else {
		n.messaging.lastMessageReceivedByNode[n.messaging.conf.Socket.GetAddress()] = counter + 1
	}

	if len(n.neighbors) < 1 {
		return nil
	}
	dest := n.getRandomNeighbor("fakeAdddress")
	remotePkt, err := n.messaging.createRumorsMsg(n.myAddr, n.myAddr, dest, []types.Rumor{rumor})
	if err != nil {
		return xerrors.Errorf("error while creating rumors msg")
	}
	err = n.messaging.SendRumorsMsg(n.conf, dest, remotePkt, time.Second*1)
	return err
}

func (m *Messaging) createRumorsMsg(origin string, relay string, dest string, rumors []types.Rumor) (transport.Packet, error) {
	rumorMsg := types.RumorsMessage{Rumors: rumors}
	remoteHeader := transport.NewHeader(m.conf.Socket.GetAddress(), m.conf.Socket.GetAddress(), dest, 0)
	remoteMsg, err := m.conf.MessageRegistry.MarshalMessage(rumorMsg)
	if err != nil {
		return transport.Packet{}, xerrors.Errorf("error while marshaling packet")
	}
	remotePkt := transport.Packet{
		Header: &remoteHeader,
		Msg:    &remoteMsg,
	}
	return remotePkt, nil
}

// AddPeer implements peer.Service
func (n *node) AddPeer(addr ...string) {
	n.neighborsMutex.Lock()
	defer n.neighborsMutex.Unlock()
	for _, m := range addr {
		if m != n.myAddr {
			n.SetRoutingEntry(m, m)
			n.neighbors[m] = struct{}{}
		}
	}
}

// GetRoutingTable implements peer.Service
func (n *node) GetRoutingTable() peer.RoutingTable {
	n.mapMutex.Lock()
	defer n.mapMutex.Unlock()
	return n.rTable
}

// SetRoutingEntry implements peer.Service
func (n *node) SetRoutingEntry(origin, relayAddr string) {
	n.mapMutex.Lock()
	defer n.mapMutex.Unlock()

	if relayAddr == "" {
		delete(n.rTable, origin)
	} else {
		_, ok := n.neighbors[origin]
		if origin == relayAddr && !ok {
			n.neighbors[origin] = struct{}{}
		}
		if n.rTable[origin] != origin {
			n.rTable[origin] = relayAddr
		}
	}
}

func (n *node) getRandomNeighbor(forbiddenReturn string) string {
	neigh := n.random()
	if neigh == forbiddenReturn {
		return n.getRandomNeighbor(forbiddenReturn)
	}
	return neigh
}

func (n *node) random() string {
	i := rand.Intn(len(n.neighbors))
	for k := range n.neighbors {
		if i == 0 {
			return k
		}
		i--
	}
	panic("error")
}

func (n *node) Upload(data io.Reader) (string, error) {

	blobStore := n.conf.Storage.GetDataBlobStore()

	metaFileValue := ""
	var hashInputBytes []byte
	firstBlockIsHere := false
	for {
		buf := make([]byte, n.conf.ChunkSize)
		nBytes, err := data.Read(buf)
		h := crypto.SHA256.New()
		var actualBuf []byte
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", xerrors.Errorf("error while reading")
		}

		if firstBlockIsHere {
			metaFileValue = metaFileValue + peer.MetafileSep
		}
		firstBlockIsHere = true

		if nBytes < int(n.conf.ChunkSize) {
			actualBuf = buf[:nBytes]
		} else {
			actualBuf = buf
		}

		_, err = h.Write(actualBuf)
		if err != nil {
			return "", xerrors.Errorf("error while writing to the hash function")
		}
		metahashSlice := h.Sum(nil)
		hashInputBytes = append(hashInputBytes, metahashSlice...)
		metahashHex := hex.EncodeToString(metahashSlice)
		metaFileValue = metaFileValue + metahashHex

		blobStore.Set(metahashHex, actualBuf)
	}
	h := crypto.SHA256.New()
	_, err := h.Write(hashInputBytes)
	if err != nil {
		return "", xerrors.Errorf("error while writing to the hash function")
	}
	metahashSlice := h.Sum(nil)
	metahashHex := hex.EncodeToString(metahashSlice)
	blobStore.Set(metahashHex, []byte(metaFileValue))

	return metahashHex, nil
}

func (n *node) Download(metahash string) ([]byte, error) {
	store := n.conf.Storage.GetDataBlobStore()
	n.catalogMutex.Lock()
	defer n.catalogMutex.Unlock()
	// metahash -> metafilevalue
	// split(metafilevalue) -> hash(c0) ... hash(cN)
	var metafile []byte
	if len(store.Get(metahash)) != 0 {
		metafile = store.Get(metahash)
	} else {
		if len(n.catalog[metahash]) != 0 {
			value, err := n.GetDataFromPeer(metahash)
			if err != nil {
				return nil, err
			}
			metafile = value
		} else {
			//nobody has the data
			return nil, xerrors.Errorf("file not found")
		}
	}
	var data []byte
	chunkHexKeys := strings.Split(string(metafile), peer.MetafileSep)
	for _, key := range chunkHexKeys {
		if len(store.Get(key)) != 0 {
			chunkData := store.Get(key)
			data = append(data, chunkData...)
		} else {
			if len(n.catalog[key]) != 0 {
				value, err := n.GetDataFromPeer(key)
				if err != nil {
					return nil, err
				}
				data = append(data, value...)
			}
		}
	}

	return data, nil
}

func (n *node) Tag(name string, mh string) error {
	store := n.messaging.conf.Storage.GetNamingStore()
	check := store.Get(name)
	if check != nil {
		return xerrors.Errorf("name already exists")
	}
	if n.conf.TotalPeers <= 1 {
		store.Set(name, []byte(mh))
	} else {
		n.messaging.paxos.proposer.MsgsPreparedMutex.Lock()
		id := n.conf.PaxosID + n.conf.TotalPeers*uint(len(n.messaging.paxos.proposer.MsgsPrepared))
		n.messaging.paxos.proposer.MsgsPreparedMutex.Unlock()
		prepareMsg := types.PaxosPrepareMessage{
			Step:   0,
			ID:     id,
			Source: n.myAddr,
		}

		prepare, err := n.conf.MessageRegistry.MarshalMessage(prepareMsg)
		if err != nil {
			return xerrors.Errorf("failed to marshall prepare message")
		}
		n.messaging.paxos.proposer.PhaseLock.Lock()
		n.messaging.paxos.proposer.Phase = 1
		n.messaging.paxos.proposer.PhaseLock.Unlock()
		n.messaging.paxos.proposer.PromisesLock.Lock()
		n.messaging.paxos.proposer.PromisesReceived = 0
		n.messaging.paxos.proposer.PromisesLock.Unlock()
		n.messaging.paxos.proposer.CountLock.Lock()
		n.messaging.paxos.proposer.AcceptCount = make(map[string]uint)
		n.messaging.paxos.proposer.CountLock.Unlock()
		uniqId := xid.New().String()
		n.messaging.paxos.proposer.AcceptedValue = PaxosAcceptStatus{
			acceptedID: id,
			acceptedValue: types.PaxosValue{
				UniqID:   uniqId,
				Filename: name,
				Metahash: mh,
			},
		}

		err = n.Broadcast(prepare)
		n.messaging.paxos.proposer.MsgsPreparedMutex.Lock()
		n.messaging.paxos.proposer.MsgsPrepared[uniqId] = struct{}{}
		n.messaging.paxos.proposer.MsgsPreparedMutex.Unlock()
		//fmt.Println("retry tag 1")
		n.RetryTag(name, mh)
		<-n.messaging.paxos.proposer.TagIsDone

		return err
	}
	return nil
}

func (n *node) RetryTag(name string, mh string) {
	go func() {
		time.Sleep(n.conf.PaxosProposerRetry)
		store := n.conf.Storage.GetNamingStore()
		val := store.Get(name)
		if val != nil {
			return
		}

		n.messaging.paxos.proposer.Retry <- types.PaxosValue{
			UniqID:   xid.New().String(),
			Filename: name,
			Metahash: mh,
		}
	}()
}

func (n *node) Resolve(name string) string {
	return string(n.conf.Storage.GetNamingStore().Get(name))
}

func (n *node) GetCatalog() peer.Catalog {
	n.catalogMutex.Lock()
	defer n.catalogMutex.Unlock()
	return n.catalog
}

func (n *node) UpdateCatalog(key string, peer string) {
	n.catalogMutex.Lock()
	defer n.catalogMutex.Unlock()
	if n.catalog[key] == nil {
		n.catalog[key] = make(map[string]struct{})
	}
	n.catalog[key][peer] = struct{}{}
}

func (n *node) SearchAll(reg regexp.Regexp, budget uint, timeout time.Duration) ([]string, error) {
	n.neighborsMutex.Lock()
	matches, err := n.SearchAllLocally(reg)
	if err != nil {
		n.neighborsMutex.Unlock()
		return nil, err
	}
	numberOfNeighbors := len(n.neighbors)
	if numberOfNeighbors != 0 {
		budgets := DivideBudgets(numberOfNeighbors, budget)
		requestId := xid.New().String()
		neighborNumber := 0
		for dest := range n.neighbors {
			go func(neighborNumber int, dest string) {
				searchRequestMsg := types.SearchRequestMessage{
					RequestID: requestId,
					Origin:    n.myAddr,
					Pattern:   reg.String(),
					Budget:    budgets[neighborNumber],
				}
				msg, err := n.conf.MessageRegistry.MarshalMessage(searchRequestMsg)
				if err != nil {
					return
				}
				header := transport.NewHeader(n.myAddr, n.myAddr, dest, 0)
				pkt := transport.Packet{
					Header: &header,
					Msg:    &msg,
				}
				err = n.conf.Socket.Send(dest, pkt, time.Second*1)
				if err != nil {
					fmt.Println(err)
				}
			}(neighborNumber, dest)
			neighborNumber++
		}
		n.neighborsMutex.Unlock()
		time.Sleep(timeout)

		n.searchAllGetResult <- requestId
		filesInfos := <-n.searchAllResult

		for _, fileInfo := range filesInfos {
			matches[fileInfo.Name] = struct{}{}
			n.Tag(fileInfo.Name, fileInfo.Metahash)
		}
	} else {
		n.neighborsMutex.Unlock()
	}

	var sliceToRet []string

	for key := range matches {
		sliceToRet = append(sliceToRet, key)
	}

	return sliceToRet, nil
}

func (n *node) SearchFirst(pattern regexp.Regexp, conf peer.ExpandingRing) (string, error) {
	matches, err := n.SearchAllLocally(pattern)
	if err != nil {
		return "", err
	}

	for key := range matches {
		if n.CheckIfAllChunksAreHere(key) {
			return key, nil
		}
	}
	budget := conf.Initial
	if len(n.neighbors) != 0 {
		requestId := xid.New().String()
		for i := 0; i < int(conf.Retry); i++ {
			dest := n.getRandomNeighbor(n.myAddr)
			go func(dest string, budget uint) {
				searchRequestMsg := types.SearchRequestMessage{
					RequestID: requestId,
					Origin:    n.myAddr,
					Pattern:   pattern.String(),
					Budget:    budget,
				}
				msg, err := n.conf.MessageRegistry.MarshalMessage(searchRequestMsg)
				if err != nil {
					return
				}
				header := transport.NewHeader(n.myAddr, n.myAddr, dest, 0)
				pkt := transport.Packet{
					Header: &header,
					Msg:    &msg,
				}
				n.conf.Socket.Send(dest, pkt, time.Second*1)
			}(dest, budget)
			budget *= conf.Factor
			time.Sleep(conf.Timeout)
			n.searchFirstGetResult <- requestId
			filesInfos := <-n.searchFirstResult
			for _, fileInfo := range filesInfos {
				n.Tag(fileInfo.Name, fileInfo.Metahash)
				complete := true
				for _, chunk := range fileInfo.Chunks {
					if len(chunk) == 0 {
						complete = false
						break
					}
				}
				if complete {
					return fileInfo.Name, nil
				}
			}
		}
	}
	return "", nil
}

func (n *node) GetDataFromPeer(hash string) ([]byte, error) {
	keys := make([]string, 0, len(n.catalog[hash]))
	for k := range n.catalog[hash] {
		keys = append(keys, k)
	}
	peer := keys[rand.Intn(len(keys))]
	n.mapMutex.Lock()
	_, ok := n.rTable[peer]
	n.mapMutex.Unlock()
	if !ok {
		//the node do not know the peer
		return nil, xerrors.Errorf("peer not found")
	} else {
		value, err := n.SendDataRequestMsg(hash, peer)
		if err != nil {
			return nil, err
		} else {
			return value, nil
		}
	}
}

func (n *node) SendDataRequestMsg(hash string, dest string) ([]byte, error) {
	header := transport.NewHeader(n.myAddr, n.myAddr, dest, 0)
	requestId := xid.New().String()
	dataRequestMsg := types.DataRequestMessage{
		RequestID: requestId,
		Key:       hash,
	}
	msg, err := n.conf.MessageRegistry.MarshalMessage(dataRequestMsg)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal data request message")
	}
	pkt := transport.Packet{
		Header: &header,
		Msg:    &msg,
	}
	n.mapMutex.Lock()
	n.conf.Socket.Send(n.rTable[dest], pkt, time.Second*1)
	n.mapMutex.Unlock()

	delay := n.conf.BackoffDataRequest.Initial
	for i := 0; i < int(n.conf.BackoffDataRequest.Retry); i++ {
		n.messaging.checkDataReplyStatus <- requestId
		dataReply := <-n.messaging.dataReply
		if !dataReply.status {
			time.Sleep(delay)
			delay *= time.Duration(n.conf.BackoffDataRequest.Factor)
			continue
		}
		return dataReply.value, nil
	}

	return nil, xerrors.Errorf("no data received")
}

func (n *node) getAllFileNames(key string, val []byte) bool {
	n.fileNamesSliceTemp = append(n.fileNamesSliceTemp, key)
	return true
}

func (n *node) SearchAllLocally(reg regexp.Regexp) (map[string]struct{}, error) {
	store := n.conf.Storage.GetNamingStore()
	store.ForEach(n.getAllFileNames)
	matches := make(map[string]struct{})
	for _, val := range n.fileNamesSliceTemp {
		res := reg.FindString(val)
		if res != "" {
			matches[res] = struct{}{}
		}
	}

	return matches, nil
}

func SelectSubset(sizeSet int, toSelect int) []int {
	p := rand.Perm(sizeSet)
	return p[:toSelect]
}

func DivideBudgets(numberOfNeighbors int, budget uint) []uint {
	var budgets []uint
	if numberOfNeighbors == 0 {
		return nil
	}
	for i := 0; i < numberOfNeighbors; i++ {
		budgets = append(budgets, 0)
	}
	if budget/uint(numberOfNeighbors) > 0 {
		basis := budget / uint(numberOfNeighbors)
		for i := 0; i < numberOfNeighbors; i++ {
			budgets[i] = basis
		}
		budget -= basis * uint(numberOfNeighbors)
	}
	if budget > 0 {
		subset := SelectSubset(numberOfNeighbors, int(budget))

		for _, i := range subset {
			budgets[i]++
		}
	}
	return budgets
}

func (n *node) CheckIfAllChunksAreHere(filename string) bool {
	nameStore := n.conf.Storage.GetNamingStore()
	blobStore := n.conf.Storage.GetDataBlobStore()
	mh := nameStore.Get(filename)
	if mh != nil {
		metafile := blobStore.Get(string(mh))
		if metafile != nil {
			chunkHexKeys := strings.Split(string(metafile), peer.MetafileSep)
			missing := false
			for _, chunkHexKey := range chunkHexKeys {
				if blobStore.Get(chunkHexKey) == nil {
					missing = true
					break
				}
			}
			if !missing {
				return true
			}
		}
	}
	return false
}
