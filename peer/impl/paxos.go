package impl

import (
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/rs/xid"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/transport"
	"go.dedis.ch/cs438/types"
)

type Paxos struct {
	proposer *PaxosProposer
	accepter PaxosAccepter

	Step  uint
	MaxID uint

	TLCMessagesReceived map[uint]uint
	HasBroadcastedTLC   map[uint]bool
	blocksReceived      map[uint]types.BlockchainBlock
	prepareMsg          chan transport.Message
}

type PaxosProposer struct {
	PhaseLock         sync.Mutex
	Phase             uint
	PromisesReceived  int
	PromisesLock      sync.Mutex
	AcceptedValue     PaxosAcceptStatus
	Retry             chan types.PaxosValue
	CountLock         sync.Mutex
	AcceptCount       map[string]uint
	MsgsPrepared      map[string]struct{}
	MsgsPreparedMutex sync.Mutex
	TagIsDone         chan bool
}

type PaxosAccepter struct {
	AcceptStatus PaxosAcceptStatus
}

type PaxosAcceptStatus struct {
	status        bool
	acceptedID    uint
	acceptedValue types.PaxosValue
}

func (n *node) RetryTag(name string, mh string) {
	go func() {
		time.Sleep(n.conf.PaxosProposerRetry)
		store := n.conf.Storage.GetNamingStore()
		val := store.Get(name)
		if val != nil {
			return
		}

		retry := types.PaxosValue{
			UniqID:   xid.New().String(),
			Filename: name,
			Metahash: mh,
		}

		n.paxos.proposer.MsgsPreparedMutex.Lock()
		id := uint(len(n.paxos.proposer.MsgsPrepared))*n.conf.TotalPeers + n.conf.PaxosID
		n.paxos.proposer.MsgsPreparedMutex.Unlock()

		prepare := types.PaxosPrepareMessage{
			Step:   n.paxos.Step,
			ID:     id,
			Source: n.conf.Socket.GetAddress(),
		}
		msg, err := n.conf.MessageRegistry.MarshalMessage(prepare)
		if err == nil {

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
		}
	}()
}

func (n *node) AddBlock(block types.BlockchainBlock) error {
	store := n.conf.Storage.GetBlockchainStore()

	buf, err := block.Marshal()
	if err != nil {
		return err
	}

	blockHashHex := hex.EncodeToString(block.Hash)
	store.Set(blockHashHex, buf)

	store.Set(storage.LastBlockKey, block.Hash)

	return nil
}

func (n *node) IncrementStep() {
	n.paxos.Step++
	n.paxos.HasBroadcastedTLC = make(map[uint]bool)
	n.paxos.proposer.CountLock.Lock()
	n.paxos.proposer.AcceptCount = make(map[string]uint)
	n.paxos.proposer.CountLock.Unlock()
	n.paxos.proposer.MsgsPreparedMutex.Lock()
	n.paxos.proposer.MsgsPrepared = make(map[string]struct{})
	n.paxos.proposer.MsgsPreparedMutex.Unlock()
	n.paxos.accepter.AcceptStatus = PaxosAcceptStatus{status: false}
	n.paxos.MaxID = 0
}

func (n *node) TLCCatchup() error {
	err := n.AddBlock(n.paxos.blocksReceived[n.paxos.Step])
	if err != nil {
		return err
	}
	store := n.conf.Storage.GetNamingStore()
	store.Set(n.paxos.blocksReceived[n.paxos.Step].Value.Filename, []byte(n.paxos.blocksReceived[n.paxos.Step].Value.Metahash))
	n.IncrementStep()
	if n.paxos.TLCMessagesReceived[n.paxos.Step] >= uint(n.conf.PaxosThreshold(n.conf.TotalPeers)) {
		n.TLCCatchup()
	}
	return nil
}
