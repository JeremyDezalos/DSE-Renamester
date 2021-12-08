package impl

import (
	"crypto"
	"encoding/hex"
	"strconv"
	"sync"

	"github.com/rs/xid"
	"go.dedis.ch/cs438/storage"
	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

type paxosStatus struct {
	sync.Mutex
	tlc     uint
	running bool
}

func (s *paxosStatus) incrTLC() {
	s.Lock()
	defer s.Unlock()
	s.tlc++
}

func (s *paxosStatus) tlcIs(v uint) bool {
	s.Lock()
	defer s.Unlock()
	return s.tlc == v
}

func (s *paxosStatus) tlcCurrent() uint {
	s.Lock()
	defer s.Unlock()
	return s.tlc
}

func (s *paxosStatus) aquireInstance() bool {
	s.Lock()
	defer s.Unlock()
	if s.running {
		return false
	}
	s.running = true
	return s.running
}

func (s *paxosStatus) releaseInstance() {
	s.Lock()
	defer s.Unlock()
	s.running = false
}

func (s *paxosStatus) instanceRunning() bool {
	s.Lock()
	defer s.Unlock()
	return s.running
}

type multiPaxos struct {
	status paxosStatus
	acceptor
	proposer
	tlcStore
	waitingNextStep chanList

	acceptCounters map[string]uint
}

type tlcStore struct {
	sync.Mutex
	blocks map[uint]*blockState
}

// State of a block for one step of TLC
type blockState struct {
	counter uint
	sent    bool
	block   types.BlockchainBlock
}

type chanList struct {
	sync.Mutex
	waitingList []chan types.PaxosValue
}

func (c *chanList) add() chan types.PaxosValue {
	c.Lock()
	defer c.Unlock()
	newChannel := make(chan types.PaxosValue)
	c.waitingList = append(c.waitingList, newChannel)
	return newChannel
}

func (c *chanList) notifyAll(result types.PaxosValue) {
	c.Lock()
	defer c.Unlock()
	for _, channel := range c.waitingList {
		go func(c chan types.PaxosValue) {
			c <- result
		}(channel)
	}
	c.waitingList = make([]chan types.PaxosValue, 0)
}

func initMultiPaxos(n *node) *multiPaxos {

	a := acceptor{
		maxID:         0,
		acceptedID:    0,
		acceptedValue: nil,
	}

	p := proposer{
		phase:                   lockedPhase{v: 0},
		collectedPromiseCounter: safeCounter{v: 0},
		acceptedID:              0,
		acceptedValue:           nil,
		acceptCounters:          safeCounters{v: make(map[string]uint)},
		collectPromisesSuccess:  make(chan struct{}),
		collectAcceptsResult:    make(chan types.PaxosValue),
	}
	paxos := multiPaxos{
		acceptor:       a,
		proposer:       p,
		acceptCounters: make(map[string]uint),
	}
	paxos.status = paxosStatus{
		tlc:     0,
		running: false,
	}
	paxos.tlcStore = tlcStore{
		blocks: make(map[uint]*blockState),
	}
	paxos.waitingNextStep = chanList{
		waitingList: make([]chan types.PaxosValue, 0),
	}
	return &paxos
}

func (mp *multiPaxos) tryPropose(name string, mh string, n *node) (bool, error) {

	instanceAquired := mp.status.aquireInstance()
	if instanceAquired {
		// Do actual proposal
		proposedValue := types.PaxosValue{
			UniqID:   xid.New().String(),
			Filename: name,
			Metahash: mh,
		}
		result, err := mp.proposer.proposal(&proposedValue, n)
		if err != nil {
			mp.status.releaseInstance()
			return false, xerrors.Errorf("proposal failed : %v", err)
		}
		if result == nil {
			mp.status.releaseInstance()
			return true, nil
		}
		block, err := newBlock(result, n)
		if err != nil {
			mp.status.releaseInstance()
			return false, xerrors.Errorf("failed to add block (proposer): %v", err)
		}

		// INDICATE WE BROADCASTED THIS STEP
		err = mp.broadcastTLC(n.paxos.status.tlcCurrent(), *block, n)
		if err != nil {
			mp.status.releaseInstance()
			return false, xerrors.Errorf("failed to broadcast TLC (proposer): %v", err)
		}
	}
	return instanceAquired, nil
}

func newBlock(value *types.PaxosValue, n *node) (*types.BlockchainBlock, error) {
	blockchain := n.conf.Storage.GetBlockchainStore()
	lastHash := blockchain.Get(storage.LastBlockKey)
	prevHash := make([]byte, 32)
	if lastHash != nil {
		prevHash = lastHash
	}

	preH := []byte(strconv.Itoa(int(n.paxos.status.tlcCurrent())))
	preH = append(preH, []byte(value.UniqID)...)
	preH = append(preH, []byte(value.Filename)...)
	preH = append(preH, []byte(value.Metahash)...)
	preH = append(preH, prevHash...)
	h := crypto.SHA256.New()
	_, err := h.Write(preH)
	if err != nil {
		return nil, xerrors.Errorf("failed to hash new block %v", err)
	}
	hash := h.Sum(nil)
	block := types.BlockchainBlock{
		Index:    n.paxos.status.tlcCurrent(),
		Hash:     hash,
		Value:    *value,
		PrevHash: prevHash,
	}
	return &block, nil
}

func (mp *multiPaxos) broadcastTLC(step uint, block types.BlockchainBlock, n *node) error {
	alreadyBroadcasted := mp.blockBroadcasted(step, block)

	if !alreadyBroadcasted {

		tlc := types.TLCMessage{
			Step:  n.paxos.status.tlcCurrent(),
			Block: block,
		}
		tlcMsg, err := n.conf.MessageRegistry.MarshalMessage(&tlc)
		if err != nil {
			return xerrors.Errorf("failed to marshal tlc message: %v", err)
		}
		err = n.Broadcast(tlcMsg)
		if err != nil {
			log.Warn().Msgf("failed to broadcast tlc message: %v", err)
		}
	}
	return nil
}

func (mp *multiPaxos) endInstance(step uint, block *types.BlockchainBlock) {

	// Reset acceptor/proposer
	mp.acceptor.reset()

	// Notify waiting result (tag)
	mp.waitingNextStep.notifyAll(block.Value)

	// Unlock instance
	mp.status.releaseInstance()
}

func (mp *multiPaxos) waitInstanceResult() types.PaxosValue {
	waitingChan := mp.waitingNextStep.add()
	result := <-waitingChan
	return result
}

func (mp *multiPaxos) collectTLC(tlcMessage types.TLCMessage, n *node) error {

	if tlcMessage.Step < mp.status.tlcCurrent() {
		return nil
	}
	mp.tlcStore.addBlock(tlcMessage.Step, tlcMessage.Block)
	threshold := uint(n.conf.PaxosThreshold(n.conf.TotalPeers))
	block, ready := mp.tlcStore.stepReady(mp.status.tlcCurrent(), threshold)
	catchingUp := false

	runningInstance := mp.status.tlcCurrent()
	runningInstanceBlock := block
	for ready {

		err := blockchainAppend(*block, n)
		if err != nil {
			return xerrors.Errorf("failed to append block: %v", err)
		}
		err = addToNameStore(*block, n)
		if err != nil {
			return xerrors.Errorf("failed to add name: %v", err)
		}

		if !catchingUp {
			err = mp.broadcastTLC(mp.status.tlcCurrent(), *block, n)
			if err != nil {
				return xerrors.Errorf("failed to broadcast TLC (TLC threshold): %v", err)
			}
		}

		mp.status.incrTLC()
		mp.acceptCounters = make(map[string]uint)

		// Remove block info for previous block
		mp.tlcStore.removeBlock(mp.status.tlcCurrent() - 1)
		block, ready = mp.tlcStore.stepReady(mp.status.tlcCurrent(), threshold)
		catchingUp = true
	}
	//  if we advanced in clock: END INSTANCE
	if !mp.status.tlcIs(runningInstance) {
		mp.endInstance(runningInstance, runningInstanceBlock)
	}
	return nil
}

func (mp *multiPaxos) collectAccept(accept types.PaxosAcceptMessage, n *node) error {

	threshold := n.conf.PaxosThreshold(n.conf.TotalPeers)
	if mp.status.tlcIs(accept.Step) {

		total := uint(0)
		_, ok := mp.acceptCounters[accept.Value.UniqID]
		if ok {
			mp.acceptCounters[accept.Value.UniqID]++
		} else {
			mp.acceptCounters[accept.Value.UniqID] = 1
		}
		total = mp.acceptCounters[accept.Value.UniqID]
		if total >= uint(threshold) {
			block, err := newBlock(&accept.Value, n)
			if err != nil {
				return xerrors.Errorf("failed to add block (peer): %v", err)
			}

			err = mp.broadcastTLC(accept.Step, *block, n)
			if err != nil {
				return xerrors.Errorf("failed to broadcast TLC (peer): %v", err)
			}
		}
	}
	return nil
}

func blockchainAppend(block types.BlockchainBlock, n *node) error {
	blockchain := n.conf.Storage.GetBlockchainStore()
	blockData, err := block.Marshal()
	if err != nil {
		return xerrors.Errorf("failed to marshal new block: %v", err)
	}
	blockchain.Set(hex.EncodeToString(block.Hash), blockData)
	blockchain.Delete(storage.LastBlockKey)
	blockchain.Set(storage.LastBlockKey, block.Hash)

	return nil
}

func addToNameStore(block types.BlockchainBlock, n *node) error {
	naming := n.conf.Storage.GetNamingStore()

	// Set the name/metahash association in the name store
	if naming.Get(block.Value.Filename) != nil {
		return xerrors.Errorf("name already in the name store")
	}
	naming.Set(block.Value.Filename, []byte(block.Value.Metahash))
	return nil
}

// Add block to tlcStore
func (tlcStore *tlcStore) addBlock(step uint, block types.BlockchainBlock) {
	tlcStore.Lock()
	defer tlcStore.Unlock()
	currentBlockState, ok := tlcStore.blocks[step]
	if !ok {
		tlcStore.blocks[step] = &blockState{
			counter: 1,
			sent:    false,
			block:   block,
		}
	} else {
		currentBlockState.counter++
	}
}

func (tlcStore *tlcStore) blockBroadcasted(step uint, block types.BlockchainBlock) bool {
	tlcStore.Lock()
	defer tlcStore.Unlock()
	currentBlockState, ok := tlcStore.blocks[step]
	broadcasted := false
	if !ok {
		tlcStore.blocks[step] = &blockState{
			counter: 0,
			sent:    true,
			block:   block,
		}
	} else {
		broadcasted = currentBlockState.sent
		currentBlockState.sent = true
	}
	return broadcasted
}

// Remove block from tlcStore
func (tlcStore *tlcStore) removeBlock(step uint) {
	tlcStore.Lock()
	defer tlcStore.Unlock()
	delete(tlcStore.blocks, step)
}

// Return the block for as step and if we have reached the threshold
func (tlcStore *tlcStore) stepReady(step uint, threshold uint) (*types.BlockchainBlock, bool) {
	tlcStore.Lock()
	defer tlcStore.Unlock()
	blockState, ok := tlcStore.blocks[step]
	if ok {
		return &blockState.block, blockState.counter >= threshold
	}
	return nil, false
}
