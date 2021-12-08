package impl

import (
	"sync"
	"time"

	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

type proposer struct {
	phase lockedPhase

	// Phase 1
	collectedPromiseCounter safeCounter
	acceptedID              uint
	acceptedValue           *types.PaxosValue

	// Phase 2
	acceptCounters safeCounters

	// Preserve channel between step so that previous step messages can be drained
	collectPromisesSuccess chan struct{}
	collectAcceptsResult   chan types.PaxosValue
}

type lockedPhase struct {
	sync.Mutex
	v uint
}

func (p *lockedPhase) get() uint {
	p.Lock()
	defer p.Unlock()
	return p.v
}

func (p *lockedPhase) set(v uint) {
	p.Lock()
	defer p.Unlock()
	p.v = v
}

type safeCounters struct {
	sync.Mutex
	v map[string]uint
}

func (c *safeCounters) reset() {
	c.Lock()
	defer c.Unlock()
	c.v = make(map[string]uint)
}

func (c *safeCounters) incr(id string) {
	c.Lock()
	defer c.Unlock()
	_, ok := c.v[id]
	if ok {
		c.v[id]++
	} else {
		c.v[id] = 1
	}
}

func (c *safeCounters) get(id string) uint {
	c.Lock()
	defer c.Unlock()
	return c.v[id]
}

func (p *proposer) proposal(value *types.PaxosValue, n *node) (*types.PaxosValue, error) {

	// Actual value accepted
	var result *types.PaxosValue
	currentID := n.conf.PaxosID
	proposeSuccess := false
	var err error

	entryStep := n.paxos.status.tlcCurrent()
	for !proposeSuccess {
		proposedID := currentID
		proposedValue := value

		prepareSuccess := false
		p.phase.set(1)
		for !prepareSuccess {
			p.acceptedID = 0
			p.acceptedValue = nil
			proposedID = currentID

			if entryStep != n.paxos.status.tlcCurrent() {
				return nil, nil
			}
			prepareSuccess, err = p.prepare(currentID, value, n)
			if err != nil {
				return nil, xerrors.Errorf("failed prepare: %v", err)
			}
			// Value receive in promise used as proposition if there is one
			if p.acceptedValue != nil {
				proposedID = p.acceptedID
				proposedValue = p.acceptedValue
			}
			currentID = currentID + n.conf.TotalPeers
		}
		p.phase.set(2)
		if entryStep != n.paxos.status.tlcCurrent() {
			return nil, nil
		}
		result, proposeSuccess, err = p.propose(proposedID, proposedValue, n)
		if err != nil {
			return nil, xerrors.Errorf("failed to propose: %v", err)
		}
		currentID = currentID + n.conf.TotalPeers
	}
	p.phase.set(0)

	return result, nil
}

func (p *proposer) prepare(id uint, value *types.PaxosValue, n *node) (bool, error) {
	err := p.sendPrepare(id, n)
	if err != nil {
		return false, xerrors.Errorf("failed to send prepare message: %v", err)
	}
	prepareSuccess := p.waitForPrepareResult(n)

	return prepareSuccess, nil
}

func (p *proposer) propose(id uint, value *types.PaxosValue, n *node) (*types.PaxosValue, bool, error) {
	err := p.sendPropose(id, *value, n)
	if err != nil {
		return nil, false, xerrors.Errorf("failed to send propose message: %v", err)
	}
	acceptedValue := p.waitForProposeResult(n)

	if acceptedValue == nil {
		return nil, false, nil
	}
	return acceptedValue, true, nil
}

func (p *proposer) collectPromise(promise types.PaxosPromiseMessage, n *node) {
	threshold := uint(n.conf.PaxosThreshold(n.conf.TotalPeers))

	if n.paxos.status.tlcIs(promise.Step) {
		if p.phase.get() != 1 {
			return
		}
		p.collectedPromiseCounter.incr()
		if promise.AcceptedValue != nil {
			if promise.AcceptedID > p.acceptedID {
				p.acceptedID = promise.AcceptedID
				p.acceptedValue = promise.AcceptedValue
			}
		}
	}
	if p.collectedPromiseCounter.get() >= threshold {
		go func() {
			p.collectPromisesSuccess <- struct{}{}
		}()
	}
}

func (p *proposer) waitForPrepareResult(n *node) bool {
	t := time.NewTimer(n.conf.PaxosProposerRetry)
	select {
	case <-p.collectPromisesSuccess:

		p.collectedPromiseCounter.reset()
		t.Stop()
		return true
	case <-t.C:
		p.collectedPromiseCounter.reset()
		return false
	}
}

func (p *proposer) collectAccept(accept types.PaxosAcceptMessage, n *node) {
	threshold := uint(n.conf.PaxosThreshold(n.conf.TotalPeers))

	if n.paxos.status.tlcIs(accept.Step) {
		if p.phase.get() != 2 {
			return
		}
		total := uint(0)
		p.acceptCounters.incr(accept.Value.UniqID)
		total = p.acceptCounters.get(accept.Value.UniqID)
		if total >= threshold {
			go func() {
				p.collectAcceptsResult <- accept.Value
			}()
		}
	}
}

// Non-nil result => success
func (p *proposer) waitForProposeResult(n *node) *types.PaxosValue {
	t := time.NewTimer(n.conf.PaxosProposerRetry)
	select {
	case consensusResult := <-p.collectAcceptsResult:

		p.acceptCounters.reset()
		t.Stop()
		return &consensusResult
	case <-t.C:
		p.acceptCounters.reset()
		t.Stop()
		return nil
	}
}

// Send a prepare message for id (for the current step)
func (p *proposer) sendPrepare(id uint, n *node) error {
	self := n.conf.Socket.GetAddress()

	paxosPrepare := types.PaxosPrepareMessage{
		Step:   n.paxos.status.tlcCurrent(),
		ID:     id,
		Source: self,
	}
	paxosPrepareMsg, err := n.conf.MessageRegistry.MarshalMessage(&paxosPrepare)
	if err != nil {
		return xerrors.Errorf("failed to marshal paxos prepare message: %v", err)
	}
	err = n.Broadcast(paxosPrepareMsg)
	if err != nil {
		return xerrors.Errorf("failed to broadcast paxos prepare message: %v", err)
	}

	return nil
}

// Send a propose message for id (for the current step)
func (p *proposer) sendPropose(id uint, value types.PaxosValue, n *node) error {
	paxosPropose := types.PaxosProposeMessage{
		Step:  n.paxos.status.tlcCurrent(),
		ID:    id,
		Value: value,
	}
	paxosProposeMsg, err := n.conf.MessageRegistry.MarshalMessage(&paxosPropose)
	if err != nil {
		return xerrors.Errorf("failed to marshal paxos prepare message: %v", err)
	}
	err = n.Broadcast(paxosProposeMsg)
	if err != nil {
		return xerrors.Errorf("failed to broadcast paxos propose message: %v", err)
	}
	return nil
}
