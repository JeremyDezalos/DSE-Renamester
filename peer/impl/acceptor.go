package impl

import (
	"sync"

	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

type acceptor struct {
	sync.Mutex
	maxID         uint
	acceptedID    uint
	acceptedValue *types.PaxosValue
}

func (a *acceptor) reset() {
	a.Lock()
	a.maxID = 0
	a.acceptedID = 0
	a.acceptedValue = nil
	a.Unlock()
}

func (a *acceptor) promise(prepare types.PaxosPrepareMessage, n *node) error {

	if !n.paxos.status.tlcIs(prepare.Step) {
		return nil
	}
	a.Lock()
	if prepare.ID <= a.maxID {
		a.Unlock()
		return nil
	}
	a.maxID = prepare.ID
	paxosPromise := types.PaxosPromiseMessage{
		Step:          prepare.Step,
		ID:            prepare.ID,
		AcceptedID:    a.acceptedID,
		AcceptedValue: a.acceptedValue,
	}
	a.Unlock()
	recipients := make(map[string]struct{})
	recipients[prepare.Source] = struct{}{}
	paxosPromiseMsg, err := n.conf.MessageRegistry.MarshalMessage(&paxosPromise)
	if err != nil {
		return xerrors.Errorf("failed to marshal paxos promise message: %v", err)
	}
	privatePromise := types.PrivateMessage{
		Recipients: recipients,
		Msg:        &paxosPromiseMsg,
	}
	privatePromiseMsg, err := n.conf.MessageRegistry.MarshalMessage(&privatePromise)
	if err != nil {
		return xerrors.Errorf("failed to marshal paxos promise message: %v", err)
	}
	err = n.Broadcast(privatePromiseMsg)
	if err != nil {
		return xerrors.Errorf("failed to broadcast paxos promise message: %v", err)
	}

	return nil
}

func (a *acceptor) accept(propose types.PaxosProposeMessage, n *node) error {

	if !n.paxos.status.tlcIs(propose.Step) {
		return nil
	}
	a.Lock()
	if propose.ID != a.maxID {
		a.Unlock()
		return nil
	}
	a.acceptedID = propose.ID
	a.acceptedValue = &propose.Value
	paxosAccept := types.PaxosAcceptMessage{
		Step:  propose.Step,
		ID:    propose.ID,
		Value: *a.acceptedValue,
	}
	a.Unlock()
	paxosAcceptMsg, err := n.conf.MessageRegistry.MarshalMessage(&paxosAccept)
	if err != nil {
		return xerrors.Errorf("failed to marshal paxos promise message: %v", err)
	}
	err = n.Broadcast(paxosAcceptMsg)
	if err != nil {
		return xerrors.Errorf("failed to broadcast paxos promise message: %v", err)
	}
	return nil
}
