package impl

import (
	"fmt"
	"sync"

	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

func (n *node) Disconnect() error {
	if !n.decoReco.isConnected {
		return xerrors.Errorf("node already disconnected")
	}
	msg := types.DisconnectMessage{}
	deco, err := n.conf.MessageRegistry.MarshalMessage(msg)
	if err != nil {
		return err
	}

	for neighbor := range n.GetRoutingTable() {
		n.Unicast(neighbor, deco)
	}

	n.decoReco.setStatus(false)
	n.socketMutex.Lock()
	n.conf.Socket.Close()
	n.socketMutex.Unlock()
	return nil
}

func (n *node) Reconnect(address string) error {
	if n.decoReco.isConnected {
		return xerrors.Errorf("node already connected")
	}
	socket, err := n.conf.Transport.CreateSocket(address)
	if err != nil {
		return err
	}

	n.socketMutex.Lock()
	n.conf.Socket = socket
	n.address.setAddress(n.conf.Socket.GetAddress())
	n.socketMutex.Unlock()

	n.decoReco.setStatus(true)
	n.NotifyWaitingGoroutines()

	return nil
}

func (n *node) NotifyWaitingGoroutines() {
	for channel := range n.decoReco.getAllChannels() {
		channel <- struct{}{}
	}
}

func (n *node) sendBackupNodes(address string, backupNodes []string) error {
	msg := types.NeighborsMessage{
		Neighbors: backupNodes,
	}
	toSend, err := n.conf.MessageRegistry.MarshalMessage(msg)
	if err != nil {
		return err
	}
	return n.Unicast(address, toSend)
}

func (n *node) sendNewNeighborsToPeers() error {
	neighbors := getNeighbors(n.GetRoutingTable())
	list := make([]string, len(neighbors)-1)
	index := 0
	fmt.Println(neighbors)
	for neighbor := range neighbors {
		if neighbor != n.address.getAddress() {
			list[index] = neighbor
			index++
		}
	}
	if len(list) > 1 {
		for index, neighbor := range list {
			if index == 0 {
				backupNodes := make([]string, 1)
				backupNodes[0] = list[index+1]
				err := n.sendBackupNodes(neighbor, backupNodes)
				if err != nil {
					return err
				}
			} else if index == len(list)-1 {
				backupNodes := make([]string, 1)
				backupNodes[0] = list[index-1]
				err := n.sendBackupNodes(neighbor, backupNodes)
				if err != nil {
					return err
				}
			} else {
				backupNodes := make([]string, 2)
				backupNodes[0] = list[index-1]
				backupNodes[1] = list[index+1]
				err := n.sendBackupNodes(neighbor, backupNodes)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (n *node) handleDisconnection(address string) {
	_, isNeighbor := getNeighbors(n.GetRoutingTable())[address]
	n.lockedRoutingTable.delete(address)
	n.messaging.missedHeartBeats.delete(address)
	if isNeighbor {
		backupNeighbors, ok := n.backupNodes.getBackup(address)
		fmt.Println(backupNeighbors)
		if ok {
			for _, newNeighbor := range backupNeighbors {
				n.SetRoutingEntry(newNeighbor, newNeighbor)
			}
		}
	}
}

type BackupMap struct {
	sync.Mutex
	backups map[string][]string
}

func (backupMap *BackupMap) getBackup(key string) ([]string, bool) {
	backupMap.Lock()
	defer backupMap.Unlock()
	val, ok := backupMap.backups[key]
	return val, ok
}

func (backupMap *BackupMap) setBackup(key string, val []string) {
	backupMap.Lock()
	defer backupMap.Unlock()
	backupMap.backups[key] = val
}

func (backupMap *BackupMap) delete(key string) {
	backupMap.Lock()
	defer backupMap.Unlock()
	delete(backupMap.backups, key)
}

type MissedHeartBeatCounter struct {
	sync.Mutex
	counters map[string]uint
}

func (counter *MissedHeartBeatCounter) getCounter(key string) (uint, bool) {
	counter.Lock()
	defer counter.Unlock()
	val, ok := counter.counters[key]
	return val, ok
}

func (counter *MissedHeartBeatCounter) getCounters() map[string]uint {
	counter.Lock()
	defer counter.Unlock()
	copy := make(map[string]uint)
	for key, val := range counter.counters {
		copy[key] = val
	}
	return copy
}

func (counter *MissedHeartBeatCounter) setCounter(key string, value uint) {
	counter.Lock()
	defer counter.Unlock()
	counter.counters[key] = value
}

func (counter *MissedHeartBeatCounter) delete(key string) {
	counter.Lock()
	defer counter.Unlock()
	delete(counter.counters, key)
}

type DecoReco struct {
	sync.Mutex
	isConnected bool
	waiting     map[chan struct{}]struct{}
}

func (waitReco *DecoReco) getAllChannels() map[chan struct{}]struct{} {
	waitReco.Lock()
	defer waitReco.Unlock()
	channels := make(map[chan struct{}]struct{})
	for waiting := range waitReco.waiting {
		channels[waiting] = struct{}{}
	}
	return channels
}

func (waitReco *DecoReco) setChannel(channel chan struct{}) {
	waitReco.Lock()
	defer waitReco.Unlock()
	waitReco.waiting[channel] = struct{}{}
}

func (waitReco *DecoReco) deleteChannel(channel chan struct{}) {
	waitReco.Lock()
	defer waitReco.Unlock()
	delete(waitReco.waiting, channel)
}

func (waitReco *DecoReco) getStatus() bool {
	waitReco.Lock()
	defer waitReco.Unlock()
	return waitReco.isConnected
}

func (waitReco *DecoReco) setStatus(status bool) {
	waitReco.Lock()
	defer waitReco.Unlock()
	waitReco.isConnected = false
}

type Address struct {
	sync.Mutex
	address string
}

func (a *Address) getAddress() string {
	a.Lock()
	defer a.Unlock()
	return a.address
}

func (a *Address) setAddress(address string) {
	a.Lock()
	defer a.Unlock()
	a.address = address
}
