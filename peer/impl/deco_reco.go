package impl

import (
	"sync"

	"go.dedis.ch/cs438/types"
	"golang.org/x/xerrors"
)

func (n *node) disconnect() error {
	if !n.isConnected {
		return xerrors.Errorf("node already disconnected")
	}
	msg := types.DisconnectMessage{}
	deco, err := n.conf.MessageRegistry.MarshalMessage(msg)
	if err != nil {
		return err
	}
	n.Broadcast(deco)
	n.isConnected = false
	n.conf.Socket.Close()
	return nil
}

func (n *node) reconnect(address string) error {
	if n.isConnected {
		return xerrors.Errorf("node already connected")
	}
	socket, err := n.conf.Transport.CreateSocket(address)
	if err != nil {
		return err
	}
	n.conf.Socket = socket
	n.isConnected = true
	for _, channel := range n.waitReconnection.getAll() {
		channel <- struct{}{}
	}
	return nil
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
	list := make([]string, len(neighbors))
	for neighbor := range neighbors {
		list = append(list, neighbor)
	}
	for index, neighbor := range list {
		if index == 0 {
			backupNodes := make([]string, 1)
			backupNodes = append(backupNodes, list[index+1])
			err := n.sendBackupNodes(neighbor, backupNodes)
			if err != nil {
				return err
			}
		} else if index == len(list)-1 {
			backupNodes := make([]string, 2)
			backupNodes = append(backupNodes, list[index-1])
			backupNodes = append(backupNodes, list[index+1])
			err := n.sendBackupNodes(neighbor, backupNodes)
			if err != nil {
				return err
			}
		} else {
			backupNodes := make([]string, 1)
			backupNodes = append(backupNodes, list[index-1])
			err := n.sendBackupNodes(neighbor, backupNodes)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *node) handleDisconnection(address string) {
	_, isNeighbor := getNeighbors(n.lockedRoutingTable.routingTable)[address]
	n.lockedRoutingTable.delete(address)
	n.messaging.missedHeartBeats.delete(address)
	if isNeighbor {
		backupNeighbors, ok := n.backupNodes.getBackup(address)
		if ok {
			for _, newNeighbor := range backupNeighbors {
				n.SetRoutingEntry(newNeighbor, newNeighbor)
			}
		}
		n.sendNewNeighborsToPeers()
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

type WaitReconnection struct {
	sync.Mutex
	waiting map[chan struct{}]struct{}
}

func (waitReco *WaitReconnection) getAll() []chan struct{} {
	waitReco.Lock()
	defer waitReco.Unlock()
	channels := make([]chan struct{}, len(waitReco.waiting))
	for waiting := range waitReco.waiting {
		channels = append(channels, waiting)
	}
	return channels
}

func (waitReco *WaitReconnection) set(channel chan struct{}) {
	waitReco.Lock()
	defer waitReco.Unlock()
	waitReco.waiting[channel] = struct{}{}
}

func (waitReco *WaitReconnection) delete(channel chan struct{}) {
	waitReco.Lock()
	defer waitReco.Unlock()
	delete(waitReco.waiting, channel)
}
