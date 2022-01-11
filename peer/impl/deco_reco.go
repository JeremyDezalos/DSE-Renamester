package impl

import (
	"go.dedis.ch/cs438/types"
)

func (n *node) disconnect() {

}

func (n *node) reconnect() {

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

func (n *node) handlePerrDisconenction() {

}
