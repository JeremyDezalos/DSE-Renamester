package peer

import "go.dedis.ch/cs438/transport"

// DecoReco defines the functions used for joining and leaving the network
type DecoReco interface {
	//Disconnects the node from the network, but does not delete it
	Disconnect() error

	//Reconnects the node to the network
	Reconnect(address string) (transport.ClosableSocket, error)

	GetConnectionStatus() bool
}
