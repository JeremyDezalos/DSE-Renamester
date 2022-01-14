package types

//Defines a message sent when a node disconnects
type DisconnectMessage struct{}

//Defines a message that contains a list of neighbors in case the sender disconnects
type NeighborsMessage struct {
	Neighbors []RoutingTableEntry
}
