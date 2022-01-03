package types

import "fmt"

// -----------------------------------------------------------------------------
// DisconnectMessage

// NewEmpty implements types.Message.
func (d DisconnectMessage) NewEmpty() Message {
	return &DisconnectMessage{}
}

// Name implements types.Message.
func (d DisconnectMessage) Name() string {
	return "disconnect"
}

// String implements types.Message.
func (d DisconnectMessage) String() string {
	return "disconnect"
}

// HTML implements types.Message.
func (d DisconnectMessage) HTML() string {
	return d.String()
}

// -----------------------------------------------------------------------------
// NeighborsMessage

// NewEmpty implements types.Message.
func (d NeighborsMessage) NewEmpty() Message {
	return &NeighborsMessage{}
}

// Name implements types.Message.
func (d NeighborsMessage) Name() string {
	return "neighbors message"
}

// String implements types.Message.
func (d NeighborsMessage) String() string {
	return fmt.Sprintf("neighbors message containing %d backup neighbors", len(d.Neighbors))
}

// HTML implements types.Message.
func (d NeighborsMessage) HTML() string {
	return d.String()
}
