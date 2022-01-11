package types

import (
	"fmt"
)

// -----------------------------------------------------------------------------
// IdRequestMessage

// NewEmpty implements types.Message.
func (c IdRequestMessage) NewEmpty() Message {
	return &IdRequestMessage{}
}

// Name implements types.Message.
func (IdRequestMessage) Name() string {
	return "idRequest"
}

// String implements types.Message.
func (c IdRequestMessage) String() string {
	return fmt.Sprintf("IP: %s", c.Ip)
}

// HTML implements types.Message.
func (c IdRequestMessage) HTML() string {
	return c.String()
}

// -----------------------------------------------------------------------------
// IdReplyMessage

// NewEmpty implements types.Message.
func (c IdReplyMessage) NewEmpty() Message {
	return &IdReplyMessage{}
}

// Name implements types.Message.
func (IdReplyMessage) Name() string {
	return "idReply"
}

// String implements types.Message.
func (c IdReplyMessage) String() string {
	return fmt.Sprintf("IP: %s", c.Ip)
}

// HTML implements types.Message.
func (c IdReplyMessage) HTML() string {
	return c.String()
}

// -----------------------------------------------------------------------------
// RenameMessage

// NewEmpty implements types.Message.
func (c RenameMessage) NewEmpty() Message {
	return &RenameMessage{}
}

// Name implements types.Message.
func (RenameMessage) Name() string {
	return "rename"
}

// String implements types.Message.
func (c RenameMessage) String() string {
	return fmt.Sprintf("renaming to %s", c.Alias)
}

// HTML implements types.Message.
func (c RenameMessage) HTML() string {
	return c.String()
}
