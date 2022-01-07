package types

// IdRequestMessage is a message sent to request a peer identity. It contains
// the ip of the sender, identity is taken from packet header.
//
// - implements types.Message
type IdRequestMessage struct {
	Ip string
}

// IdReplyMessage is a message reponding to an identity request. It contains
// the ip of the sender, identity is taken from packet header.
//
// - implements types.Message
type IdReplyMessage struct {
	Ip string
}
