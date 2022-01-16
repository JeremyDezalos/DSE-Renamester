package unit

import (
	"crypto/ed25519"
	"encoding/base64"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	z "go.dedis.ch/cs438/internal/testing"
	"go.dedis.ch/cs438/transport/channel"
	"go.dedis.ch/cs438/types"
)

func Test_HW4_reconnect_without_disconnecting(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:10000")
	defer node1.Stop()

	time.Sleep(time.Second * 1)

	_, err := node1.Reconnect("127.0.0.1:20000")
	require.Error(t, err)

	time.Sleep(time.Second * 1)
}

func Test_HW4_disconnecting_twice(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:10000")
	defer node1.Stop()

	time.Sleep(time.Second * 1)
	err := node1.Disconnect()
	require.NoError(t, err)
	time.Sleep(time.Second * 1)
	err = node1.Disconnect()
	require.Error(t, err)

}

//A lone node disconnecting then reconnecting should give no errors
func Test_HW4_disconnect_then_reconnect(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:1")
	defer node1.Stop()
	err := node1.Disconnect()
	require.NoError(t, err)

	time.Sleep(time.Second * 1)

	_, err = node1.Reconnect("127.0.0.1:2")
	require.NoError(t, err)

	time.Sleep(time.Second * 1)
}

//A lone node can stop properly after disconnecting
func Test_HW4_disconnect_then_stop(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:3")
	defer node1.Stop()
	err := node1.Disconnect()
	require.NoError(t, err)

	time.Sleep(time.Second * 1)
}

// A <-> B <-> C ==(B disconnects)==> A <-> C
func Test_HW4_disconnect_neighbor_distribution(t *testing.T) {
	transp := channel.NewTransport()
	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:4")
	defer node1.Stop()
	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:5")
	defer node2.Stop()
	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:6")
	defer node3.Stop()
	node1.AddPeer(node2.GetAddr())
	node2.AddPeer(node1.GetAddr())

	node2.AddPeer(node3.GetAddr())
	node3.AddPeer(node2.GetAddr())

	time.Sleep(time.Second * 1)

	//B disconnects
	node2.Disconnect()

	time.Sleep(time.Second * 1)

	require.True(t, node1.GetRoutingTable()["127.0.0.1:5"] == "")
	require.True(t, node3.GetRoutingTable()["127.0.0.1:5"] == "")
}

// A <-> B <-> C ==(B fails)==> A <-> C
func Test_HW4_unexpected_disconnection(t *testing.T) {
	transp := channel.NewTransport()
	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:7", z.WithHeartbeat(time.Second*1), z.WithHeartbeatTimeout(3))
	defer node1.Stop()
	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:8", z.WithHeartbeat(time.Second*1), z.WithHeartbeatTimeout(3))

	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:9", z.WithHeartbeat(time.Second*1), z.WithHeartbeatTimeout(3))
	defer node3.Stop()
	node1.AddPeer(node2.GetAddr())
	node2.AddPeer(node1.GetAddr())

	node2.AddPeer(node3.GetAddr())
	node3.AddPeer(node2.GetAddr())

	time.Sleep(time.Second * 1)

	//B fails
	node2.Stop()

	time.Sleep(time.Second * 5)

	require.True(t, node1.GetRoutingTable()["127.0.0.1:8"] == "")
	require.True(t, node3.GetRoutingTable()["127.0.0.1:8"] == "")
}

func Test_HW4_node_with_existing_private_key(t *testing.T) {
	transp := channel.NewTransport()
	_, privKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		panic("could not generate keypair")
	}
	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:10", z.WithHeartbeat(time.Second*1), z.WithHeartbeatTimeout(3), z.WithPrivateKey(privKey))

	var pk ed25519.PrivateKey
	pk, err = base64.StdEncoding.DecodeString(node1.GetPrivateKey())
	if err != nil {
		panic("could not decode private key")
	}
	require.Equal(t, privKey, pk)
}

func Test_HW4_node_broadcast_neighbor_correctly_sign(t *testing.T) {
	transp := channel.NewTransport()
	pubKey1, privKey1, err := ed25519.GenerateKey(nil)
	if err != nil {
		panic("could not generate keypair")
	}
	_, privKey2, err := ed25519.GenerateKey(nil)
	if err != nil {
		panic("could not generate keypair")
	}
	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:11", z.WithHeartbeat(time.Second*1), z.WithHeartbeatTimeout(3), z.WithPrivateKey(privKey1))
	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:12", z.WithHeartbeat(time.Second*1), z.WithHeartbeatTimeout(3), z.WithPrivateKey(privKey2))

	node1.AddPeer(node2.GetAddr())
	time.Sleep(time.Second * 1)
	chatMsg := types.ChatMessage{
		Message: "HelloM8!",
	}
	msg, err := node1.GetRegistry().MarshalMessage(chatMsg)
	if err != nil {
		panic("could not marshal chat msg")
	}
	//b64pubKey2 := base64.StdEncoding.EncodeToString(pubKey2)
	err = node1.Broadcast(msg)
	require.NoError(t, err, "Failed broadcast")

	for _, pkt := range node1.GetOuts() {

		if pkt.Msg.Type != "idRequest" {
			require.True(t, ed25519.Verify(pubKey1, pkt.Msg.Payload, pkt.Msg.Signature))
		}
	}
	for _, pkt := range node2.GetIns() {
		if pkt.Msg.Type != "idRequest" {
			require.True(t, ed25519.Verify(pubKey1, pkt.Msg.Payload, pkt.Msg.Signature))
		}
	}

}

func Test_HW4_node_unicast_neighbor_correctly_sign(t *testing.T) {
	transp := channel.NewTransport()
	_, privKey1, err := ed25519.GenerateKey(nil)
	if err != nil {
		panic("could not generate keypair")
	}
	pubKey2, privKey2, err := ed25519.GenerateKey(nil)
	if err != nil {
		panic("could not generate keypair")
	}
	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:13", z.WithHeartbeat(time.Second*1), z.WithHeartbeatTimeout(3), z.WithPrivateKey(privKey1))
	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:14", z.WithHeartbeat(time.Second*1), z.WithHeartbeatTimeout(3), z.WithPrivateKey(privKey2))

	node1.AddPeer("127.0.0.1:14")
	time.Sleep(time.Second * 1)
	chatMsg := types.ChatMessage{
		Message: "HelloM8!",
	}
	msg, err := node1.GetRegistry().MarshalMessage(chatMsg)
	if err != nil {
		panic("could not marshal chat msg")
	}
	b64pubKey2 := base64.StdEncoding.EncodeToString(pubKey2)
	err = node1.Unicast(b64pubKey2, msg)
	require.NoError(t, err, "Failed unicast")

	for _, pkt := range node1.GetOuts() {
		if pkt.Msg.Type != "idRequest" {
			expectedSig := ed25519.Sign(privKey1, pkt.Msg.Payload)
			require.Equal(t, expectedSig, pkt.Msg.Signature)
		}
	}
	for _, pkt := range node2.GetIns() {
		if pkt.Msg.Type != "idRequest" {
			expectedSig := ed25519.Sign(privKey1, pkt.Msg.Payload)
			require.Equal(t, expectedSig, pkt.Msg.Signature)
		}
	}
}

func Test_HW4_node_broadcast_and_unicast_with_hop_correctly_sign(t *testing.T) {
	transp := channel.NewTransport()
	pubKey1, privKey1, err := ed25519.GenerateKey(nil)
	if err != nil {
		panic("could not generate keypair")
	}
	pubKey2, privKey2, err := ed25519.GenerateKey(nil)
	if err != nil {
		panic("could not generate keypair")
	}
	pubKey3, privKey3, err := ed25519.GenerateKey(nil)
	if err != nil {
		panic("could not generate keypair")
	}
	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:18", z.WithHeartbeat(time.Second*1), z.WithPrivateKey(privKey1))
	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:19", z.WithHeartbeat(time.Second*1), z.WithPrivateKey(privKey2))
	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:20", z.WithHeartbeat(time.Second*1), z.WithPrivateKey(privKey3))

	err = node1.AddPeer(node2.GetAddr())
	require.NoError(t, err, "Node1 failed add node2")
	// One hop
	err = node3.AddPeer(node2.GetAddr())
	require.NoError(t, err, "Node3 failed add node2")

	time.Sleep(time.Second * 1)
	chatMsg := types.ChatMessage{
		Message: "HelloM8!",
	}
	msg, err := node1.GetRegistry().MarshalMessage(&chatMsg)
	if err != nil {
		panic("could not marshal chat msg")
	}
	//b64pubKey2 := base64.StdEncoding.EncodeToString(pubKey2)
	err = node1.Broadcast(msg)
	require.NoError(t, err, "Failed broadcast")

	// Make sure node1 discover node3
	chatMsg3 := types.ChatMessage{
		Message: "HelloM9!",
	}
	msg3, err := node3.GetRegistry().MarshalMessage(&chatMsg3)
	if err != nil {
		panic("could not marshal chat msg")
	}
	err = node3.Broadcast(msg3)
	require.NoError(t, err, "Failed broadcast from node3")
	time.Sleep(time.Millisecond * 2000)

	// fmt.Printf("n1 routing\n%s\n", node1.GetRoutingTable())
	// fmt.Printf("n2 routing\n%s\n", node2.GetRoutingTable())
	// fmt.Printf("n3 routing\n%s\n", node3.GetRoutingTable())

	chatMsgUni := types.ChatMessage{
		Message: "HelloM8!",
	}
	msgUni, err := node1.GetRegistry().MarshalMessage(&chatMsgUni)
	if err != nil {
		panic("could not marshal chat msg")
	}
	// Sending one hop away
	b64pubKey3 := base64.StdEncoding.EncodeToString(pubKey3)
	err = node1.Unicast(b64pubKey3, msgUni)
	time.Sleep(time.Millisecond * 200)

	b64pubKey2 := base64.StdEncoding.EncodeToString(pubKey2)

	b64pubKey1 := base64.StdEncoding.EncodeToString(pubKey1)

	require.NoError(t, err, "Failed unicast")

	for _, pkt := range node1.GetOuts() {
		if pkt.Msg.Type != "idRequest" {
			require.True(t, ed25519.Verify(pubKey1, pkt.Msg.Payload, pkt.Msg.Signature))
		}
	}
	// Verify Signature validity of all in going packets
	for _, pkt := range node1.GetIns() {

		if pkt.Msg.Type != "idRequest" {
			if pkt.Header.Source == b64pubKey2 {
				require.True(t, ed25519.Verify(pubKey2, pkt.Msg.Payload, pkt.Msg.Signature))
			} else if pkt.Header.Source == b64pubKey3 {
				require.True(t, ed25519.Verify(pubKey3, pkt.Msg.Payload, pkt.Msg.Signature))
			}
		}
	}
	for _, pkt := range node2.GetIns() {
		// Check who sent!!! (with pkt.Header) to know which pubkey to use
		if pkt.Msg.Type != "idRequest" {
			// fmt.Printf("type: %s\nPayload: %s\n sig: %v\n", pkt.Msg.Type, pkt.Msg.Payload, pkt.Msg.Signature)
			if pkt.Header.Source == b64pubKey1 {
				require.True(t, ed25519.Verify(pubKey1, pkt.Msg.Payload, pkt.Msg.Signature))
			} else if pkt.Header.Source == b64pubKey3 {
				require.True(t, ed25519.Verify(pubKey3, pkt.Msg.Payload, pkt.Msg.Signature))
			}
		}
	}
	for _, pkt := range node3.GetIns() {
		if pkt.Msg.Type != "idRequest" {
			// fmt.Printf("Type: %s\nPayload: %s\nSig: %v\n", pkt.Msg.Type, pkt.Msg.Payload, pkt.Msg.Signature)
			if pkt.Header.Source == b64pubKey1 {
				require.True(t, ed25519.Verify(pubKey1, pkt.Msg.Payload, pkt.Msg.Signature))
			} else if pkt.Header.Source == b64pubKey2 {
				require.True(t, ed25519.Verify(pubKey2, pkt.Msg.Payload, pkt.Msg.Signature))
			}
		}
	}

}

func Test_HW4_node_broadcast_rename(t *testing.T) {
	transp := channel.NewTransport()
	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:21")
	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:22")
	node1.AddPeer(node2.GetAddr())
	renameMsg := types.RenameMessage{
		Alias: "new",
	}
	msg, err := node1.GetRegistry().MarshalMessage(&renameMsg)
	require.NoError(t, err, "failed to marshal")
	node1.Broadcast(msg)
	time.Sleep(time.Millisecond * 20)
	names := make([]string, 0)
	for _, v := range node2.GetAliasTable() {
		// fmt.Printf("%s\n", v)
		names = append(names, v)
	}
	require.Contains(t, names, "new")

}

func Test_HW4_node_unicast_rename(t *testing.T) {
	transp := channel.NewTransport()
	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:21")
	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:22")
	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:23")

	node1.AddPeer(node2.GetAddr())
	node2.AddPeer(node3.GetAddr())
	time.Sleep(time.Millisecond * 20)

	// Replace heartbeat (node1 and 3 have to discover each others)
	emptyMsg := types.EmptyMessage{}
	empty, err := node3.GetRegistry().MarshalMessage(&emptyMsg)
	require.NoError(t, err, "failed to marshal")
	node3.Broadcast(empty)
	empty, err = node1.GetRegistry().MarshalMessage(&emptyMsg)
	require.NoError(t, err, "failed to marshal")
	node1.Broadcast(empty)

	time.Sleep(time.Millisecond * 20)

	renameMsg := types.RenameMessage{
		Alias: "new",
	}
	msg, err := node1.GetRegistry().MarshalMessage(&renameMsg)
	require.NoError(t, err, "failed to marshal")

	b64privKey, err := base64.StdEncoding.DecodeString(node3.GetPrivateKey())
	privKey := ed25519.PrivateKey(b64privKey)
	require.NoError(t, err, "failed to decode key")
	pubKey, ok := privKey.Public().(ed25519.PublicKey)
	if !ok {
		panic("failed to generate public key from given private key, cannot recover")
	}
	id3 := base64.StdEncoding.EncodeToString(pubKey)
	node1.Unicast(id3, msg)
	time.Sleep(time.Millisecond * 20)
	names := make([]string, 0)
	for _, v := range node2.GetAliasTable() {
		// fmt.Printf("%s\n", v)
		names = append(names, v)
	}
	require.NotContains(t, names, "new")

	names = make([]string, 0)
	for _, v := range node3.GetAliasTable() {
		// fmt.Printf("%s\n", v)
		names = append(names, v)
	}
	require.Contains(t, names, "new")
}
