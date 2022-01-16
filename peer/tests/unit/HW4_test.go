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
	_, privKey2, err := ed25519.GenerateKey(nil)
	if err != nil {
		panic("could not generate keypair")
	}
	pubKey3, privKey3, err := ed25519.GenerateKey(nil)
	if err != nil {
		panic("could not generate keypair")
	}
	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:15", z.WithHeartbeat(time.Second*1), z.WithPrivateKey(privKey1))
	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:16", z.WithHeartbeat(time.Second*1), z.WithPrivateKey(privKey2))
	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:17", z.WithHeartbeat(time.Second*1), z.WithPrivateKey(privKey3))

	err = node1.AddPeer(node2.GetAddr())
	require.NoError(t, err, "Node1 failed add node2")
	// One hop
	err = node3.AddPeer(node2.GetAddr())
	require.NoError(t, err, "Node3 failed add node2")

	time.Sleep(time.Second * 1)
	chatMsg := types.ChatMessage{
		Message: "HelloM9!",
	}
	msg, err := node1.GetRegistry().MarshalMessage(chatMsg)
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
	msg3, err := node3.GetRegistry().MarshalMessage(chatMsg3)
	if err != nil {
		panic("could not marshal chat msg")
	}
	err = node3.Broadcast(msg3)
	require.NoError(t, err, "Failed broadcast from node3")
	time.Sleep(time.Millisecond * 20)

	// fmt.Printf("Node1 INS\n")

	// for _, pkt := range node1.GetIns() {

	// 	fmt.Printf("%s\nMessage:%s\n", pkt.Header, pkt.Msg)
	// }

	// fmt.Printf("Node1 OUTS\n")

	// for _, pkt := range node1.GetOuts() {

	// 	fmt.Printf("%s\nMessage:%s\n", pkt.Header, pkt.Msg)
	// }

	// fmt.Printf("Node2 INS\n")

	// for _, pkt := range node2.GetIns() {

	// 	fmt.Printf("%s\nMessage:%s\n", pkt.Header, pkt.Msg)
	// }

	// fmt.Printf("Node2 OUTS\n")

	// for _, pkt := range node2.GetOuts() {

	// 	fmt.Printf("%s\nMessage:%s\n", pkt.Header, pkt.Msg)
	// }

	// fmt.Printf("%s\n%s\n", node3.GetRoutingTable(), node3.GetNeighborsTable())

	// for _, pkt := range node3.GetOuts() {

	// 	fmt.Printf("%s\nMessage:%s\n", pkt.Header, pkt.Msg)
	// }

	// fmt.Printf("%s\n%s\n", node2.GetRoutingTable(), node2.GetNeighborsTable())
	// fmt.Printf("Node2 INS\n")

	// for _, pkt := range node2.GetIns() {

	// 	fmt.Printf("%s\nMessage:%s\n", pkt.Header, pkt.Msg)
	// }

	// fmt.Printf("Node2 OUTS\n")

	// for _, pkt := range node2.GetOuts() {

	// 	fmt.Printf("%s\nMessage:%s\n", pkt.Header, pkt.Msg)
	// }
	// fmt.Printf("%s\n%s\n", node1.GetRoutingTable(), node1.GetNeighborsTable())
	// for _, pkt := range node1.GetIns() {

	// 	fmt.Printf("%s\nMessage:%s\n", pkt.Header, pkt.Msg)
	// }

	// Sending one hop away
	b64pubKey3 := base64.StdEncoding.EncodeToString(pubKey3)
	err = node1.Unicast(b64pubKey3, msg)
	require.NoError(t, err, "Failed unicast")

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
	for _, pkt := range node3.GetIns() {
		if pkt.Msg.Type != "idRequest" {
			require.True(t, ed25519.Verify(pubKey1, pkt.Msg.Payload, pkt.Msg.Signature))
		}
	}

}
