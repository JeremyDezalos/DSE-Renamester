package unit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	z "go.dedis.ch/cs438/internal/testing"
	"go.dedis.ch/cs438/transport/channel"
)

//A lone node disconnecting then reconnecting should give no errors
func Test_HW4_disconnect_then_reconnect(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:1")
	defer node1.Stop()
	err := node1.Disconnect()
	require.NoError(t, err)

	time.Sleep(time.Second * 1)

	err = node1.Reconnect("127.0.0.1:2")
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
