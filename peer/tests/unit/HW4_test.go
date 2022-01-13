package unit

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	z "go.dedis.ch/cs438/internal/testing"
	"go.dedis.ch/cs438/transport/channel"
)

func Test_HW4_disconnect_then_reconnect(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()
	err := node1.Disconnect()
	require.NoError(t, err)

	time.Sleep(time.Second * 1)

	err = node1.Reconnect("127.0.0.1:0")
	require.NoError(t, err)

	time.Sleep(time.Second * 1)
}

func Test_HW4_disconnect_then_stop(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()
	err := node1.Disconnect()
	require.NoError(t, err)

	time.Sleep(time.Second * 1)
}

// A <-> B <-> C ==(B disconnects)==> A <-> C
func Test_HW4_disconnect_neighbor_distribution(t *testing.T) {
	transp := channel.NewTransport()
	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node1.Stop()
	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node2.Stop()
	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0")
	defer node3.Stop()
	node1.AddPeer(node2.GetAddr())
	node2.AddPeer(node1.GetAddr())

	node2.AddPeer(node3.GetAddr())
	node3.AddPeer(node2.GetAddr())

	time.Sleep(time.Second * 1)

	node2.Disconnect()

	time.Sleep(time.Second * 1)

	fmt.Println(node1.GetRoutingTable().String())
}

func Test_HW4_unexpected_disconnection(t *testing.T) {
	transp := channel.NewTransport()
	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithHeartbeat(time.Second*1), z.WithHeartbeatTimeout(3))
	defer node1.Stop()
	node2 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithHeartbeat(time.Second*1), z.WithHeartbeatTimeout(3))
	//defer node2.Stop()
	node3 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithHeartbeat(time.Second*1), z.WithHeartbeatTimeout(3))
	defer node3.Stop()
	node1.AddPeer(node2.GetAddr())
	node2.AddPeer(node1.GetAddr())

	node2.AddPeer(node3.GetAddr())
	node3.AddPeer(node2.GetAddr())

	time.Sleep(time.Second * 1)

	node2.Stop()

	time.Sleep(time.Second * 5)

	fmt.Println(node1.GetRoutingTable().String())
}
