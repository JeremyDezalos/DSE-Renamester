package unit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	z "go.dedis.ch/cs438/internal/testing"
	"go.dedis.ch/cs438/transport/channel"
)

func Test_HW4_deco_reco(t *testing.T) {
	transp := channel.NewTransport()

	node1 := z.NewTestNode(t, peerFac, transp, "127.0.0.1:0", z.WithHeartbeat(time.Second), z.WithHeartbeatTimeout(3))
	defer node1.Stop()
	err := node1.Disconnect()
	require.NoError(t, err)
	err = node1.Reconnect("127.0.0.1:0")
	require.NoError(t, err)
}
