package udp

import (
	"errors"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.dedis.ch/cs438/transport"
	"golang.org/x/xerrors"
)

const bufSize = 65000

var logout = zerolog.ConsoleWriter{
	Out:        os.Stdout,
	TimeFormat: time.RFC3339,
}
var log = zerolog.New(logout).Level(zerolog.Disabled).
	With().Timestamp().Logger().
	With().Caller().Logger()

// NewUDP returns a new udp transport implementation.
func NewUDP() transport.Transport {
	return &UDP{}
}

// UDP implements a transport layer using UDP
//
// - implements transport.Transport
type UDP struct {
}

// CreateSocket implements transport.Transport
func (n *UDP) CreateSocket(address string) (transport.ClosableSocket, error) {

	UDPAddr, err := net.ResolveUDPAddr("udp", address)
	s := Socket{}
	if err != nil {
		return &s, xerrors.Errorf("failed to resolve address: %v", err)
	}

	// Open UDP connection
	conn, err := net.ListenUDP("udp", UDPAddr)
	if err != nil {
		return &s, xerrors.Errorf("failed to open UDP connection (listen): %v", err)
	}
	// Id the original address did not have a port, make sure store the right address
	if strings.HasSuffix(address, ":0") {
		UDPAddr, err = net.ResolveUDPAddr("udp", conn.LocalAddr().String())
		if err != nil {
			return &s, xerrors.Errorf("failed to resolve address: %v", err)
		}
	}

	log.Debug().Msgf("Successfully opened socket on : %s", UDPAddr.String())
	return &Socket{
		conn:   conn,
		myAddr: UDPAddr,

		ins:  packets{},
		outs: packets{},
	}, nil
}

// Socket implements a network socket using UDP.
//
// - implements transport.Socket
// - implements transport.ClosableSocket
type Socket struct {
	conn   *net.UDPConn
	myAddr *net.UDPAddr

	ins  packets
	outs packets
}

// Close implements transport.Socket. It returns an error if already closed.
func (s *Socket) Close() error {

	err := s.conn.Close()
	if err != nil {
		return xerrors.Errorf("failed to close UDP connection: %v", err)
	}

	return nil
}

// Send implements transport.Socket
func (s *Socket) Send(dest string, pkt transport.Packet, timeout time.Duration) error {

	destUDP, err := net.ResolveUDPAddr("udp", dest)
	if err != nil {
		return xerrors.Errorf("failed to resolve address: %v", err)
	}

	buffer, err := pkt.Marshal()
	if err != nil {
		return xerrors.Errorf("failed to marshal packet: %v", err)
	}
	s.conn.SetWriteDeadline(time.Now().Add(timeout))
	if timeout == 0 {
		s.conn.SetWriteDeadline(time.Time{})
	}
	s.conn.WriteToUDP(buffer, destUDP)
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return transport.TimeoutErr(0)
	} else if err != nil {
		return xerrors.Errorf("failed to send packet: %v", err)
	}

	log.Debug().Msgf("Successfully sent packet to : %s", destUDP.String())

	s.outs.add(pkt.Copy())
	return nil
}

// Recv implements transport.Socket. It blocks until a packet is received, or
// the timeout is reached. In the case the timeout is reached, return a
// TimeoutErr.
func (s *Socket) Recv(timeout time.Duration) (transport.Packet, error) {

	pkt := transport.Packet{}
	buffer := make([]byte, bufSize)
	// Set deadline AKA send error when read or write takes too long
	s.conn.SetReadDeadline(time.Now().Add(timeout))
	if timeout == 0 {
		s.conn.SetReadDeadline(time.Time{})
	}
	n, _, err := s.conn.ReadFromUDP(buffer)

	if errors.Is(err, os.ErrDeadlineExceeded) {
		return pkt, transport.TimeoutErr(0)
	} else if err != nil {
		return pkt, xerrors.Errorf("failed to read UDP packet: %v", err)
	}

	err = pkt.Unmarshal(buffer[:n])
	if err != nil {
		// should be TimeoutErr
		return pkt, xerrors.Errorf("failed to unmarshal received packet: %v", err)
	}

	s.ins.add(pkt.Copy())
	return pkt, nil

}

// GetAddress implements transport.Socket. It returns the address assigned. Can
// be useful in the case one provided a :0 address, which makes the system use a
// random free port.
func (s *Socket) GetAddress() string {
	return s.myAddr.String()
}

// GetIns implements transport.Socket
func (s *Socket) GetIns() []transport.Packet {
	return s.ins.getAll()
}

// GetOuts implements transport.Socket
func (s *Socket) GetOuts() []transport.Packet {
	return s.outs.getAll()
}

type packets struct {
	sync.Mutex
	data []transport.Packet
}

func (p *packets) add(pkt transport.Packet) {
	p.Lock()
	defer p.Unlock()

	p.data = append(p.data, pkt)
}

func (p *packets) getAll() []transport.Packet {
	p.Lock()
	defer p.Unlock()

	res := make([]transport.Packet, len(p.data))

	for i, pkt := range p.data {
		res[i] = pkt.Copy()
	}

	return res
}
