// /!\ CODE SHAMELESSLY STOLEN FROM THE CODE OF STUDENT 68 OF HOMEHORK 0
package udp

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.dedis.ch/cs438/transport"
)

const bufSize = 65000

// NewUDP returns a new udp transport implementation.
func NewUDP() transport.Transport {
	return &UDP{}
}

// UDP implements a transport layer using UDP
//
// - implements transport.Transport
type UDP struct {
	transport.Transport
}

func splitAddress(address string) (string, int, error) {
	s := strings.Split(address, ":")
	if len(s) != 2 {
		return "", -1, fmt.Errorf("the syntax of address is wrong! ")
	}
	ip := s[0]
	port, err := strconv.Atoi(s[1])
	if err != nil {
		return "", -1, fmt.Errorf("the syntax of port is wrong! ")
	}
	return ip, port, nil
}

// CreateSocket implements transport.Transport
func (n *UDP) CreateSocket(address string) (transport.ClosableSocket, error) {
	ip, port, err0 := splitAddress(address)
	if err0 != nil {
		return &Socket{}, err0
	}

	addr := net.UDPAddr{
		Port: port,
		IP:   net.ParseIP(ip),
	}
	conn, err := net.ListenUDP("udp", &addr)
	//ln, err := net.Listen("udp", ":8080")
	if err != nil {
		log.Fatal("Listen udp fails.")
	}
	UdpSocket := Socket{myAddr: address, Conn: conn}
	return &UdpSocket, nil
}

// Socket implements a network socket using UDP.
//
// - implements transport.Socket
// - implements transport.ClosableSocket
type Socket struct {
	sync.Mutex
	transport.ClosableSocket
	myAddr string
	Conn   *net.UDPConn
	ins    []transport.Packet
	outs   []transport.Packet
}

// Close implements transport.Socket. It returns an error if already closed.
func (s *Socket) Close() error {
	err := s.Conn.Close()
	return err
}

// Send implements transport.Socket
func (s *Socket) Send(dest string, pkt transport.Packet, timeout time.Duration) error {
	conn, err := net.DialTimeout("udp", dest, timeout)
	if err != nil {
		log.Fatal("connect destination fails.")
	}
	buf, err1 := pkt.Marshal()
	if err1 != nil {
		log.Fatal("can not transform the packet to bytes")
	}

	_, err2 := conn.Write(buf)
	if err2 != nil {
		log.Fatal("write bytes to socket fails.")
	}
	s.Lock()
	defer s.Unlock()
	s.outs = append(s.outs, pkt)
	return nil
}

// Recv implements transport.Socket. It blocks until a packet is received, or
// the timeout is reached. In the case the timeout is reached, return a
// TimeoutErr.
func (s *Socket) Recv(timeout time.Duration) (transport.Packet, error) {
	buf := make([]byte, bufSize)
	err0 := s.Conn.SetReadDeadline(time.Now().Add(timeout))
	if err0 != nil {
		log.Fatal("set read deadline fails.")
	}
	n, err1 := s.Conn.Read(buf)
	buf = buf[:n]

	// if timeout
	if err1 != nil {
		return transport.Packet{}, transport.TimeoutErr(0)
	}

	var newPkt transport.Packet
	err2 := newPkt.Unmarshal(buf)
	if err2 != nil {
		log.Fatal("can't transform bytes to packet.")
	}

	s.Lock()
	defer s.Unlock()
	s.ins = append(s.ins, newPkt)

	return newPkt, nil
}

// GetAddress implements transport.Socket. It returns the address assigned. Can
// be useful in the case one provided a :0 address, which makes the system use a
// random free port.
func (s *Socket) GetAddress() string {
	return s.Conn.LocalAddr().String()
}

// GetIns implements transport.Socket
func (s *Socket) GetIns() []transport.Packet {

	s.Lock()
	defer s.Unlock()

	ins := []transport.Packet{}
	ins = append(ins, s.ins...)

	return ins

}

// GetOuts implements transport.Socket
func (s *Socket) GetOuts() []transport.Packet {

	s.Lock()
	defer s.Unlock()

	outs := []transport.Packet{}

	outs = append(outs, s.outs...)

	return outs

}
