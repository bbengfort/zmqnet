package zmqnet

import (
	"fmt"

	"github.com/bbengfort/x/peers"
	zmq "github.com/pebbe/zmq4"
)

//===========================================================================
// Server and HTTP Transport
//===========================================================================

// Server responds to requests from other peers.
type Server struct {
	net   *Network    // parent network the server is a part of
	host  *peers.Peer // the host and address information of the server
	sock  *zmq.Socket // the socket that the server is bound on
	count uint64      // number of messages received
}

// Init the server with the specified host and any other internal data.
func (s *Server) Init(host *peers.Peer, net *Network) {
	s.host = host
	s.net = net
}

// Run the server and listen for messages
func (s *Server) Run() (err error) {

	// Create the socket
	if s.sock, err = s.net.context.NewSocket(zmq.REP); err != nil {
		return err
	}

	// Bind the socket and run the listener
	ep := s.host.ZMQEndpoint(true)
	if err := s.sock.Bind(ep); err != nil {
		return err
	}
	info("bound to %s\n", ep)

	for {
		msg, err := s.sock.Recv(0)
		if err != nil {
			warne(err)
			break
		}
		s.count++
		s.handle(msg)
	}

	return s.Shutdown()
}

// Shutdown the server and clean up the socket
func (s *Server) Shutdown() error {
	info("shutting down")
	s.sock.Close()
	zmq.Term()
	return nil
}

//===========================================================================
// Message Handling
//===========================================================================

func (s *Server) handle(msg string) {
	info("received: %s\n", msg)
	reply := fmt.Sprintf("reply msg #%d from %s", s.count, s.host.Name)
	s.sock.Send(reply, 0)
}
