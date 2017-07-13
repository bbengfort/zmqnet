package zmqnet

import (
	"fmt"

	"github.com/bbengfort/x/peers"
	"github.com/bbengfort/zmqnet/msg"
	"github.com/gogo/protobuf/proto"
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
		msg, err := s.recv()
		if err != nil {
			warne(err)
			break
		}
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

func (s *Server) handle(message *msg.Message) {
	info("received: %s\n", message.String())
	reply := fmt.Sprintf("reply msg #%d", s.count)
	s.send(reply)
}

// Reads a zmq message from the socket and composes it into a protobuff
// message for handling downstream. This method blocks until a message is
// received.
func (s *Server) recv() (*msg.Message, error) {
	bytes, err := s.sock.RecvBytes(0)
	if err != nil {
		return nil, err
	}

	s.count++

	message := new(msg.Message)
	if err := proto.Unmarshal(bytes, message); err != nil {
		return nil, err
	}

	return message, nil
}

// Composes a message into protocol buffers and puts it on the socket.
// Does not wait for the receiver, just fires off the reply.
func (s *Server) send(message string) error {
	// Compose the protobuf message
	reply := &msg.Message{
		Type:    msg.MessageType_SINGLE,
		Id:      s.count,
		Sender:  s.host.Name,
		Message: message,
	}

	// Serialize the message
	data, err := proto.Marshal(reply)
	if err != nil {
		return err
	}

	// Send the bytes on the wire
	_, err = s.sock.SendBytes(data, zmq.DONTWAIT)
	return err
}
