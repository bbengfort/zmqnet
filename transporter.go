package zmqnet

import (
	"errors"

	"github.com/bbengfort/x/peers"
	pb "github.com/bbengfort/zmqnet/msg"
	"github.com/gogo/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
)

//===========================================================================
// Network Transporter
//===========================================================================

// Transporter is a wrapper around a zmq.Socket object that is accessed by
// a single host, either remote or local. Both clients and servers are
// transporters.
//
// The primary role of the transporter is to send and receive messages
// defined as protocol buffers. They can wrap any type of ZMQ object and its
// up to the primary classes to instantiate the socket correctly.
type Transporter struct {
	net    *Network    // parent network the transporter is a part of
	host   *peers.Peer // the host and address information of the connection
	sock   *zmq.Socket // the zmq socket to send and receive messages
	nSent  uint64      // number of messages sent
	nRecv  uint64      // number of messages received
	nBytes uint64      // number of bytes sent
}

// Init the transporter with the specified host and any other internal data.
func (t *Transporter) Init(host *peers.Peer, net *Network) {
	t.host = host
	t.net = net
}

// Close the socket and clean up the connections.
func (t *Transporter) Close() error {
	return t.sock.Close()
}

//===========================================================================
// Send and Recv Protobuf Messages
//===========================================================================

// Reads a zmq message from the socket and composes it into a protobuff
// message for handling downstream. This method blocks until a message is
// received.
func (t *Transporter) recv() (*pb.Message, error) {
	// Break if the socket hasn't been created
	if t.sock == nil {
		return nil, errors.New("socket is not initialized")
	}

	// Read the data off the wire
	bytes, err := t.sock.RecvBytes(0)
	if err != nil {
		return nil, err
	}

	// Parse the protocol buffers message
	message := new(pb.Message)
	if err := proto.Unmarshal(bytes, message); err != nil {
		return nil, err
	}

	// Increment the number of messages received
	t.nRecv++

	// Return the message
	return message, nil
}

// Composes a message into protocol buffers and puts it on the socket.
// Does not wait for the receiver, just fires off the reply.
func (t *Transporter) send(message string, mtype pb.MessageType) error {
	if t.sock == nil {
		return errors.New("socket is not initialized")
	}

	// Compose the protobuf message
	msg := &pb.Message{
		Type:    mtype,
		Sender:  t.host.Name,
		Message: message,
	}

	// Serialize the message
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	// Send the bytes on the wire
	nbytes, err := t.sock.SendBytes(data, zmq.DONTWAIT)
	if err != nil {
		return err
	}

	// Increment the number of messages sent and the number of bytes sent
	t.nBytes += uint64(nbytes)
	t.nSent++

	return nil
}
