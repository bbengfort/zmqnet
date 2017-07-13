package zmqnet

import (
	"fmt"
	"time"

	"github.com/bbengfort/x/peers"
	"github.com/bbengfort/zmqnet/msg"
	"github.com/gogo/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
)

//===========================================================================
// Client for Remote Peers
//===========================================================================

// Client communicates with a remote peer.
type Client struct {
	net   *Network    // parent network the client is a part of
	host  *peers.Peer // the host and address information of the server
	sock  *zmq.Socket // the socket that the client is connected to
	count uint64      // number of messages sent
}

// Init the client with the specified host and any other internal data.
func (c *Client) Init(host *peers.Peer, net *Network) {
	c.host = host
	c.net = net
}

// Connect to the remote peer
func (c *Client) Connect() (err error) {
	// Create the socket
	if c.sock, err = c.net.context.NewSocket(zmq.REQ); err != nil {
		return err
	}

	// Connect to the server
	ep := c.host.ZMQEndpoint(false)
	if err = c.sock.Connect(ep); err != nil {
		return err
	}
	info("connected to %s\n", ep)

	// Set socket options
	if err := c.sock.SetSndtimeo(2 * time.Second); err != nil {
		return err
	}

	// Ensure the socket is closed on termination
	// go signalHandler(c.Close)

	return nil
}

// Close the connection to the remote peer
func (c *Client) Close() error {
	return c.sock.Close()
}

//===========================================================================
// Transport Methods
//===========================================================================

// Send a message to the remote peer
func (c *Client) Send(message string) error {
	c.count++ // Increment the number of sent messages

	pb := &msg.Message{
		Type:    msg.MessageType_SINGLE,
		Id:      c.count,
		Sender:  "client",
		Message: message,
	}

	data, err := proto.Marshal(pb)
	if err != nil {
		return err
	}

	if _, err = c.sock.SendBytes(data, zmq.DONTWAIT); err != nil {
		return err
	}

	// Wait for the reply
	reply, err := c.Recv()
	if err != nil {
		return err
	}

	info("received: %s\n", reply.String())
	return nil
}

// Recv a message from the remote peer, parsing prtocol buffers along the way.
func (c *Client) Recv() (*msg.Message, error) {
	data, err := c.sock.RecvBytes(0)
	if err != nil {
		return nil, fmt.Errorf("could not recv data: %s", err)
	}

	var message = new(msg.Message)
	if err = proto.Unmarshal(data, message); err != nil {
		return nil, fmt.Errorf("could not unmarshal data: %s", err)
	}

	return message, nil
}
