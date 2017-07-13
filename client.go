package zmqnet

import (
	"fmt"

	"github.com/bbengfort/x/peers"
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
func (c *Client) Send(msg string) error {
	c.count++
	envelope := fmt.Sprintf("sent msg #%d from %s: %s", c.count, c.host.Name, msg)
	if _, err := c.sock.Send(envelope, 0); err != nil {
		return err
	}

	// Wait for the reply
	reply, err := c.sock.Recv(0)
	if err != nil {
		return err
	}

	info("received: %s\n", reply)
	return nil
}
