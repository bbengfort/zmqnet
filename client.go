package zmqnet

import (
	"fmt"
	"log"

	"github.com/bbengfort/x/peers"
	"github.com/pebbe/zmq4"
)

//===========================================================================
// Client for Remote Peers
//===========================================================================

// Client communicates with a remote peer.
type Client struct {
	host  *peers.Peer  // the host and address information of the server
	sock  *zmq4.Socket // the socket that the client is connected to
	count uint64       // number of messages sent
}

// Connect to the remote peer
func (c *Client) Connect(ctx *zmq4.Context) (err error) {
	// Create the socket
	if c.sock, err = ctx.NewSocket(zmq4.REQ); err != nil {
		return err
	}

	// Connect to the server
	ep := c.host.ZMQEndpoint(false)
	if err = c.sock.Connect(ep); err != nil {
		return err
	}

	log.Printf("connected to %s\n", ep)

	// Ensure the socket is closed on termination
	go signalHandler(c.Close)

	return nil
}

// Close the connection to the remote peer
func (c *Client) Close() error {
	return c.sock.Close()
}

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

	log.Printf("received: %s\n", reply)
	return nil
}
