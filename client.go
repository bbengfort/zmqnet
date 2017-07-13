package zmqnet

import (
	"time"

	zmq "github.com/pebbe/zmq4"
)

//===========================================================================
// Client for Remote Peers
//===========================================================================

// Client communicates with a remote peer.
type Client struct {
	Transporter
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

//===========================================================================
// Transport Methods
//===========================================================================

// Send a message to the remote peer
func (c *Client) Send(message string) error {
	if err := c.send(message); err != nil {
		return err
	}

	// Wait for the reply
	reply, err := c.recv()
	if err != nil {
		return err
	}

	info("received: %s\n", reply.String())
	return nil
}
