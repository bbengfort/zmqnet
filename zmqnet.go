// Package zmqnet is a test for a fully connected zmq network.
package zmqnet

import (
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/bbengfort/x/peers"
	zmq "github.com/pebbe/zmq4"
)

//===========================================================================
// Package Initialization
//===========================================================================

// Initialize the package and random numbers, etc.
func init() {
	// Set the random seed to something different each time.
	rand.Seed(time.Now().Unix())

	// Initialize our debug logging with our prefix
	logger = log.New(os.Stdout, "[zmqnet] ", log.Lmicroseconds)
}

//===========================================================================
// Instantiate Network
//===========================================================================

// New accepts a path to the peers.json file  and the name of the local host.
func New(hosts, name string) (network *Network, err error) {
	// Initialize the network and data structures
	network = new(Network)

	// Load the peers file
	if network.peers, err = peers.LoadFrom(hosts); err != nil {
		return nil, err
	}

	//  Find the name of the local peer
	if network.local, err = network.peers.Get(name); err != nil {
		return nil, err
	}

	// Initialize the remotes
	network.remotes = make([]*Client, 0, len(network.peers.Peers))

	// Initialize the context
	if network.context, err = zmq.NewContext(); err != nil {
		return nil, err
	}

	return network, nil
}

// Network wraps a peers.json file and constructs zmq connections.
type Network struct {
	context *zmq.Context // Messaging context for the server and clients
	local   *peers.Peer  // Localhost on the network
	peers   *peers.Peers // Neighbors on the network
	server  *Server      // Local message responder
	remotes []*Client    // Clients for remote hosts on the network
}

// Server constructs and returns a server for the local network host.
func (n *Network) Server() *Server {
	if n.server == nil {
		n.server = new(Server)
		n.server.Init(n.local, n)
	}

	return n.server
}

// Client constructs and returns a client for the remote network host.
func (n *Network) Client(host string) (*Client, error) {
	// Find the specified host
	peer, err := n.peers.Get(host)
	if err != nil {
		return nil, err
	}

	// Create the client for the host
	client := new(Client)
	client.Init(peer, n)

	// Append the client to the list of clients
	n.remotes = append(n.remotes, client)
	return client, nil
}
