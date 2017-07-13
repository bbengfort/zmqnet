// Package zmqnet is a test for a fully connected zmq network.
package zmqnet

import (
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/bbengfort/x/peers"
	pb "github.com/bbengfort/zmqnet/msg"
	zmq "github.com/pebbe/zmq4"
)

// Timeout for all broadcast messages
const Timeout = 2 * time.Second

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
func New(hosts, local string) (network *Network, err error) {
	// Initialize the network and data structures
	network = new(Network)

	// Load the peers file
	if network.peers, err = peers.LoadFrom(hosts); err != nil {
		return nil, err
	}

	//  Find the name of the local peer
	if network.local, err = network.peers.Get(local); err != nil {
		return nil, err
	}

	// Initialize the remotes
	network.remotes = make([]*Client, 0, len(network.peers.Peers)-1)

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

// Run the network server and connect the clients for a complete broadcast
// network across all clients.
func (n *Network) Run() error {
	if err := n.Connect(); err != nil {
		return err
	}

	return n.Server().Run()
}

// Connect connects all peer clients and creates transporters for each.
// NOTE: this function does not run the local server.
func (n *Network) Connect() error {
	// Connect all the clients and add them to remotes
	for _, peer := range n.peers.Peers {
		// Don't connect to the local
		if peer == n.local {
			continue
		}

		// Create the client for the peer
		client := new(Client)
		client.Init(peer, n)

		// Append the client to the remotes
		n.remotes = append(n.remotes, client)

		// Connect the client
		if err := client.Connect(); err != nil {
			return err
		}
	}

	return nil
}

// Shutdown the sever and close connections to all clients.
func (n *Network) Shutdown() error {
	if n.server != nil {
		if err := n.server.Shutdown(); err != nil {
			return err
		}
	}

	for _, client := range n.remotes {
		if err := client.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Broadcast a message to all remote hosts.
func (n *Network) Broadcast(message string) error {
	// Send messages to all clients
	for _, client := range n.remotes {
		go client.send(message, pb.MessageType_BOUNCE)
	}

	// Create poller and add poll sockets
	poller := zmq.NewPoller()
	for _, client := range n.remotes {
		poller.Add(client.sock, zmq.POLLIN)
	}

	// Process messages from clients
	sockets, _ := poller.PollAll(Timeout)
	for idx, socket := range sockets {

		// Check to see if we got a reply from the server.
		if socket.Events&zmq.POLLIN != 0 {
			// We got a reply from the remote
			msg, err := n.remotes[idx].recv()
			if err != nil {
				return err
			}
			info(msg.String())
		} else {
			warn("could not broadcast message to %s", n.remotes[idx].host.Name)
			if err := n.remotes[idx].Reset(); err != nil {
				return err
			}
		}
	}

	return nil
}

// Server constructs and returns a server for the local network host.
func (n *Network) Server() *Server {
	if n.server == nil {
		n.server = new(Server)
		n.server.Init(n.local, n)
	}

	return n.server
}

// Client constructs and returns a client for the remote network host. This
// method remains mostly as a helper feature for clients that want to connect
// to the broadcast network without initializing the network on their own.
func (n *Network) Client(host string) (*Client, error) {
	// Find the specified host
	peer, err := n.peers.Get(host)
	if err != nil {
		return nil, err
	}

	// Create the client for the host
	client := new(Client)
	client.Init(peer, n)
	return client, nil
}
