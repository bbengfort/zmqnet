// Package zmqnet is a test for a fully connected zmq network.
package zmqnet

import "github.com/bbengfort/x/peers"

// NewServer constructs a server from the given peers.json and the name of
// the server to run.
func NewServer(hosts, name string) (*Server, error) {
	peers, err := peers.LoadFrom(hosts)
	if err != nil {
		return nil, err
	}

	server := new(Server)
	if server.host, err = peers.Get(name); err != nil {
		return nil, err
	}

	return server, nil
}

// NewClient constructs a client from the given peers.json and name of the
// server to connect to.
func NewClient(hosts, name string) (*Client, error) {
	peers, err := peers.LoadFrom(hosts)
	if err != nil {
		return nil, err
	}

	client := new(Client)
	if client.host, err = peers.Get(name); err != nil {
		return nil, err
	}

	return client, nil
}
