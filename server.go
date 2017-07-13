package zmqnet

import (
	"fmt"
	"log"
	"time"

	"github.com/bbengfort/x/peers"
	"github.com/pebbe/zmq4"
)

//===========================================================================
// Server and HTTP Transport
//===========================================================================

// Server responds to requests from other peers.
type Server struct {
	host  *peers.Peer  // the host and address information of the server
	sock  *zmq4.Socket // the socket that the server is bound on
	done  chan bool    // shutdown semaphore channel
	msgs  chan string  // channel to receive ZMQ messages on
	count uint64       // number of messages received
}

// Run the server and listen for messages
func (s *Server) Run(ctx *zmq4.Context) (err error) {
	// Create the socket
	if s.sock, err = ctx.NewSocket(zmq4.REP); err != nil {
		return err
	}

	// Bind the socket and run the listener
	ep := s.host.ZMQEndpoint(true)
	s.sock.Bind(ep)
	log.Printf("bound to %s\n", ep)
	go s.listen()

	// Ensure the socket is closed on termination
	go signalHandler(s.Shutdown)

	// Wait for messages or the shutdown signal
outer:
	for {
		select {
		case done := <-s.done:
			if done {
				break outer
			}
		case msg := <-s.msgs:
			s.handle(msg)
		}
	}

	return nil
}

// Shutdown the server and clean up the socket
func (s *Server) Shutdown() error {
	log.Printf("shutting down")
	s.done <- true
	s.sock.Close()
	zmq4.Term()
	return nil
}

func (s *Server) listen() {
	for {

		msg, _ := s.sock.Recv(0)
		if msg != "" {
			s.count++
			s.msgs <- string(msg)
		}

	}
}

func (s *Server) handle(msg string) {
	log.Printf("received: %s\n", msg)
	time.Sleep(time.Second)
	reply := fmt.Sprintf("reply msg #%d from %s", s.count, s.host.Name)
	s.sock.Send(reply, 0)
}
