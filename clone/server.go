package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	zmq "github.com/pebbe/zmq4"
	"github.com/urfave/cli"
)

//===========================================================================
// Server Commands
//===========================================================================

func pub(c *cli.Context) error {
	var err error
	var context *zmq.Context
	var snapshot *zmq.Socket
	var publisher *zmq.Socket
	var collector *zmq.Socket
	var poller *zmq.Poller
	var sequence uint64
	state := make(map[string]*KVMsg)
	recv := make(chan error)

	// Listen for keyboard interupt
	notify := make(chan os.Signal)
	signal.Notify(notify)

	// Create the ZMQ Context and defer termination
	if context, err = zmq.NewContext(); err != nil {
		return exit(err)
	}
	defer zmq.Term()

	// Bind to the snapshot ROUTER socket
	if snapshot, err = context.NewSocket(zmq.ROUTER); err != nil {
		return exit(err)
	}
	endpoint := fmt.Sprintf("tcp://%s:%d", c.String("host"), c.Uint("snapshot"))
	if err = snapshot.Bind(endpoint); err != nil {
		return exit(err)
	}
	log.Printf("bound snapshot ROUTER socket to %s\n", endpoint)

	// Bind to the publisher PUB socket
	if publisher, err = context.NewSocket(zmq.PUB); err != nil {
		return exit(err)
	}
	endpoint = fmt.Sprintf("tcp://%s:%d", c.String("host"), c.Uint("publisher"))
	if err = publisher.Bind(endpoint); err != nil {
		return exit(err)
	}
	log.Printf("bound publisher PUB socket to %s\n", endpoint)

	// Bind to the collector PULL socket
	if collector, err = context.NewSocket(zmq.PULL); err != nil {
		return exit(err)
	}
	endpoint = fmt.Sprintf("tcp://%s:%d", c.String("host"), c.Uint("collector"))
	if err = collector.Bind(endpoint); err != nil {
		return exit(err)
	}
	log.Printf("bound collector PULL socket to %s\n", endpoint)

	// Create the Poller to poll on all our sockets
	poller = zmq.NewPoller()
	poller.Add(collector, zmq.POLLIN)
	poller.Add(snapshot, zmq.POLLIN)

	// Handler Function
	handler := func(recv chan<- error) {
		// Poll the sockets with a 1 second timeout
		items, err := poller.Poll(time.Second * 1)
		if err != nil {
			recv <- err
			return
		}

		// Go through each item to handle requests
		for _, item := range items {

			// Apply state update sent from client
			if item.Socket == collector {
				kvmsg, err := RecvKVMsg(collector)
				if err != nil {
					recv <- err
					return
				}

				sequence++
				kvmsg.sequence = sequence
				if err := kvmsg.Send(publisher); err != nil {
					recv <- err
					return
				}

				kvmsg.Store(state)
				log.Printf("I: publishing update %d\n", sequence)
			}

			// Execute state snapshot request
			if item.Socket == snapshot {
				msg, err := snapshot.RecvMessage(0)
				if err != nil {
					recv <- err
					return
				}

				identity := msg[0]
				request := msg[1]

				if request != "ICANHAZ?" {
					log.Fatal("E: bad request, aborting")
				}

				// Send each entry in state to client
				for _, val := range state {
					snapshot.Send(identity, zmq.SNDMORE)
					val.Send(snapshot)
				}

				// Send end with sequence number
				log.Printf("Sending state snapshot=%d\n", sequence)
				snapshot.Send(identity, zmq.SNDMORE)
				resp := &KVMsg{
					key:      "KTHXBAI",
					sequence: sequence,
					body:     []byte(""),
				}
				resp.Send(snapshot)
			}

		}

		recv <- nil
	}

	// Run the handler function
	go handler(recv)

	// Now poll for messages
outer:
	for {
		select {
		case <-notify:
			// Handle OS Signals
			break outer
		case err := <-recv:
			if err != nil {
				log.Fatal(err)
			} else {
				go handler(recv)
			}
		}
	}

	log.Printf("shutdown after handling %d messages\n", sequence)
	return nil
}
