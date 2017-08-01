package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"

	zmq "github.com/pebbe/zmq4"
	"github.com/urfave/cli"
)

//===========================================================================
// Client Commands
//===========================================================================

func sub(c *cli.Context) error {
	var err error
	var context *zmq.Context
	var snapshot *zmq.Socket
	var publisher *zmq.Socket
	var collector *zmq.Socket
	var poller *zmq.Poller
	var sequence uint64

	recv := make(chan error)
	state := make(map[string]*KVMsg)

	// Listen for keyboard interupt
	notify := make(chan os.Signal)
	signal.Notify(notify)

	// Create the ZMQ Context and defer termination
	if context, err = zmq.NewContext(); err != nil {
		return exit(err)
	}
	defer zmq.Term()

	// Bind to the snapshot DEALER socket
	if snapshot, err = context.NewSocket(zmq.DEALER); err != nil {
		return exit(err)
	}
	if err = snapshot.SetLinger(0); err != nil {
		return exit(err)
	}
	endpoint := fmt.Sprintf("tcp://%s:%d", c.String("host"), c.Uint("snapshot"))
	if err = snapshot.Connect(endpoint); err != nil {
		return exit(err)
	}
	log.Printf("connected snapshot DEALER socket to %s\n", endpoint)

	// Bind to the publisher SUB socket
	if publisher, err = context.NewSocket(zmq.SUB); err != nil {
		return exit(err)
	}
	if err = publisher.SetLinger(0); err != nil {
		return exit(err)
	}
	if err = publisher.SetSubscribe(""); err != nil {
		return exit(err)
	}
	endpoint = fmt.Sprintf("tcp://%s:%d", c.String("host"), c.Uint("publisher"))
	if err = publisher.Connect(endpoint); err != nil {
		return exit(err)
	}
	log.Printf("connected publisher SUB socket to %s\n", endpoint)

	// Bind to the collector PUSH socket
	if collector, err = context.NewSocket(zmq.PUSH); err != nil {
		return exit(err)
	}
	if err = collector.SetLinger(0); err != nil {
		return exit(err)
	}
	endpoint = fmt.Sprintf("tcp://%s:%d", c.String("host"), c.Uint("collector"))
	if err = collector.Connect(endpoint); err != nil {
		return exit(err)
	}
	log.Printf("connected collector PUSH socket to %s\n", endpoint)

	// Get a state snapshot
	snapshot.Send("ICANHAZ?", 0)
	for {
		kvmsg, err := RecvKVMsg(snapshot)
		if err != nil {
			return exit(err)
		}

		if kvmsg.key == "KTHXBAI" {
			sequence = kvmsg.sequence
			log.Printf("I: Received snapshot=%d\n", sequence)
			break
		}

		kvmsg.Store(state)
	}

	// Register the poller
	poller = zmq.NewPoller()
	poller.Add(publisher, zmq.POLLIN)

	// Handle subscription messages
	handler := func(recv chan<- error) {
		items, err := poller.Poll(time.Second)
		if err != nil {
			recv <- err
			return
		}

		for _, item := range items {
			if item.Socket == publisher {
				kvmsg, err := RecvKVMsg(publisher)
				if err != nil {
					recv <- err
					return
				}

				// Discard out of sequence messages
				if kvmsg.sequence > sequence {
					sequence = kvmsg.sequence
					kvmsg.Store(state)
					log.Printf("I: received update=%d\n", sequence)
				}
			}
		}

		recv <- nil
	}

	// Run the handler and generator
	go handler(recv)
	go generate(collector, state, recv)

	// Listen for subscriptions and timeouts for generating new data
outer:
	for {
		select {
		case <-notify:
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

func generate(sock *zmq.Socket, state map[string]*KVMsg, recv chan<- error) {

	kvmsg := &KVMsg{
		key:  fmt.Sprintf("%d", rand.Intn(10000)),
		body: []byte(fmt.Sprintf("%d", rand.Intn(1000000))),
	}
	if err := kvmsg.Send(sock); err != nil {
		recv <- err
		return
	}

	kvmsg.Store(state)
	time.AfterFunc(time.Second*1, func() { generate(sock, state, recv) })
}
