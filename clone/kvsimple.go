package main

import (
	"encoding/binary"

	zmq "github.com/pebbe/zmq4"
)

// RecvKVMsg collects and parses a message from the specified socket
func RecvKVMsg(sock *zmq.Socket) (*KVMsg, error) {
	parts, err := sock.RecvMessageBytes(0)
	if err != nil {
		return nil, err
	}

	msg := &KVMsg{}
	msg.key = string(parts[0])
	msg.sequence = binary.LittleEndian.Uint64(parts[1])
	msg.body = parts[2]

	return msg, nil
}

// KVMsg is a simple Key/Value message struct
type KVMsg struct {
	key      string
	sequence uint64
	body     []byte
}

// Store the message in a state machine
func (m *KVMsg) Store(state map[string]*KVMsg) {
	if m.key != "" && m.body != nil {
		state[m.key] = m
	}
}

// Send the message to the specified socket
func (m *KVMsg) Send(sock *zmq.Socket) error {
	// Convert the sequence to bytes
	seq := make([]byte, 8)
	binary.LittleEndian.PutUint64(seq, m.sequence)

	// Send the multipart message on the socket
	_, err := sock.SendMessage([]byte(m.key), seq, m.body)
	return err
}

//
