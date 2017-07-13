package zmqnet

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

//===========================================================================
// OS Signal Handlers
//===========================================================================

func signalHandler(shutdown func() error) {
	// Make signal channel and register notifiers for Interupt and Terminate
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)
	signal.Notify(sigchan, syscall.SIGTERM)

	// Block until we receive a signal on the channel
	<-sigchan

	// Defer the clean exit until the end of the function
	defer os.Exit(0)

	// Shutdown now that we've received the signal
	if err := shutdown(); err != nil {
		log.Printf("could not gracefully shutdown: %s\n", err)
		os.Exit(1)
	}

	// Declare graceful shutdown.
	log.Println("zmqnet has gracefully shutdown")
}
