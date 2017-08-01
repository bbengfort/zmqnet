package main

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
	"github.com/urfave/cli"
)

//===========================================================================
// Main Method
//===========================================================================

func main() {

	// Load the .env file if it exists
	godotenv.Load()

	// Instantiate the command line application
	app := cli.NewApp()
	app.Name = "zmqmsg"
	app.Version = "0.1"
	app.Usage = "publish and clone key/value state using zmq4"

	// Define commands available to the application
	app.Commands = []cli.Command{
		{
			Name:     "pub",
			Usage:    "run the zmq state publisher",
			Category: "server",
			Action:   pub,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "H, host",
					Usage: "host to bind the socket on",
					Value: "*",
				},
				cli.UintFlag{
					Name:  "s, snapshot",
					Usage: "port to bind ROUTER on to get snapshots",
					Value: 3264,
				},
				cli.UintFlag{
					Name:  "p, publisher",
					Usage: "port to bind PUB on for state updates",
					Value: 3265,
				},
				cli.UintFlag{
					Name:  "c, collector",
					Usage: "port to bind PULL on for client updates",
					Value: 3266,
				},
			},
		},
		{
			Name:     "sub",
			Usage:    "subscribe to the zmq state publisher",
			Category: "client",
			Action:   sub,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "H, host",
					Usage: "host to connect to the server on",
					Value: "localhost",
				},
				cli.UintFlag{
					Name:  "s, snapshot",
					Usage: "port to bind DEALER on for snapshots",
					Value: 3264,
				},
				cli.UintFlag{
					Name:  "p, publisher",
					Usage: "port to bind SUB on for state updates",
					Value: 3265,
				},
				cli.UintFlag{
					Name:  "c, collector",
					Usage: "port to bind PUSH on to send client updates",
					Value: 3266,
				},
			},
		},
	}

	// Run the CLI program
	app.Run(os.Args)
}

// Helper function for CLI errors
func exit(err error) error {
	if err != nil {
		return cli.NewExitError(fmt.Sprintf("fatal error: %s", err), 1)
	}
	return nil
}
