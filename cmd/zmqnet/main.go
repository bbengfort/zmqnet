package main

import (
	"fmt"
	"os"

	"github.com/bbengfort/zmqnet"
	"github.com/joho/godotenv"
	"github.com/pebbe/zmq4"
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
	app.Name = "zmqnet"
	app.Version = "0.1"
	app.Usage = "run the zmq network test platform"

	// Define commands available to the application
	app.Commands = []cli.Command{
		{
			Name:     "serve",
			Usage:    "run the zmq broadcast net",
			Category: "server",
			Action:   serve,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "p, peers",
					Usage:  "path to peers configuration",
					Value:  "",
					EnvVar: "PEERS_PATH",
				},
				cli.StringFlag{
					Name:  "n, name",
					Usage: "name of the replica to initialize",
					Value: "",
				},
			},
		},
		{
			Name:     "send",
			Usage:    "send a message to the server ",
			Category: "client",
			Action:   send,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "p, peers",
					Usage:  "path to peers configuration",
					Value:  "",
					EnvVar: "PEERS_PATH",
				},
				cli.StringFlag{
					Name:  "n, name",
					Usage: "name of the replica to initialize",
					Value: "",
				},
			},
		},
	}

	// Run the CLI program
	app.Run(os.Args)
}

//===========================================================================
// Server Commands
//===========================================================================

func serve(c *cli.Context) error {
	defer zmq4.Term()

	server, err := zmqnet.NewServer(c.String("peers"), c.String("name"))
	if err != nil {
		return cli.NewExitError(fmt.Sprintf("fatal error: %s", err), 1)
	}

	ctx, err := zmq4.NewContext()
	if err != nil {
		return cli.NewExitError(fmt.Sprintf("fatal error: %s", err), 1)
	}

	if err := server.Run(ctx); err != nil {
		return cli.NewExitError(fmt.Sprintf("fatal error: %s", err), 1)
	}

	return nil
}

//===========================================================================
// Client Commands
//===========================================================================

func send(c *cli.Context) error {
	defer zmq4.Term()

	client, err := zmqnet.NewClient(c.String("peers"), c.String("name"))
	if err != nil {
		return cli.NewExitError(fmt.Sprintf("fatal error: %s", err), 1)
	}

	ctx, err := zmq4.NewContext()
	if err != nil {
		return cli.NewExitError(fmt.Sprintf("fatal error: %s", err), 1)
	}

	if err := client.Connect(ctx); err != nil {
		return cli.NewExitError(fmt.Sprintf("fatal error: %s", err), 1)
	}

	for _, msg := range c.Args() {
		if err := client.Send(msg); err != nil {
			return cli.NewExitError(fmt.Sprintf("fatal error: %s", err), 1)
		}
	}

	return nil
}
