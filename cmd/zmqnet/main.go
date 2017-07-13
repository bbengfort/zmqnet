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

func exit(err error) error {
	return cli.NewExitError(fmt.Sprintf("fatal error: %s", err), 1)
}

func serve(c *cli.Context) error {
	network, err := zmqnet.New(c.String("peers"), c.String("name"))
	if err != nil {
		return exit(err)
	}

	server := network.Server()
	if err := server.Run(); err != nil {
		return exit(err)
	}

	return nil
}

//===========================================================================
// Client Commands
//===========================================================================

func send(c *cli.Context) error {
	defer zmq4.Term()

	network, err := zmqnet.New(c.String("peers"), c.String("name"))
	if err != nil {
		return exit(err)
	}

	client, err := network.Client(c.String("name"))
	if err != nil {
		return exit(err)
	}

	if err := client.Connect(); err != nil {
		return exit(err)
	}

	for _, msg := range c.Args() {
		if err := client.Send(msg); err != nil {
			return exit(err)
		}
	}

	return client.Close()
}
