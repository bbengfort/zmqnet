package main

import (
	"fmt"
	"os"
	"time"

	"github.com/bbengfort/zmqnet"
	pb "github.com/bbengfort/zmqnet/msg"
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
	app.Version = "0.2"
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
					Name:   "n, name",
					Usage:  "name of the replica to initialize",
					Value:  "",
					EnvVar: "ALIA_REPLICA_NAME",
				},
				cli.StringFlag{
					Name:   "u, uptime",
					Usage:  "pass a parsable duration to shut the server down after",
					EnvVar: "ALIA_SERVER_UPTIME",
				},
				cli.UintFlag{
					Name:   "verbosity",
					Usage:  "set log level from 0-4, lower is more verbose",
					Value:  2,
					EnvVar: "ALIA_VERBOSITY",
				},
			},
		},
		{
			Name:     "send",
			Usage:    "send a message to the server",
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
					Name:   "n, name",
					Usage:  "name of the replica to connect to",
					Value:  "",
					EnvVar: "KILO_LEADER_NAME",
				},
				cli.StringFlag{
					Name:  "d, delay",
					Usage: "parsable duration to delay between messages",
					Value: "0s",
				},
				cli.StringFlag{
					Name:   "t, timeout",
					Usage:  "recv timeout for each message",
					Value:  "2s",
					EnvVar: "KILO_TIMEOUT",
				},
				cli.IntFlag{
					Name:   "r, retries",
					Usage:  "number of retries before quitting",
					Value:  3,
					EnvVar: "KILO_RETRIES",
				},
			},
		},
		{
			Name:     "bench",
			Usage:    "run throughput benchmarks",
			Category: "client",
			Action:   bench,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "p, peers",
					Usage:  "path to peers configuration",
					Value:  "",
					EnvVar: "PEERS_PATH",
				},
				cli.StringFlag{
					Name:   "n, name",
					Usage:  "name of the replica to connect to",
					Value:  "",
					EnvVar: "KILO_LEADER_NAME",
				},
				cli.StringFlag{
					Name:   "d, duration",
					Usage:  "parsable duration of the benchmark",
					Value:  "30s",
					EnvVar: "KILO_BENCH_DURATION",
				},
				cli.StringFlag{
					Name:   "t, timeout",
					Usage:  "recv timeout for each message",
					Value:  "5s",
					EnvVar: "KILO_TIMEOUT",
				},
				cli.IntFlag{
					Name:   "r, retries",
					Usage:  "number of retries before quitting",
					Value:  3,
					EnvVar: "KILO_RETRIES",
				},
				cli.IntFlag{
					Name:  "c, clients",
					Usage: "extra information: number of clients",
				},
				cli.StringFlag{
					Name:   "o, results",
					Usage:  "path to write the results to",
					Value:  "results.json",
					EnvVar: "KILO_RESULTS_PATH",
				},
				cli.BoolFlag{
					Name:   "X, disabled",
					Usage:  "exit without running benchmarks",
					EnvVar: "KILO_DISABLED",
				},
				cli.UintFlag{
					Name:   "verbosity",
					Usage:  "set log level from 0-4, lower is more verbose",
					Value:  2,
					EnvVar: "ALIA_VERBOSITY",
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
	defer zmq4.Term()

	// Set the debug log level
	verbose := c.Uint("verbosity")
	zmqnet.SetLogLevel(uint8(verbose))

	// Create the network
	network, err := zmqnet.New(c.String("peers"), c.String("name"))
	if err != nil {
		return exit(err)
	}

	// If uptime is specified, set a fixed duration for the server to run.
	if uptime := c.String("uptime"); uptime != "" {
		d, err := time.ParseDuration(uptime)
		if err != nil {
			return err
		}

		time.AfterFunc(d, func() {
			zmq4.Term()
			os.Exit(0)
		})
	}

	// Run the network server and broadcast clients
	if err := network.Run(); err != nil {
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

	if err = client.Connect(); err != nil {
		return exit(err)
	}

	var delay time.Duration
	if delay, err = time.ParseDuration(c.String("delay")); err != nil {
		return exit(err)
	}

	var timeout time.Duration
	if timeout, err = time.ParseDuration(c.String("timeout")); err != nil {
		return exit(err)
	}

	for _, msg := range c.Args() {
		if err := client.Send(msg, pb.MessageType_SINGLE, c.Int("retries"), timeout); err != nil {
			return exit(err)
		}
		if delay != 0 {
			time.Sleep(delay)
		}
	}

	return client.Close()
}

func bench(c *cli.Context) error {
	if c.Bool("disabled") {
		fmt.Println("this client is disabled, exiting")
		return nil
	}

	// Set the debug log level
	verbose := c.Uint("verbosity")
	zmqnet.SetLogLevel(uint8(verbose))

	defer zmq4.Term()

	network, err := zmqnet.New(c.String("peers"), c.String("name"))
	if err != nil {
		return exit(err)
	}

	client, err := network.Client(c.String("name"))
	if err != nil {
		return exit(err)
	}

	if err = client.Connect(); err != nil {
		return exit(err)
	}
	defer client.Close()

	var duration time.Duration
	if duration, err = time.ParseDuration(c.String("duration")); err != nil {
		return exit(err)
	}

	var timeout time.Duration
	if timeout, err = time.ParseDuration(c.String("timeout")); err != nil {
		return exit(err)
	}

	nClients := c.Int("clients")
	retries := c.Int("retries")
	results := c.String("results")

	return client.Benchmark(duration, results, retries, timeout, nClients)
}
