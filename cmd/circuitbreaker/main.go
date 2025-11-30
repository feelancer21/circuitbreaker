package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/btcsuite/btcd/btcutil"
	cb "github.com/lightningequipment/circuitbreaker"
	"github.com/urfave/cli"
	"golang.org/x/sync/errgroup"
)

const (
	defaultDataDir          = "data"
	defaultChainSubDir      = "chain"
	defaultTLSCertFilename  = "tls.cert"
	defaultMacaroonFilename = "admin.macaroon"

	chain = "bitcoin"
	dbFn  = "circuitbreaker.db"
)

var (
	defaultLndDir      = btcutil.AppDataDir("lnd", false)
	defaultTLSCertPath = filepath.Join(defaultLndDir, defaultTLSCertFilename)

	defaultRPCPort     = "10009"
	defaultRPCHostPort = "localhost:" + defaultRPCPort

	httpListenFlag = cli.StringFlag{
		Name:  "httplisten",
		Value: "127.0.0.1:9235",
		Usage: "http server listen address",
	}

	stubFlag = cli.BoolFlag{
		Name:  "stub",
		Usage: "set to enable stub mode (no lnd instance connected)",
	}

	errUserExit = errors.New("user requested termination")
)

// extractPathArgs parses the TLS certificate and macaroon paths from the
// command.
func extractPathArgs(ctx *cli.Context) (string, string, error) {
	network := strings.ToLower(ctx.GlobalString("network"))
	switch network {
	case "mainnet", "testnet", "regtest", "simnet", "signet", "testnet4":
	default:
		return "", "", fmt.Errorf("unknown network: %v", network)
	}

	// We'll now fetch the lnddir so we can make a decision  on how to
	// properly read the macaroons (if needed) and also the cert. This will
	// either be the default, or will have been overwritten by the end
	// user.
	lndDir := cleanAndExpandPath(ctx.GlobalString("lnddir"))

	// If the macaroon path as been manually provided, then we'll only
	// target the specified file.
	var macPath string
	if ctx.GlobalString("macaroonpath") != "" {
		macPath = cleanAndExpandPath(ctx.GlobalString("macaroonpath"))
	} else {
		// Otherwise, we'll go into the path:
		// lnddir/data/chain/<chain>/<network> in order to fetch the
		// macaroon that we need.
		macPath = filepath.Join(
			lndDir, defaultDataDir, defaultChainSubDir, chain,
			network, defaultMacaroonFilename,
		)
	}

	tlsCertPath := cleanAndExpandPath(ctx.GlobalString("tlscertpath"))

	// If a custom lnd directory was set, we'll also check if custom paths
	// for the TLS cert and macaroon file were set as well. If not, we'll
	// override their paths so they can be found within the custom lnd
	// directory set. This allows us to set a custom lnd directory, along
	// with custom paths to the TLS cert and macaroon file.
	if lndDir != defaultLndDir {
		tlsCertPath = filepath.Join(lndDir, defaultTLSCertFilename)
	}

	return tlsCertPath, macPath, nil
}

func main() {
	defaultAppDir := btcutil.AppDataDir("circuitbreaker", false)

	app := cli.NewApp()
	app.Name = "circuitbreakerd"
	app.Version = cb.BuildVersion
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "rpcserver",
			Value: defaultRPCHostPort,
			Usage: "host:port of ln daemon",
		},
		cli.StringFlag{
			Name:  "lnddir",
			Value: defaultLndDir,
			Usage: "path to lnd's base directory",
		},
		cli.StringFlag{
			Name:  "tlscertpath",
			Value: defaultTLSCertPath,
			Usage: "path to TLS certificate",
		},
		cli.StringFlag{
			Name: "network, n",
			Usage: "the network lnd is running on e.g. mainnet, " +
				"testnet, etc.",
			Value: "mainnet",
		},
		cli.StringFlag{
			Name:  "macaroonpath",
			Usage: "path to macaroon file",
		},
		cli.StringFlag{
			Name:  "configdir",
			Value: defaultAppDir,
			Usage: "path to CircuitBreaker's base directory",
		},
		cli.StringFlag{
			Name:  "listen",
			Value: "127.0.0.1:9234",
			Usage: "grpc server listen address",
		},
		cli.Uint64Flag{
			Name:  "fwdhistorylimit",
			Usage: "limit the number of htlc forwards that are persisted",
			Value: cb.DefaultFwdHistoryLimit,
		},
		httpListenFlag,
		stubFlag,
	}

	app.Action = run

	if err := app.Run(os.Args); err != nil && err != errUserExit {
		log.Errorw("Unexpected exit", "err", err)

		// Return a non-zero error code. This is particularly important when
		// running in docker-compose. A zero error code wouldn't trigger a
		// restart.
		os.Exit(1)
	}
}

// cleanAndExpandPath expands environment variables and leading ~ in the
// passed path, cleans the result, and returns it.
// This function is taken from https://github.com/btcsuite/btcd
func cleanAndExpandPath(path string) string {
	if path == "" {
		return ""
	}

	// Expand initial ~ to OS specific home directory.
	if strings.HasPrefix(path, "~") {
		var homeDir string
		user, err := user.Current()
		if err == nil {
			homeDir = user.HomeDir
		} else {
			homeDir = os.Getenv("HOME")
		}

		path = strings.Replace(path, "~", homeDir, 1)
	}

	// NOTE: The os.ExpandEnv doesn't work with Windows-style %VARIABLE%,
	// but the variables can still be expanded via POSIX-style $VARIABLE.
	return filepath.Clean(os.ExpandEnv(path))
}

// run is the main entry point for the circuitbreaker application from the CLI.
// It parses the configuration and starts the main application.
func run(c *cli.Context) error {
	log.Infow("Circuit Breaker starting", "version", cb.BuildVersion)
	log.Sync()

	cfg := cb.RunConfig{
		FwdHistoryLimit: c.Int("fwdhistorylimit"),
		LndStubFlag:     c.Bool(stubFlag.Name),
		GrpcListenAddr:  c.String("listen"),
		HttpListenAddr:  c.String(httpListenFlag.Name),
		GetPreProcessor: cb.DefaultPreProcessorFactory,
	}

	confDir := c.String("configdir")
	err := os.MkdirAll(confDir, os.ModePerm)
	if err != nil {
		return err
	}
	cfg.DbPath = filepath.Join(confDir, dbFn)

	if !cfg.LndStubFlag {
		// First, we'll parse the args from the command.
		cfg.LndTlsCertPath, cfg.LndMacaroonPath, err = extractPathArgs(c)
		if err != nil {
			return err
		}
		cfg.LndRpcServer = c.GlobalString("rpcserver")
	}

	group, ctx := errgroup.WithContext(context.Background())

	// Run main application.
	group.Go(func() error {
		return cb.Run(ctx, &cfg)
	})

	group.Go(func() error {
		log.Infof("Press ctrl-c to exit")

		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

		select {
		case <-sigint:
			return errUserExit

		case <-ctx.Done():
			return nil
		}
	})

	return group.Wait()
}
