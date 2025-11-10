package circuitbreaker

import (
	"context"
	"embed"
	"io/fs"
	"net"
	"net/http"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/lightningequipment/circuitbreaker/circuitbreakerrpc"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

// maxGrpcMsgSize is used when we configure both server and clients to allow sending and
// receiving at most 32 MB GRPC messages.
//
// This value is based on the default number of forwarding history entries that we'll
// store in the database, as this is the largest query we currently make (~13 MB of data)
// plus some leeway for nodes that override this default to a larger value.
const maxGrpcMsgSize = 32 * 1024 * 1024

//go:embed all:webui-build
var content embed.FS

type RunConfig struct {
	DbPath          string
	FwdHistoryLimit int
	LndStubFlag     bool
	LndRpcServer    string
	LndTlsCertPath  string
	LndMacaroonPath string
	Logger          *zap.SugaredLogger
	GrpcListenAddr  string
	HttpListenAddr  string
	GetPreProcessor PreProcessorFactory
}

// Run starts the main application logic using the provided configuration.
func Run(ctx context.Context, c *RunConfig) error {
	var logger *zap.SugaredLogger

	// If no logger is provided, use default global one
	if c.Logger == nil {
		logger = log
	} else {
		logger = c.Logger
		// Overwrite global logger with provided one
		log = c.Logger
	}
	logger.Infow("Opening database", "path", c.DbPath)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Open database.
	db, err := NewDb(ctx, c.DbPath, c.FwdHistoryLimit)
	if err != nil {
		return err
	}
	defer func() {
		err := db.Close()
		if err != nil {
			logger.Errorw("Error closing db", "err", err)
		}
	}()

	var client lndclient
	if c.LndStubFlag {
		stubClient := newStubClient(ctx)

		client = stubClient
	} else {
		lndCfg := LndConfig{
			RpcServer:   c.LndRpcServer,
			TlsCertPath: c.LndTlsCertPath,
			MacPath:     c.LndMacaroonPath,
			Log:         logger,
		}

		lndClient, err := NewLndClient(&lndCfg)
		if err != nil {
			return err
		}
		defer lndClient.Close()

		client = lndClient
	}

	limits, err := db.GetLimits(ctx)
	if err != nil {
		return err
	}

	factory := c.GetPreProcessor
	if factory == nil {
		factory = DefaultPreProcessorFactory
	}
	p := NewProcess(client, logger, limits, db, factory)

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxGrpcMsgSize),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer()),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer()),
	)

	reflection.Register(grpcServer)

	server := NewServer(logger, p, client, db)

	circuitbreakerrpc.RegisterServiceServer(
		grpcServer, server,
	)
	grpcInternalListener, err := net.Listen("tcp", c.GrpcListenAddr)
	if err != nil {
		return err
	}

	// Create a client connection to the gRPC server we just started
	// This is where the gRPC-Gateway proxies the requests
	conn, err := grpc.DialContext(
		ctx,
		c.GrpcListenAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxGrpcMsgSize),
		),
	)
	if err != nil {
		return err
	}

	// Create http server.
	gwmux := runtime.NewServeMux()

	err = circuitbreakerrpc.RegisterServiceHandler(ctx, gwmux, conn)
	if err != nil {
		return err
	}

	serverRoot, err := fs.Sub(content, "webui-build")
	if err != nil {
		logger.Fatal(err)
	}

	fs := http.FileServer(http.FS(serverRoot))
	mux := http.NewServeMux()
	mux.Handle("/api/", http.StripPrefix("/api", gwmux))
	mux.HandleFunc("/", fs.ServeHTTP)

	gwServer := &http.Server{
		Addr:              c.HttpListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: time.Second * 10,
	}

	group, ctx := errgroup.WithContext(ctx)

	// Run circuitbreaker core.
	group.Go(func() error {
		return p.Run(ctx)
	})

	// Run grpc server.
	group.Go(func() error {
		logger.Infow("Grpc server starting", "listenAddress", c.GrpcListenAddr)
		err := grpcServer.Serve(grpcInternalListener)
		if err != nil && err != grpc.ErrServerStopped {
			logger.Errorw("grpc server error", "err", err)
		}

		return err
	})

	// Run http server.
	group.Go(func() error {
		logger.Infow("HTTP server starting", "listenAddress", c.HttpListenAddr)

		return gwServer.ListenAndServe()
	})

	// Stop servers when context is cancelled.
	group.Go(func() error {
		<-ctx.Done()

		// Stop http server.
		logger.Infof("Stopping http server")
		err := gwServer.Shutdown(context.Background()) //nolint:contextcheck
		if err != nil {
			logger.Errorw("Error shutting down http server", "err", err)
		}

		// Stop grpc server.
		logger.Infof("Stopping grpc server")
		grpcServer.Stop()

		return nil
	})

	return group.Wait()
}
