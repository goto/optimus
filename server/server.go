package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/goto/salt/log"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpctags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/goto/optimus/config"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

const (
	shutdownWait       = 30 * time.Second
	GRPCMaxRecvMsgSize = 128 << 20 // 128MB
	GRPCMaxSendMsgSize = 128 << 20 // 128MB

	DialTimeout      = time.Second * 5
	BootstrapTimeout = time.Second * 10

	GRPCKeepaliveMinTime = 2 * time.Minute
)

func checkRequiredConfigs(conf config.Serve) error {
	errRequiredMissing := errors.New("required config missing")
	if conf.IngressHost == "" {
		return fmt.Errorf("serve.ingress_host: %w", errRequiredMissing)
	}
	if conf.IngressHostGRPC == "" {
		return fmt.Errorf("serve.ingress_host_grpc: %w", errRequiredMissing)
	}
	if conf.DB.DSN == "" {
		return fmt.Errorf("serve.db.dsn: %w", errRequiredMissing)
	}
	if parsed, err := url.Parse(conf.DB.DSN); err != nil {
		return fmt.Errorf("failed to parse serve.db.dsn: %w", err)
	} else if parsed.Scheme != "postgres" {
		return errors.New("unsupported database scheme, use 'postgres'")
	}
	return nil
}

func setupGRPCServer(l log.Logger) (*grpc.Server, error) {
	// Logrus entry is used, allowing pre-definition of certain fields by the user.
	grpcLogLevel, err := logrus.ParseLevel(l.Level())
	if err != nil {
		return nil, err
	}
	grpcLogrus := logrus.New()
	grpcLogrus.SetLevel(grpcLogLevel)
	grpcLogrusEntry := logrus.NewEntry(grpcLogrus)
	// Shared options for the logger, with a custom gRPC code to log level function.
	opts := []grpc_logrus.Option{
		grpc_logrus.WithLevels(grpc_logrus.DefaultCodeToLevel),
	}
	// Make sure that log statements internal to gRPC library are logged using the logrus logger as well.
	grpc_logrus.ReplaceGrpcLogger(grpcLogrusEntry)

	recoverPanic := func(p interface{}) (err error) {
		return status.Error(codes.Unknown, fmt.Sprintf("panic is triggered: %v", p))
	}
	grpcOpts := []grpc.ServerOption{
		grpc_middleware.WithUnaryServerChain(
			grpctags.UnaryServerInterceptor(grpctags.WithFieldExtractor(grpctags.CodeGenRequestFieldExtractor)),
			grpc_logrus.UnaryServerInterceptor(grpcLogrusEntry, opts...),
			otelgrpc.UnaryServerInterceptor(),
			grpc_prometheus.UnaryServerInterceptor,
			grpc_recovery.UnaryServerInterceptor(grpc_recovery.WithRecoveryHandler(recoverPanic)),
		),
		grpc_middleware.WithStreamServerChain(
			otelgrpc.StreamServerInterceptor(),
			grpc_logrus.StreamServerInterceptor(grpcLogrusEntry, opts...),
			grpc_prometheus.StreamServerInterceptor,
			grpc_recovery.StreamServerInterceptor(grpc_recovery.WithRecoveryHandler(recoverPanic)),
		),
		grpc.MaxRecvMsgSize(GRPCMaxRecvMsgSize),
		grpc.MaxSendMsgSize(GRPCMaxSendMsgSize),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             GRPCKeepaliveMinTime, // MinTime client should wait until next ping as in client side send ping every 1 Minute
			PermitWithoutStream: true,                 // PermitWithoutStream send ping even without active stream
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    1 * time.Minute, // Ping the client if it is idle for 1 minute to ensure the connection is still active
			Timeout: 1 * time.Second, // Wait 1 second for the ping ack before assuming the connection is dead
		}),
	}
	grpcServer := grpc.NewServer(grpcOpts...)

	hsrv := health.NewServer()
	hsrv.SetServingStatus("Optimus", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, hsrv)

	reflection.Register(grpcServer)
	return grpcServer, nil
}

func prepareHTTPProxy(httpAddr, grpcAddr string) (*http.Server, func(), error) {
	timeoutGrpcDialCtx, grpcDialCancel := context.WithTimeout(context.Background(), DialTimeout)
	defer grpcDialCancel()

	// prepare http proxy
	gwmux := runtime.NewServeMux(
		runtime.WithErrorHandler(runtime.DefaultHTTPErrorHandler),
	)
	// gRPC dialup options to proxy http connections
	grpcConn, err := grpc.DialContext(timeoutGrpcDialCtx, grpcAddr, []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(GRPCMaxRecvMsgSize),
			grpc.MaxCallSendMsgSize(GRPCMaxSendMsgSize),
		),
	}...)
	if err != nil {
		return nil, func() {}, fmt.Errorf("grpc.DialContext: %w", err)
	}
	runtimeCtx, runtimeCancel := context.WithCancel(context.Background())
	cleanup := func() {
		runtimeCancel()
	}

	if err := pb.RegisterRuntimeServiceHandler(runtimeCtx, gwmux, grpcConn); err != nil {
		return nil, cleanup, fmt.Errorf("RegisterRuntimeServiceHandler: %w", err)
	}
	if err := pb.RegisterJobSpecificationServiceHandler(runtimeCtx, gwmux, grpcConn); err != nil {
		return nil, cleanup, fmt.Errorf("RegisterJobSpecificationServiceHandler: %w", err)
	}
	if err := pb.RegisterJobRunServiceHandler(runtimeCtx, gwmux, grpcConn); err != nil {
		return nil, cleanup, fmt.Errorf("RegisterJobRunServiceHandler: %w", err)
	}
	if err := pb.RegisterProjectServiceHandler(runtimeCtx, gwmux, grpcConn); err != nil {
		return nil, cleanup, fmt.Errorf("RegisterProjectServiceHandler: %w", err)
	}
	if err := pb.RegisterNamespaceServiceHandler(runtimeCtx, gwmux, grpcConn); err != nil {
		return nil, cleanup, fmt.Errorf("RegisterNamespaceServiceHandler: %w", err)
	}
	if err := pb.RegisterReplayServiceHandler(runtimeCtx, gwmux, grpcConn); err != nil {
		return nil, cleanup, fmt.Errorf("RegisterReplayServiceHandler: %w", err)
	}
	if err := pb.RegisterBackupServiceHandler(runtimeCtx, gwmux, grpcConn); err != nil {
		return nil, cleanup, fmt.Errorf("RegisterBackupServiceHandler: %w", err)
	}
	if err := pb.RegisterResourceServiceHandler(runtimeCtx, gwmux, grpcConn); err != nil {
		return nil, cleanup, fmt.Errorf("RegisterResourceServiceHandler: %w", err)
	}
	if err := pb.RegisterSecretServiceHandler(runtimeCtx, gwmux, grpcConn); err != nil {
		return nil, cleanup, fmt.Errorf("RegisterSecretServiceHandler: %w", err)
	}

	// base router
	baseMux := http.NewServeMux()
	baseMux.HandleFunc("/ping", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, "pong")
	})
	baseMux.Handle("/api/", otelhttp.NewHandler(http.StripPrefix("/api", gwmux), "api"))

	//nolint: mnd
	srv := &http.Server{
		Handler:      baseMux,
		Addr:         httpAddr,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Minute,
	}

	return srv, cleanup, nil
}
