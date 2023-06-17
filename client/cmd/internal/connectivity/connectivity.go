package connectivity

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/MakeNowJust/heredoc"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/goto/optimus/client/cmd/internal/auth"
)

const (
	grpcMaxClientSendSize      = 128 << 20 // 128MB
	grpcMaxClientRecvSize      = 128 << 20 // 128MB
	grpcMaxRetry          uint = 3

	optimusDialTimeout = time.Second * 2
	backoffDuration    = 100 * time.Millisecond
)

var errServerNotReachable = func(host string) error {
	return errors.New(heredoc.Docf(`Unable to reach optimus server at %s, this can happen due to following reasons:
		1. Check if you are connected to internet
		2. Is the host correctly configured in optimus config
		3. Is Optimus server currently unreachable`, host))
}

// Connectivity defines client connection to a targeted server host
type Connectivity struct {
	requestCtx       context.Context //nolint:containedctx
	cancelRequestCtx func()

	connection *grpc.ClientConn
	auth       *auth.Auth
}

// NewConnectivity initializes client connection
func NewConnectivity(serverHost string, requestTimeout time.Duration, a *auth.Auth) (*Connectivity, error) {
	connection, err := createConnection(serverHost, a)
	if err != nil {
		return nil, err
	}

	reqCtx, reqCancel := context.WithTimeout(context.Background(), requestTimeout)
	return &Connectivity{
		requestCtx:       reqCtx,
		cancelRequestCtx: reqCancel,
		connection:       connection,
	}, nil
}

// GetContext gets request context
func (c *Connectivity) GetContext() context.Context {
	return c.requestCtx
}

// GetConnection gets client connection
func (c *Connectivity) GetConnection() *grpc.ClientConn {
	return c.connection
}

// Close closes client connection and its context
func (c *Connectivity) Close() {
	c.connection.Close()
	c.cancelRequestCtx()
}

func createConnection(host string, auth *auth.Auth) (*grpc.ClientConn, error) {
	opts := getDefaultDialOptions()

	ctx, dialCancel := context.WithTimeout(context.Background(), optimusDialTimeout)
	defer dialCancel()

	err := addAuthentication(ctx, opts, auth)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.DialContext(ctx, host, opts...)
	if errors.Is(err, context.DeadlineExceeded) {
		err = errServerNotReachable(host)
	}

	return conn, err
}

func getDefaultDialOptions() []grpc.DialOption {
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(backoffDuration)),
		grpc_retry.WithMax(grpcMaxRetry),
	}
	var opts []grpc.DialOption
	opts = append(opts,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(grpcMaxClientSendSize),
			grpc.MaxCallRecvMsgSize(grpcMaxClientRecvSize),
		),
		grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
			grpc_retry.UnaryClientInterceptor(retryOpts...),
			otelgrpc.UnaryClientInterceptor(),
			grpc_prometheus.UnaryClientInterceptor,
		)),
		grpc.WithStreamInterceptor(grpc_middleware.ChainStreamClient(
			otelgrpc.StreamClientInterceptor(),
			grpc_prometheus.StreamClientInterceptor,
		)),
	)
	return opts
}

func addAuthentication(ctx context.Context, opts []grpc.DialOption, auth *auth.Auth) error {
	if useInsecure() {
		return nil
	}

	token, err := auth.GetToken(ctx)
	if err != nil {
		return err
	}
	if token == nil {
		return errors.New("unable to get valid token")
	}

	opts = append(opts, grpc.WithPerRPCCredentials(&bearerAuthentication{
		Token: token.AccessToken,
	}))
	return nil
}

func useInsecure() bool {
	if insecure := os.Getenv("OPTIMUS_INSECURE"); insecure != "" {
		return true
	}
	return false
}
