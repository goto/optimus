package namespace

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/goto/salt/log"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/goto/optimus/client/cmd/internal/connection"
	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/config"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

const describeAccessTimeout = time.Minute * 5

type DescribeAccessCommand struct {
	logger         log.Logger
	configFilePath string
	clientConfig   *config.ClientConfig

	namespaceName string
	roleName      string
}

// NewDescribeAccessCommand initializes command for registering namespace
func NewDescribeAccessCommand() *cobra.Command {
	d := &DescribeAccessCommand{
		logger: logger.NewClientLogger(),
	}

	cmd := &cobra.Command{
		Use:     "describe-access",
		Short:   "Describes Team capabilities for scheduled jobs",
		Example: "optimus namespace describe-access [--flag]",
		PreRunE: d.PreRunE,
		RunE:    d.RunE,
	}

	cmd.Flags().StringVarP(&d.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")
	cmd.Flags().StringVarP(&d.namespaceName, "name", "n", d.namespaceName, "If set, then only that namespace will be described")
	cmd.Flags().StringVarP(&d.roleName, "role", "r", d.namespaceName, "If set, then Role will be described")
	return cmd
}

func (r *DescribeAccessCommand) PreRunE(_ *cobra.Command, _ []string) error {
	conf, err := config.LoadClientConfig(r.configFilePath)
	if err != nil {
		return err
	}

	r.clientConfig = conf
	return nil
}

func (r *DescribeAccessCommand) RunE(_ *cobra.Command, _ []string) error {
	conn := connection.New(r.logger, r.clientConfig)
	c, err := conn.Create(r.clientConfig.Host)
	if err != nil {
		return err
	}
	defer c.Close()

	namespace, err := r.clientConfig.GetNamespaceByName(r.namespaceName)
	if err != nil {
		return err
	}

	err = GetSchedulerRole(r.logger, c, r.clientConfig.Project.Name, namespace, r.roleName)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			r.logger.Info("Scheduler Role [%s] not found", r.namespaceName)
			return nil
		}
		return fmt.Errorf("failed to Fetch scheduler role, namespace [%s]: %w", r.namespaceName, err)
	}
	return err
}

func GetSchedulerRole(l log.Logger, conn *grpc.ClientConn, projectName string, namespace *config.Namespace, roleName string) error {
	schedulerServiceClient := pb.NewJobRunServiceClient(conn)

	ctx, cancelFunc := context.WithTimeout(context.Background(), describeAccessTimeout)
	defer cancelFunc()

	if roleName == "" {
		roleName = namespace.Name
	}

	resp, err := schedulerServiceClient.GetSchedulerRole(ctx, &pb.GetSchedulerRoleRequest{
		ProjectName:   projectName,
		NamespaceName: namespace.Name,
		RoleName:      roleName,
	})
	if err != nil {
		return fmt.Errorf("failed to describe scheduler role [%s], err: %w", namespace.Name, err)
	}
	l.Info(stringifyListOfPermissions(resp.GetPermissions()))

	return nil
}

func stringifyListOfPermissions(permissions []string) string {
	buff := &bytes.Buffer{}
	table := tablewriter.NewWriter(buff)
	table.SetBorder(true)
	table.SetColWidth(100)
	table.SetHeader([]string{"Permissions"})

	table.SetAlignment(tablewriter.ALIGN_LEFT)
	sort.Slice(permissions, func(i, j int) bool {
		return permissions[i] < permissions[j]
	})
	for _, permission := range permissions {
		table.Append([]string{permission})
	}
	table.Render()
	return buff.String()
}
