package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/goto/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v2"

	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/client/cmd/internal/plan"
	providermodel "github.com/goto/optimus/client/extension/model"
	"github.com/goto/optimus/client/extension/provider/github"
	"github.com/goto/optimus/client/extension/provider/gitlab"
	"github.com/goto/optimus/client/local"
	"github.com/goto/optimus/client/local/model"
	"github.com/goto/optimus/client/local/specio"
	"github.com/goto/optimus/config"
)

const jobFileName = "job.yaml"

type planCommand struct {
	logger log.Logger

	sourceRef, destinationRef     string
	gitURL, gitToken, gitProvider string
	verbose                       bool
	projectID                     string
	output                        string

	configFilePath string
	clientConfig   *config.ClientConfig

	repository     providermodel.RepositoryAPI
	specReadWriter local.SpecReadWriter[*model.JobSpec]
}

func NewPlanCommand() *cobra.Command {
	planCmd := &planCommand{logger: logger.NewClientLogger()}
	cmd := &cobra.Command{
		Use:     "plan",
		Short:   "Plan job deployment",
		Long:    "Plan job deployment based on git diff state using git reference (commit SHA, branch, tag)",
		Example: "optimus job plan <ref> <ref-before>",
		Args:    cobra.MinimumNArgs(2), //nolint
		PreRunE: planCmd.PreRunE,
		RunE:    planCmd.RunE,
	}

	planCmd.inject(cmd)
	return cmd
}

func (p *planCommand) inject(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&p.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")

	cmd.Flags().StringVarP(&p.gitProvider, "git-provider", "p", os.Getenv("GIT_PROVIDER"), "selected git provider used in the repository")
	cmd.Flags().StringVarP(&p.gitURL, "git-host", "h", os.Getenv("GIT_HOST"), "Git host based on git provider used in the repository")
	cmd.Flags().StringVarP(&p.gitToken, "git-token", "t", os.Getenv("GIT_TOKEN"), "Git token based on git provider used in the repository")

	cmd.Flags().StringVarP(&p.projectID, "project-id", "I", os.Getenv("GIT_PROJECT_ID"), "Determine which project will be checked")

	cmd.Flags().StringVarP(&p.output, "output", "o", "./job.json", "File path for output of plan")
	cmd.Flags().BoolVarP(&p.verbose, "verbose", "v", false, "Print details related to operation")
}

func (p *planCommand) PreRunE(_ *cobra.Command, args []string) error {
	var err error

	p.destinationRef = args[0]
	p.sourceRef = args[1]
	p.logger.Info("[plan] compare: `%s` ← `%s`", p.destinationRef, p.sourceRef)
	p.clientConfig, err = config.LoadClientConfig(p.configFilePath)
	if err != nil {
		return err
	}

	switch p.gitProvider {
	case "gitlab":
		p.repository, err = gitlab.NewAPI(p.gitURL, p.gitToken)
	case "github":
		p.repository, err = github.NewAPI(p.gitURL, p.gitToken)
	default:
		return errors.New("unsupported git provider, we currently only support: [github,gitlab]")
	}
	if err != nil {
		return err
	}

	p.specReadWriter, err = specio.NewJobSpecReadWriter(afero.NewOsFs())
	if err != nil {
		return err
	}

	return nil
}

func (p *planCommand) RunE(_ *cobra.Command, _ []string) error {
	ctx := context.Background()
	diffs, err := p.repository.CompareDiff(ctx, p.projectID, p.sourceRef, p.destinationRef)
	if err != nil {
		return err
	}

	directories := providermodel.Diffs(diffs).GetAllDirectories(p.appendDirectory)
	p.logger.Info("job plan found changed in directories: %+v", directories)

	var plans plan.Plans
	for _, directory := range directories {
		var jobPlan *plan.Plan
		jobPlan, err = p.describePlanFromDirectory(ctx, directory)
		if err != nil {
			return err
		}
		plans = append(plans, jobPlan)
	}

	mergedPlans := plans.Merge()
	if p.verbose {
		for i := range mergedPlans {
			msg := fmt.Sprintf("[%s] plan operation %s for %s %s", mergedPlans[i].NamespaceName, mergedPlans[i].Operation, mergedPlans[i].Kind, mergedPlans[i].KindName)
			if mergedPlans[i].OldNamespaceName != nil {
				msg += " with old namespace: " + *mergedPlans[i].OldNamespaceName
			}
			p.logger.Info(msg)
		}
	}
	return p.saveFile(mergedPlans)
}

func (p *planCommand) describePlanFromDirectory(ctx context.Context, directory string) (*plan.Plan, error) {
	var (
		namespaceName             string
		err                       error
		sourceRaw, destinationRaw []byte
		fileName                  = filepath.Join(directory, jobFileName)
	)

	for _, namespace := range p.clientConfig.Namespaces {
		if strings.HasPrefix(directory, namespace.Job.Path) {
			namespaceName = namespace.Name
			break
		}
	}
	if namespaceName == "" {
		return nil, fmt.Errorf("failed to find namespace specified on directory: %s", directory)
	}

	destinationRaw, err = p.repository.GetFileContent(ctx, p.projectID, p.destinationRef, fileName)
	if err != nil {
		return nil, errors.Join(err, fmt.Errorf("failed to get file with ref: %s and directory %s", p.destinationRef, directory))
	}
	sourceRaw, err = p.repository.GetFileContent(ctx, p.projectID, p.sourceRef, fileName)
	if err != nil {
		return nil, errors.Join(err, fmt.Errorf("failed to get file with ref: %s and directory %s", p.sourceRef, directory))
	}

	var sourceSpec, destinationSpec model.JobSpec
	if err = yaml.Unmarshal(sourceRaw, &sourceSpec); err != nil {
		return nil, errors.Join(err, errors.New("failed to unmarshal source job specification"))
	}
	if err = yaml.Unmarshal(destinationRaw, &destinationSpec); err != nil {
		return nil, errors.Join(err, errors.New("failed to unmarshal destination job specification"))
	}

	jobPlan := &plan.Plan{ProjectName: p.clientConfig.Project.Name, NamespaceName: namespaceName, Kind: plan.KindJob}
	if p.isOperationCreate(sourceSpec, destinationSpec) {
		jobPlan.KindName = destinationSpec.Name
		jobPlan.Operation = plan.OperationCreate
	} else if p.isOperationDelete(sourceSpec, destinationSpec) {
		jobPlan.KindName = sourceSpec.Name
		jobPlan.Operation = plan.OperationDelete
	} else {
		jobPlan.KindName = destinationSpec.Name
		jobPlan.Operation = plan.OperationUpdate
	}

	return jobPlan, nil
}

// isOperationCreate return true when destinationSpec is exists, but sourceSpec is missing
func (*planCommand) isOperationCreate(sourceSpec, destinationSpec model.JobSpec) bool {
	return len(sourceSpec.Name) == 0 && len(destinationSpec.Name) > 0
}

// isOperationCreate return true when sourceSpec is exists, but destinationSpec is missing
func (*planCommand) isOperationDelete(sourceSpec, destinationSpec model.JobSpec) bool {
	return len(sourceSpec.Name) > 0 && len(destinationSpec.Name) == 0
}

func (*planCommand) appendDirectory(directory string, directoryExists map[string]bool, fileDirectories []string) []string {
	index := strings.Index(directory, "/assets")
	if !strings.HasSuffix(directory, "/"+jobFileName) && index < 1 {
		return fileDirectories
	}
	directory = strings.TrimSuffix(directory, "/"+jobFileName)
	if index > 0 {
		directory = directory[:index]
	}
	if !directoryExists[directory] {
		fileDirectories = append(fileDirectories, directory)
		directoryExists[directory] = true
	}
	return fileDirectories
}

func (p *planCommand) saveFile(plans plan.Plans) error {
	file, err := os.OpenFile(p.output, unix.O_RDWR|unix.O_CREAT, os.ModePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	_ = file.Truncate(0)
	_, _ = file.Seek(0, 0)
	planBytes, err := json.MarshalIndent(plans, "", " ")
	if err != nil {
		return err
	}
	file.Write(planBytes)
	p.logger.Info("job plan file created: %s", file.Name())
	return nil
}
