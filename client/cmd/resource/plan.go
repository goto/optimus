package resource

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/gocarina/gocsv"
	"github.com/goto/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v2"

	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/client/cmd/internal/plan"
	"github.com/goto/optimus/client/local"
	"github.com/goto/optimus/client/local/model"
	"github.com/goto/optimus/client/local/specio"
	"github.com/goto/optimus/config"
	"github.com/goto/optimus/ext/git"
	"github.com/goto/optimus/ext/git/gitlab"
)

const resourceFileName = "resource.yaml"

type planCommand struct {
	logger log.Logger

	sourceRef, destinationRef     string
	gitURL, gitToken, gitProvider string
	verbose                       bool
	projectID                     string
	output                        string

	configFilePath string
	clientConfig   *config.ClientConfig

	repository     git.Repository
	repositoryFile git.RepositoryFiles
	specReadWriter local.SpecReadWriter[*model.ResourceSpec]
}

func NewPlanCommand() *cobra.Command {
	planCmd := &planCommand{logger: logger.NewClientLogger()}
	cmd := &cobra.Command{
		Use:     "plan",
		Short:   "Plan resource deployment",
		Long:    "Plan resource deployment based on git diff state using git reference (commit SHA, branch, tag)",
		Example: "optimus resource plan <ref> <ref-before>",
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

	cmd.Flags().StringVarP(&p.output, "output", "o", "./resource.csv", "File Output Path")
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
		var gitlabAPI *gitlab.API
		gitlabAPI, err = gitlab.NewGitlab(p.gitURL, p.gitToken)
		p.repository = gitlabAPI
		p.repositoryFile = gitlabAPI
	default:
		return errors.New("unsupported git provider, we currently only support: [gitlab]")
	}
	if err != nil {
		return err
	}

	p.specReadWriter, err = specio.NewResourceSpecReadWriter(afero.NewOsFs())
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

	directories := git.Diffs(diffs).GetAllDirectories(p.appendDirectory)
	plans := make(plan.Plans, 0, len(directories))
	p.logger.Info("[plan] found changed directories: %+v", directories)

	for _, directory := range directories {
		var jobPlan *plan.Plan
		jobPlan, err = p.describePlanFromDirectory(ctx, directory)
		if err != nil {
			return err
		}
		if p.verbose {
			p.logger.Info("[plan] %s operation for project %s, namespace %s, resource %s", jobPlan.Operation, jobPlan.ProjectName, jobPlan.NamespaceName, jobPlan.KindName)
		}
		plans = append(plans, jobPlan)
	}

	sort.SliceStable(plans, plans.SortByOperationPriority)
	return p.saveFile(plans)
}

func (p *planCommand) describePlanFromDirectory(ctx context.Context, directory string) (resourcePlan *plan.Plan, err error) {
	var (
		namespaceName, datastoreType string
		sourceRaw, destinationRaw    []byte
		fileName                     = filepath.Join(directory, resourceFileName)
	)

	for _, namespace := range p.clientConfig.Namespaces {
		for _, datastore := range namespace.Datastore {
			if strings.HasPrefix(directory, datastore.Path) {
				namespaceName = namespace.Name
				datastoreType = datastore.Type
				break
			}
		}
	}
	if namespaceName == "" {
		return nil, fmt.Errorf("failed to find namespace specified on directory: %s", directory)
	}

	destinationRaw, err = p.repositoryFile.GetRaw(ctx, p.projectID, p.destinationRef, fileName)
	if err != nil {
		return nil, errors.Join(err, fmt.Errorf("failed to get file with ref: %s and directory %s", p.destinationRef, directory))
	}
	sourceRaw, err = p.repositoryFile.GetRaw(ctx, p.projectID, p.sourceRef, fileName)
	if err != nil {
		return nil, errors.Join(err, fmt.Errorf("failed to get file with ref: %s and directory %s", p.sourceRef, directory))
	}

	var sourceSpec, destinationSpec model.ResourceSpec
	if err = yaml.Unmarshal(sourceRaw, &sourceSpec); err != nil {
		return nil, errors.Join(err, errors.New("failed to unmarshal source resource specification"))
	}
	if err = yaml.Unmarshal(destinationRaw, &destinationSpec); err != nil {
		return nil, errors.Join(err, errors.New("failed to unmarshal destination resource specification"))
	}

	resourcePlan = &plan.Plan{ProjectName: p.clientConfig.Project.Name, NamespaceName: namespaceName, Kind: plan.KindResource}
	if len(sourceSpec.Name) == 0 && len(destinationSpec.Name) > 0 {
		resourcePlan.KindName = fmt.Sprintf("%s:%s", datastoreType, destinationSpec.Name)
		resourcePlan.Operation = plan.OperationCreate
	} else if len(sourceSpec.Name) > 0 && len(destinationSpec.Name) == 0 {
		resourcePlan.KindName = fmt.Sprintf("%s:%s", datastoreType, sourceSpec.Name)
		resourcePlan.Operation = plan.OperationDelete
	} else {
		resourcePlan.KindName = fmt.Sprintf("%s:%s", datastoreType, destinationSpec.Name)
		resourcePlan.Operation = plan.OperationUpdate
	}

	return resourcePlan, nil
}

func (*planCommand) appendDirectory(directory string, directoryExists map[string]bool, fileDirectories []string) []string {
	if !strings.HasSuffix(directory, "/"+resourceFileName) {
		return fileDirectories
	}
	directory = strings.TrimSuffix(directory, "/"+resourceFileName)
	if !directoryExists[directory] {
		fileDirectories = append(fileDirectories, directory)
		directoryExists[directory] = true
	}
	return fileDirectories
}

func (p *planCommand) saveFile(plans plan.Plans) error {
	file, err := os.OpenFile(p.output, unix.O_RDWR, os.ModePerm)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		file, err = os.Create(p.output)
		if err != nil {
			return err
		}
	}
	defer file.Close()

	if err = gocsv.MarshalFile(plans, file); err != nil {
		return errors.Join(errors.New("failed marshal to csv file"), err)
	}
	p.logger.Info("[plan] file plan created: %s", file.Name())
	return nil
}
