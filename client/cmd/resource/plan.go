package resource

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/goto/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
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

const resourceFileName = "resource.yaml"

type planCommand struct {
	logger         log.Logger
	clientConfig   *config.ClientConfig
	specReadWriter local.SpecReadWriter[*model.ResourceSpec]

	syncAll        bool // TODO: implement it
	verbose        bool
	output         string
	configFilePath string

	sourceRef                     string // sourceRef is new state
	targetRef                     string // targetRef is current state
	gitURL, gitToken, gitProvider string
	projectID                     string
	repository                    providermodel.RepositoryAPI

	// TODO: add with statefile
}

func NewPlanCommand() *cobra.Command {
	planCmd := &planCommand{logger: logger.NewClientLogger()}
	cmd := &cobra.Command{
		Use:   "plan",
		Short: "Plan resource deployment",
		Long:  "Plan resource deployment based on git diff state using git reference (commit SHA, branch, tag)",
		Example: `optimus resource plan --sync-all # Create Plan diff from latest branch and current state
optimus resource plan --source <source_ref> --target <target_ref>   # Create Plan using git diff 2 references`,
		PreRunE: planCmd.PreRunE,
		RunE:    planCmd.RunE,
	}

	planCmd.inject(cmd)
	return cmd
}

func (p *planCommand) inject(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&p.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")
	cmd.Flags().StringVarP(&p.output, "output", "o", "./plan.json", "File path for output of plan")
	cmd.Flags().BoolVarP(&p.verbose, "verbose", "v", false, "Print details related to operation")

	cmd.Flags().StringVarP(&p.gitProvider, "git-provider", "p", os.Getenv("GIT_PROVIDER"), "selected git provider used in the repository")
	cmd.Flags().StringVarP(&p.gitURL, "git-host", "h", os.Getenv("GIT_HOST"), "Git host based on git provider used in the repository")
	cmd.Flags().StringVarP(&p.gitToken, "git-token", "t", os.Getenv("GIT_TOKEN"), "Git token based on git provider used in the repository")
	cmd.Flags().StringVarP(&p.projectID, "project-id", "I", os.Getenv("GIT_PROJECT_ID"), "Determine which project will be checked")

	// - sync all
	cmd.Flags().BoolVar(&p.syncAll, "sync-all", false, "Create Plan from current state with latest git")

	// - sync diff
	cmd.Flags().StringVarP(&p.sourceRef, "source", "S", p.sourceRef, "Git diff source reference [commit SHA, branch, tag]")
	cmd.Flags().StringVarP(&p.targetRef, "target", "T", p.targetRef, "Git diff target reference [commit SHA, branch, tag]")
}

func (p *planCommand) PreRunE(_ *cobra.Command, _ []string) error {
	var err error

	if p.syncAll {
		p.logger.Info("[plan] compare latest with state file")
	} else {
		p.logger.Info("[plan] compare: `%s` ← `%s`", p.targetRef, p.sourceRef)
	}

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

	p.specReadWriter, err = specio.NewResourceSpecReadWriter(afero.NewOsFs())
	if err != nil {
		return err
	}

	return nil
}

func (p *planCommand) RunE(_ *cobra.Command, _ []string) error {
	var (
		ctx   = context.Background()
		plans plan.Plan
		err   error
	)

	plans, err = p.generatePlanWithGitDiff(ctx)
	if err != nil {
		return err
	}

	p.printPlan(plans)
	return p.savePlan(plans)
}

func (p *planCommand) generatePlanWithGitDiff(ctx context.Context) (plan.Plan, error) {
	plans := plan.NewPlan(p.clientConfig.Project.Name)
	affectedDirectories, err := p.getAffectedDirectory(ctx)
	if err != nil {
		return plans, err
	}

	for _, directory := range affectedDirectories {
		namespace, datastore, err := p.getNamespaceAndDatastoreNameByPath(directory)
		if err != nil {
			return plans, err
		}

		sourceSpec, err := p.getResourceSpec(ctx, filepath.Join(directory, resourceFileName), p.sourceRef)
		if err != nil {
			return plans, err
		}
		targetSpec, err := p.getResourceSpec(ctx, filepath.Join(directory, resourceFileName), p.targetRef)
		if err != nil {
			return plans, err
		}

		plans.Resource.Add(namespace, sourceSpec.Name, targetSpec.Name, &plan.ResourcePlan{Datastore: datastore})
	}

	plans = plans.GetResult()
	return plans, nil
}

func (p *planCommand) getAffectedDirectory(ctx context.Context) ([]string, error) {
	diffs, err := p.repository.CompareDiff(ctx, p.projectID, p.targetRef, p.sourceRef)
	if err != nil {
		return nil, err
	}

	twice := 2
	affectedDirectories := make([]string, 0, len(diffs)*twice)
	for i := range diffs {
		affectedDirectories = append(affectedDirectories, diffs[i].OldPath, diffs[i].NewPath)
	}

	directories := plan.DistinctDirectory(plan.GetValidResourceDirectory(affectedDirectories))
	p.logger.Info("resource plan found changed in directories: %+v", directories)
	return directories, nil
}

func (p *planCommand) getNamespaceAndDatastoreNameByPath(directory string) (string, string, error) {
	for _, namespace := range p.clientConfig.Namespaces {
		for _, datastore := range namespace.Datastore {
			if strings.HasPrefix(directory, datastore.Path) {
				return namespace.Name, datastore.Type, nil
			}
		}
	}
	return "", "", fmt.Errorf("failed to find namespace specified on directory: %s", directory)
}

func (p *planCommand) getResourceSpec(ctx context.Context, fileName, ref string) (model.ResourceSpec, error) {
	var spec model.ResourceSpec
	raw, err := p.repository.GetFileContent(ctx, p.projectID, ref, fileName)
	if err != nil {
		return spec, errors.Join(err, fmt.Errorf("failed to get file with ref: %s and directory %s", p.sourceRef, fileName))
	}
	if err = yaml.Unmarshal(raw, &spec); err != nil {
		return spec, errors.Join(err, fmt.Errorf("failed to unmarshal resource specification with ref: %s and directory %s", p.sourceRef, fileName))
	}
	return spec, nil
}

func (p *planCommand) printPlan(plans plan.Plan) {
	if !p.verbose {
		return
	}

	for namespace, planList := range plans.Job.Create {
		names := plan.KindList[*plan.JobPlan](planList).GetNames()
		msg := fmt.Sprintf("[%s] plan create jobs %v", namespace, names)
		p.logger.Info(msg)
	}

	for namespace, planList := range plans.Job.Delete {
		names := plan.KindList[*plan.JobPlan](planList).GetNames()
		msg := fmt.Sprintf("[%s] plan delete jobs %v", namespace, names)
		p.logger.Info(msg)
	}

	for namespace, planList := range plans.Job.Update {
		names := plan.KindList[*plan.JobPlan](planList).GetNames()
		msg := fmt.Sprintf("[%s] plan update jobs %v", namespace, names)
		p.logger.Info(msg)
	}

	for namespace, planList := range plans.Job.Migrate {
		for i := range planList {
			msg := fmt.Sprintf("[%s] plan migrate job %v from old_namespace: %s", namespace, planList[i].GetName(), *planList[i].OldNamespace)
			p.logger.Info(msg)
		}
	}
}

func (p *planCommand) savePlan(plans plan.Plan) error {
	file, err := os.OpenFile(p.output, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	existingBytes, _ := io.ReadAll(file)
	var existingPlan plan.Plan
	_ = json.Unmarshal(existingBytes, &existingBytes)
	if existingPlan.SameProjectName(plans) && !existingPlan.Job.IsZero() {
		plans.Job = existingPlan.Job
	}

	_ = file.Truncate(0)
	_, _ = file.Seek(0, 0)
	planBytes, err := json.MarshalIndent(plans, "", " ")
	if err != nil {
		return err
	}
	file.Write(planBytes)
	p.logger.Info("plan file created: %s", file.Name())
	return nil
}
