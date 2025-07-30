package job

import (
	"bytes"
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
	"gopkg.in/yaml.v3"

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
	logger         log.Logger
	clientConfig   *config.ClientConfig
	specReadWriter local.SpecReadWriter[*model.JobSpec]

	verbose        bool
	output         string
	configFilePath string

	sourceRef                     string // sourceRef is new state
	targetRef                     string // targetRef is current state
	diffSHA                       string // diffSHA is the commit SHA of the diff
	gitURL, gitToken, gitProvider string
	gitProjectID                  string
	repository                    providermodel.RepositoryAPI
	commit                        providermodel.CommitAPI
}

func (p *planCommand) useReferenceComparison() bool {
	return len(p.sourceRef) > 0 && len(p.targetRef) > 0
}

func NewPlanCommand() *cobra.Command {
	planCmd := &planCommand{logger: logger.NewClientLogger()}
	cmd := &cobra.Command{
		Use:   "plan",
		Short: "Plan job deployment",
		Long:  "Plan job deployment based on git diff state using git reference (commit SHA, branch, tag)",
		Example: `optimus job plan --source <source_ref> --target <target_ref> --output <output_plan_file>   # Create Plan using git diff 2 references
optimus job plan --diff-sha <diff_sha> --output <output_plan_file>   # Create Plan using git diff commit SHA`,
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

	cmd.Flags().StringVar(&p.gitProvider, "git-provider", os.Getenv("GIT_PROVIDER"), "selected git provider used in the repository")
	cmd.Flags().StringVar(&p.gitURL, "git-host", os.Getenv("GIT_HOST"), "Git host based on git provider used in the repository")
	cmd.Flags().StringVar(&p.gitToken, "git-token", os.Getenv("GIT_TOKEN"), "Git token based on git provider used in the repository")
	cmd.Flags().StringVar(&p.gitProjectID, "git-project-id", os.Getenv("GIT_PROJECT_ID"), "Determine which git project will be checked")
	cmd.Flags().StringVar(&p.sourceRef, "source", p.sourceRef, "Git diff source reference (new state) [commit SHA, branch, tag]")
	cmd.Flags().StringVar(&p.targetRef, "target", p.targetRef, "Git diff target reference (current state) [commit SHA, branch, tag]")
	cmd.Flags().StringVar(&p.diffSHA, "diff-sha", p.diffSHA, "Git diff commit SHA, it will be ignored if source and target are provided")
}

func (p *planCommand) PreRunE(_ *cobra.Command, _ []string) error {
	var err error
	if p.useReferenceComparison() {
		p.logger.Info("[plan] compare: `%s` ← `%s`", p.targetRef, p.sourceRef)
	} else {
		p.logger.Info("[plan] compare with diff SHA: `%s`", p.diffSHA)
	}

	p.clientConfig, err = config.LoadClientConfig(p.configFilePath)
	if err != nil {
		return err
	}

	switch p.gitProvider {
	case providermodel.ProviderGitLab:
		api, err := gitlab.NewAPI(p.gitURL, p.gitToken)
		if err != nil {
			return err
		}
		p.repository = api
		p.commit = api
	case providermodel.ProviderGitHub:
		api, err := github.NewAPI(p.gitURL, p.gitToken)
		if err != nil {
			return err
		}
		p.repository = api
		p.commit = api
	default:
		return errors.New("unsupported git provider, we currently only support: [github,gitlab]")
	}

	p.specReadWriter, err = specio.NewJobSpecReadWriter(afero.NewOsFs())
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

func (p *planCommand) getDiff(ctx context.Context) ([]*providermodel.Diff, error) {
	if p.useReferenceComparison() {
		return p.repository.CompareDiff(ctx, p.gitProjectID, p.targetRef, p.sourceRef)
	}
	return p.commit.GetCommitDiff(ctx, p.gitProjectID, p.diffSHA)
}

func (p *planCommand) generatePlanWithGitDiff(ctx context.Context) (plan.Plan, error) {
	plans := plan.NewPlan(p.clientConfig.Project.Name)
	affectedDirectories, err := p.getAffectedDirectory(ctx)
	if err != nil {
		return plans, err
	}

	for _, directory := range affectedDirectories {
		namespace, err := p.getNamespaceNameByJobPath(directory)
		if err != nil {
			return plans, err
		}

		sourceSpec, err := p.getJobSpec(ctx, filepath.Join(directory, jobFileName), p.sourceRef)
		if err != nil {
			return plans, err
		}

		targetSpec, err := p.getJobSpec(ctx, filepath.Join(directory, jobFileName), p.targetRef)
		if err != nil {
			return plans, err
		}

		if len(sourceSpec.Name) > 0 && len(targetSpec.Name) == 0 {
			if sourceSpec.Version < 3 {
				return plans, fmt.Errorf("spec[%s] should use version 3", sourceSpec.Name)
			}
		}

		plans.Job.Add(namespace, sourceSpec.Name, targetSpec.Name, &plan.JobPlan{Path: directory})
	}

	return plans.GetResult(), nil
}

func (p *planCommand) getAffectedDirectory(ctx context.Context) ([]string, error) {
	diffs, err := p.getDiff(ctx)
	if err != nil {
		return nil, err
	}

	totalPathEachDiff := 2
	affectedDirectories := make([]string, 0, len(diffs)*totalPathEachDiff)
	for i := range diffs {
		affectedDirectories = append(affectedDirectories, diffs[i].OldPath, diffs[i].NewPath)
	}

	directories := plan.DistinctDirectory(plan.GetValidJobDirectory(affectedDirectories))
	p.logger.Info("job plan found changed in directories: %+v", directories)
	return directories, nil
}

func (p *planCommand) getNamespaceNameByJobPath(directory string) (string, error) {
	for _, namespace := range p.clientConfig.Namespaces {
		if strings.HasPrefix(directory, namespace.Job.Path) {
			return namespace.Name, nil
		}
	}
	return "", fmt.Errorf("failed to find namespace specified on directory: %s", directory)
}

func (p *planCommand) getJobSpec(ctx context.Context, fileName, ref string) (model.JobSpec, error) {
	var spec model.JobSpec
	raw, err := p.repository.GetFileContent(ctx, p.gitProjectID, ref, fileName)
	if err != nil {
		return spec, fmt.Errorf("failed to get file with ref: %s and directory %s: %w", ref, fileName, err)
	}
	// note: in this case, we want to also read empty file as empty spec and not return an error
	// yaml.Decoder returns io.EOF if the file is empty
	buf := bytes.NewBuffer(raw)
	if err := yaml.NewDecoder(buf).Decode(&spec); err != nil && !errors.Is(err, io.EOF) {
		return spec, fmt.Errorf("failed to unmarshal job specification with ref: %s and directory %s: %w", ref, fileName, err)
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
	if existingPlan.SameProjectName(plans) && !existingPlan.Resource.IsZero() {
		plans.Resource = existingPlan.Resource
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
