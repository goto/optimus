package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/goto/salt/log"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/goto/optimus/client/cmd/internal/connection"
	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/client/cmd/internal/plan"
	"github.com/goto/optimus/client/local/model"
	"github.com/goto/optimus/client/local/specio"
	"github.com/goto/optimus/config"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

const validateTimeout = time.Minute * 15

type validateCommand struct {
	logger     log.Logger
	connection connection.Connection

	usePlan      bool
	planFilePath string
	plans        plan.Plans

	configFilePath string
	namespaceName  string
	jobNames       []string

	fromServer bool
	forDelete  bool
	verbose    bool

	clientConfig *config.ClientConfig
}

// NewValidateCommand initializes command for validating job specification
func NewValidateCommand() *cobra.Command {
	validate := &validateCommand{
		logger: logger.NewClientLogger(),
	}

	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Execute validation on the selected jobs",
		Long:  "Validation is executed on the selected jobs. Each job will be validated against a sequence of criterions.",
		Example: `... --namespace <namespace_name>   # upload all jobs within namespace to be validated
... --jobs <job_name1,job_name2> -n <namespace_name>   # upload the selected jobs within namespace to be validated
... -j <job_name1,job_name2> -n <namespace_name> --from-server   # validate existing jobs in the server, no upload will be done
... -j <job_name1,job_name2> -n <namespace_name> -s --for-delete   # validation is for delete purpose, no actual deletion
... --plan <plan_file_path>   # validate job based on job plan command output`,
		RunE:    validate.RunE,
		PreRunE: validate.PreRunE,
	}

	cmd.Flags().StringVarP(&validate.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")
	cmd.Flags().StringVarP(&validate.planFilePath, "plan", "p", config.EmptyPath, "File path for plan result")
	cmd.Flags().StringVarP(&validate.namespaceName, "namespace", "n", validate.namespaceName, "Namespace name in which the job resides")
	cmd.Flags().StringSliceVarP(&validate.jobNames, "jobs", "j", nil, "Selected job names, comma separated without any whitespace if more than one")

	cmd.Flags().BoolVarP(&validate.fromServer, "from-server", "s", false, "Determines whether to upload jobs from local or to use existing from the server")
	cmd.Flags().BoolVarP(&validate.forDelete, "for-delete", "d", false, "Determines whether the validation is for delete purpose or not, only valid with from-server flag")
	cmd.Flags().BoolVarP(&validate.verbose, "verbose", "v", false, "Determines whether to show the complete message or just the summary")

	return cmd
}

func (v *validateCommand) PreRunE(_ *cobra.Command, _ []string) error {
	conf, err := config.LoadClientConfig(v.configFilePath)
	if err != nil {
		return err
	}
	v.clientConfig = conf

	v.plans, err = v.getPlanFromFile()
	if err != nil {
		return err
	}

	v.connection = connection.New(v.logger, conf)
	return nil
}

func (v *validateCommand) RunE(_ *cobra.Command, _ []string) error {
	if v.usePlan {
		if v.forDelete || v.fromServer || v.namespaceName != "" || len(v.jobNames) > 0 {
			return errors.New("[for-delete,from-server,namespace] flag should not be send when validate using plan")
		}
		return v.runValidateUsingPlan()
	}

	if v.forDelete && !v.fromServer {
		return errors.New("for-delete flag is only valid with from-server flag")
	}

	if v.fromServer && len(v.jobNames) == 0 {
		return errors.New("from-server flag is only valid with jobs flag being set")
	}

	namespace, err := v.clientConfig.GetNamespaceByName(v.namespaceName)
	if err != nil {
		return err
	}

	v.logger.Info("Validating job specifications for namespace [%s]\n", v.namespaceName)
	start := time.Now()

	if err := v.executeLocalValidation(); err != nil {
		v.logger.Info("Validation is finished, took %s", time.Since(start).Round(time.Second))
		return err
	}

	if err := v.executeServerValidation(namespace); err != nil {
		v.logger.Info("Validation is finished, took %s", time.Since(start).Round(time.Second))
		return err
	}

	v.logger.Info("Validation is finished, took %s", time.Since(start).Round(time.Second))
	return nil
}

func (v *validateCommand) executeLocalValidation() error {
	jobSpecReadWriter, err := specio.NewJobSpecReadWriter(afero.NewOsFs())
	if err != nil {
		return err
	}

	namespaceNamesByJobName := make(map[string][]string)
	for _, namespace := range v.clientConfig.Namespaces {
		jobs, err := jobSpecReadWriter.ReadAll(namespace.Job.Path)
		if err != nil {
			return err
		}

		for _, j := range jobs {
			namespaceNamesByJobName[j.Name] = append(namespaceNamesByJobName[j.Name], namespace.Name)
		}
	}

	success := true
	for jobName, namespaceNames := range namespaceNamesByJobName {
		if len(namespaceNames) == 1 {
			continue
		}

		uniqueNames := v.deduplicate(namespaceNames)
		v.logger.Error("[%s] is written [%d] times in namespace [%s]", jobName, len(namespaceNames), strings.Join(uniqueNames, ", "))
		success = false
	}

	if !success {
		return errors.New("local duplication is detected")
	}

	return nil
}

func (*validateCommand) deduplicate(input []string) []string {
	var output []string

	tmp := make(map[string]bool)
	for _, s := range input {
		if !tmp[s] {
			tmp[s] = true
			output = append(output, s)
		}
	}

	return output
}

func (v *validateCommand) executeServerValidation(namespace *config.Namespace) error {
	var (
		request *pb.ValidateRequest
		err     error
	)

	if v.fromServer {
		request = v.getRequestForJobNames()
	} else {
		request, err = v.getRequestForJobSpecs(namespace)
	}

	if err != nil {
		return err
	}

	success := true

	response, err := v.executeValidation(request)
	if err != nil {
		v.logger.Error("error returned when executing validation: %v", err)
		success = false
	}

	if err := v.processResponse(response); err != nil {
		v.logger.Error("error returned when processing response: %v", err)
		success = false
	}

	if !success {
		return errors.New("encountered one or more errors")
	}

	return nil
}

func (v *validateCommand) processResponse(response *pb.ValidateResponse) error {
	if response == nil {
		return nil
	}

	if v.verbose {
		return v.printCompleteResponse(response)
	}

	return v.printBriefResponse(response)
}

func (v *validateCommand) printBriefResponse(response *pb.ValidateResponse) error {
	resultsByJobName := response.GetResultsByJobName()

	var successJobsCount int
	for jobName, rawResult := range resultsByJobName {
		var successResultCount int

		v.logger.Info("[%s]", jobName)
		result := rawResult.GetResults()
		for _, r := range result {
			if r.Success {
				successResultCount++
			} else {
				v.logger.Info("  %s", r.GetName())
				for _, m := range r.GetMessages() {
					v.logger.Error("    %s", m)
				}
			}
		}

		if successResultCount == len(result) {
			successJobsCount++
		}

		v.logger.Info("[%s] pass %d of %d validations", jobName, successResultCount, len(result))
	}

	if successJobsCount != len(resultsByJobName) {
		return errors.New("validation encountered errors")
	}

	return nil
}

func (v *validateCommand) printCompleteResponse(response *pb.ValidateResponse) error {
	resultsByJobName := response.GetResultsByJobName()

	var successJobsCount int
	for jobName, rawResult := range resultsByJobName {
		v.logger.Info("[%s]", jobName)

		table := tablewriter.NewWriter(os.Stdout)
		table.SetAutoMergeCellsByColumnIndex([]int{0, 1})
		table.SetRowLine(true)

		table.SetHeader([]string{"NAME", "SUCCESS", "MESSAGE"})

		var successResultCount int

		result := rawResult.GetResults()
		for _, r := range result {
			if r.Success {
				successResultCount++
			}

			for i := 0; i < len(r.Messages); i++ {
				table.Append([]string{r.Name, strings.ToUpper(fmt.Sprintf("%t", r.Success)), r.Messages[i]})
			}
		}

		if successResultCount == len(result) {
			successJobsCount++
		}

		table.SetFooter([]string{"", "", fmt.Sprintf("passed %d of %d validations", successResultCount, len(result))})
		table.SetFooterAlignment(tablewriter.ALIGN_RIGHT)

		table.Render()

		v.logger.Info("\n")
	}

	if successJobsCount != len(resultsByJobName) {
		return errors.New("validation encountered errors")
	}

	return nil
}

func (v *validateCommand) executeValidation(request *pb.ValidateRequest) (*pb.ValidateResponse, error) {
	conn, err := v.connection.Create(v.clientConfig.Host)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pb.NewJobSpecificationServiceClient(conn)

	ctx, dialCancel := context.WithTimeout(context.Background(), validateTimeout)
	defer dialCancel()

	return client.Validate(ctx, request)
}

func (v *validateCommand) getRequestForJobNames() *pb.ValidateRequest {
	return &pb.ValidateRequest{
		ProjectName:   v.clientConfig.Project.Name,
		NamespaceName: v.namespaceName,
		Payload: &pb.ValidateRequest_FromServer_{
			FromServer: &pb.ValidateRequest_FromServer{
				JobNames:     v.jobNames,
				DeletionMode: v.forDelete,
			},
		},
	}
}

func (v *validateCommand) getRequestForJobSpecs(namespace *config.Namespace) (*pb.ValidateRequest, error) {
	jobSpecReadWriter, err := specio.NewJobSpecReadWriter(afero.NewOsFs(), specio.WithJobSpecParentReading())
	if err != nil {
		return nil, err
	}

	jobSpecs, err := jobSpecReadWriter.ReadAll(namespace.Job.Path)
	if err != nil {
		return nil, err
	}

	if len(v.jobNames) > 0 {
		specByName := make(map[string]*model.JobSpec)
		for _, spec := range jobSpecs {
			specByName[spec.Name] = spec
		}

		var specsToValidate []*model.JobSpec
		for _, name := range v.jobNames {
			spec, ok := specByName[name]
			if !ok {
				return nil, fmt.Errorf("spec for job [%s] is not found in local namespace [%s]", name, v.namespaceName)
			}

			specsToValidate = append(specsToValidate, spec)
		}

		jobSpecs = specsToValidate
	}

	jobSpecsProto := make([]*pb.JobSpecification, len(jobSpecs))
	for i, jobSpec := range jobSpecs {
		jobSpecsProto[i] = jobSpec.ToProto()
	}

	return &pb.ValidateRequest{
		ProjectName:   v.clientConfig.Project.Name,
		NamespaceName: namespace.Name,
		Payload: &pb.ValidateRequest_FromOutside_{
			FromOutside: &pb.ValidateRequest_FromOutside{
				Jobs: jobSpecsProto,
			},
		},
	}, nil
}

func (v *validateCommand) runValidateUsingPlan() error {
	if len(v.plans) == 0 {
		v.logger.Info("Validation job is skipped, got empty plan from path: %s", v.planFilePath)
		return nil
	}

	plansByNamespace, plansDeleteByNamespace, err := v.getPlanByNamespace()
	if err != nil {
		return err
	}

	namespaces := make([]string, 0)
	for namespace := range plansByNamespace {
		namespaces = append(namespaces, namespace.Name)
	}
	for namespace := range plansDeleteByNamespace {
		if _, exist := plansByNamespace[namespace]; exist {
			continue
		}
		namespaces = append(namespaces, namespace.Name)
	}

	v.logger.Info("Validating job specifications for namespaces %s\n", namespaces)
	start := time.Now()
	defer v.logger.Info("Validation is finished, took %s", time.Since(start).Round(time.Second))

	if err := v.executeLocalValidation(); err != nil {
		return err
	}

	v.forDelete, v.fromServer = false, false
	if err := v.executeServerValidationWithPlans(plansByNamespace); err != nil {
		return err
	}

	v.forDelete, v.fromServer = true, true
	return v.executeServerValidationWithPlans(plansDeleteByNamespace)
}

func (v *validateCommand) getPlanFromFile() ([]*plan.Plan, error) {
	if v.planFilePath == "" {
		return nil, nil
	}

	v.usePlan = true
	file, err := os.Open(v.planFilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	bytes, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var plans []*plan.Plan
	err = json.Unmarshal(bytes, &plans)
	return plans, err
}

func (v *validateCommand) getPlanByNamespace() (map[*config.Namespace][]*plan.Plan, map[*config.Namespace][]*plan.Plan, error) {
	plansByNamespace := make(map[*config.Namespace][]*plan.Plan)
	plansDeleteByNamespace := make(map[*config.Namespace][]*plan.Plan)

	for i := range v.plans {
		if v.plans[i].ProjectName != v.clientConfig.Project.Name || v.plans[i].AllowJobValidate() {
			continue
		}

		namespace, err := v.clientConfig.GetNamespaceByName(v.plans[i].NamespaceName)
		if err != nil {
			return nil, nil, err
		}

		if v.plans[i].Operation == plan.OperationDelete {
			jobPlans := plansDeleteByNamespace[namespace]
			plansDeleteByNamespace[namespace] = append(jobPlans, v.plans[i])
			continue
		}

		jobPlans := plansByNamespace[namespace]
		plansByNamespace[namespace] = append(jobPlans, v.plans[i])
	}

	return plansByNamespace, plansDeleteByNamespace, nil
}

func (v *validateCommand) executeServerValidationWithPlans(plansByNamespace map[*config.Namespace][]*plan.Plan) error {
	for namespace, plans := range plansByNamespace {
		v.namespaceName = namespace.Name
		jobNames := make([]string, 0, len(plans))
		for i := range plans {
			jobNames = append(jobNames, plans[i].KindName)
		}
		v.jobNames = jobNames
		if err := v.executeServerValidation(namespace); err != nil {
			return err
		}
	}
	return nil
}
