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

	configFilePath string
	namespaceName  string
	jobNames       []string

	fromServer bool
	forDelete  bool
	verbose    bool

	clientConfig *config.ClientConfig

	usePlan      bool
	planFilePath string
	jobPlan      *plan.Plan
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
	cmd.Flags().StringVarP(&validate.planFilePath, "plan", "p", config.EmptyPath, "File path for plan output")
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

	err = v.storeJobPlan()
	if err != nil {
		return err
	}

	v.connection = connection.New(v.logger, conf)
	return nil
}

func (v *validateCommand) RunE(_ *cobra.Command, _ []string) error {
	if v.usePlan {
		if v.invalidPlanFlag() {
			return errors.New("[for-delete,from-server,namespace,jobs] flag should not be send when validate using plan")
		}
		return v.runValidateUsingPlan(v.jobPlan)
	}

	if v.namespaceName == "" {
		return errors.New("namespace flag is required on validate using non plan")
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

	if err := v.executeServerValidation(namespace, v.jobNames, v.fromServer); err != nil {
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

func (v *validateCommand) executeServerValidation(namespace *config.Namespace, jobNames []string, fromServer bool) error {
	var (
		request *pb.ValidateRequest
		err     error
	)

	if fromServer {
		request = v.getRequestForJobNames(namespace, jobNames)
	} else {
		request, err = v.getRequestForJobSpecs(namespace, jobNames)
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

func (v *validateCommand) getRequestForJobNames(namespace *config.Namespace, jobNames []string) *pb.ValidateRequest {
	return &pb.ValidateRequest{
		ProjectName:   v.clientConfig.Project.Name,
		NamespaceName: namespace.Name,
		Payload: &pb.ValidateRequest_FromServer_{
			FromServer: &pb.ValidateRequest_FromServer{
				JobNames:     jobNames,
				DeletionMode: v.forDelete,
			},
		},
	}
}

func (v *validateCommand) getRequestForJobSpecs(namespace *config.Namespace, jobNames []string) (*pb.ValidateRequest, error) {
	jobSpecReadWriter, err := specio.NewJobSpecReadWriter(afero.NewOsFs(), specio.WithJobSpecParentReading())
	if err != nil {
		return nil, err
	}

	jobSpecs, err := jobSpecReadWriter.ReadAll(namespace.Job.Path)
	if err != nil {
		return nil, err
	}

	if len(jobNames) > 0 {
		specByName := make(map[string]*model.JobSpec)
		for _, spec := range jobSpecs {
			specByName[spec.Name] = spec
		}

		var specsToValidate []*model.JobSpec
		for _, name := range jobNames {
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

func (v *validateCommand) storeJobPlan() error {
	if v.planFilePath == "" {
		return nil
	}

	v.usePlan = true
	file, err := os.Open(v.planFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	bytes, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	return json.Unmarshal(bytes, &v.jobPlan)
}

func (v *validateCommand) invalidPlanFlag() bool {
	return v.forDelete || v.fromServer || v.namespaceName != "" || len(v.jobNames) > 0
}

func (v *validateCommand) runValidateUsingPlan(jobPlan *plan.Plan) error {
	v.logger.Info("Validating job specifications for namespaces %s\n", jobPlan.Job.GetAllNamespaces())
	start := time.Now()
	defer v.logger.Info("Validation is finished, took %s", time.Since(start).Round(time.Second))

	if err := v.executeLocalValidation(); err != nil {
		return err
	}

	if err := v.executeServerValidationByNamespaces(jobPlan.Job.Create.GetPlansByNamespaceName(), false); err != nil {
		return err
	}
	if err := v.executeServerValidationByNamespaces(jobPlan.Job.Update.GetPlansByNamespaceName(), false); err != nil {
		return err
	}
	return v.executeServerValidationByNamespaces(jobPlan.Job.Delete.GetPlansByNamespaceName(), true)
}

func (v *validateCommand) executeServerValidationByNamespaces(jobNamesByNamespace map[string][]string, fromServer bool) error {
	for namespace, jobNames := range jobNamesByNamespace {
		namespaceConfig, err := v.clientConfig.GetNamespaceByName(namespace)
		if err != nil {
			return err
		}

		if err := v.executeServerValidation(namespaceConfig, jobNames, fromServer); err != nil {
			return err
		}
	}
	return nil
}
