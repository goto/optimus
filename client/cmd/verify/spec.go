package verify

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/goto/salt/log"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/goto/optimus/client/cmd/internal"
	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/client/cmd/internal/plan"
	schema "github.com/goto/optimus/client/jsonschema"
	"github.com/goto/optimus/config"
)

type validateSpecCommand struct {
	logger log.Logger

	configFilePath string
	clientConfig   *config.ClientConfig

	planFilePath string
	specCompiler *jsonschema.Compiler

	plan *plan.Plan

	jobPath      string
	resourcePath string

	namespace string
}

const (
	jobFile             = "job.yaml"
	resourceFile        = "resource.yaml"
	dataStoreMaxCompute = "maxcompute"
)

// NewValidateSpecCommand initialize command to list backup
func NewValidateSpecCommand() *cobra.Command {
	validate := &validateSpecCommand{
		logger: logger.NewClientLogger(),
	}

	cmd := &cobra.Command{
		Use:     "specs",
		Short:   "validate specs using optimus json schema",
		Example: "optimus verify specs",
		RunE:    validate.RunE,
		PreRunE: validate.PreRunE,
	}

	cmd.Flags().StringVarP(&validate.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")
	cmd.Flags().StringVarP(&validate.planFilePath, "plan", "p", config.EmptyPath, "File path for plan output")

	cmd.Flags().StringVarP(&validate.jobPath, "job", "j", config.EmptyPath, "File path for job spec")
	cmd.Flags().StringVarP(&validate.resourcePath, "resource", "r", config.EmptyPath, "File path for resource spec")

	cmd.Flags().StringVarP(&validate.namespace, "namespace", "n", "", "Validate all jobs and resources in a given namespace")

	return cmd
}

func (v *validateSpecCommand) readPlan() (*plan.Plan, error) {
	file, err := os.Open(v.planFilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	bytes, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	plan := &plan.Plan{}
	err = json.Unmarshal(bytes, plan)
	return plan, err
}

func (v *validateSpecCommand) PreRunE(_ *cobra.Command, _ []string) error {
	// Load config
	conf, err := internal.LoadOptionalConfig(v.configFilePath)
	if err != nil {
		return err
	}
	v.clientConfig = conf
	v.specCompiler = schema.GetCompiler()
	return nil
}

func (v *validateSpecCommand) RunE(_ *cobra.Command, _ []string) error {
	switch {
	case v.planFilePath != "":
		v.logger.Info("\nValidating Specs (Plan Mode)")
		v.logger.Info("└─ Using plan file: %s", v.planFilePath)

		plan, err := v.readPlan()
		if err != nil {
			return err
		}
		v.plan = plan

		if v.validateSpecUsingPlan(v.plan) != nil {
			return errors.New("validate spec using plan failed")
		}

	case v.jobPath != "":
		v.logger.Info("\nValidating Specs (Single Spec Mode)")
		v.logger.Info("└─ Job file: %s", v.jobPath)
		if v.validateJobSpecs(v.jobPath) != nil {
			return errors.New("validate job spec failed")
		}

	case v.resourcePath != "":
		v.logger.Info("\nValidating Specs (Single Spec Mode)")
		v.logger.Info("└─ Resource file: %s", v.resourcePath)
		if v.validateResourceSpecs(v.resourcePath) != nil {
			return errors.New("validate resource spec failed")
		}

	case v.namespace != "":
		v.logger.Info("\nValidating Specs (Namespace Mode)")
		v.logger.Info("└─ Namespace: %s", v.namespace)
		namespace, err := v.clientConfig.GetNamespaceByName(v.namespace)
		if err != nil {
			v.logger.Error("Failed to get namespace: %s", err.Error())
			return err
		}

		var errs []error
		jobFolder := namespace.Job.Path
		if jobFolder != "" {
			v.logger.Info("\nValidating all job specs in namespace: [%s]", v.namespace)
			err = v.validateJobSpecsInDir(jobFolder)
			if err != nil {
				v.logger.Error("Failed to validate job specs in directory %s", jobFolder)
				errs = append(errs, err)
			}
			v.sectionBreak()
		}

		var resourcesFolder string
		for _, v := range namespace.Datastore {
			if v.Type == dataStoreMaxCompute {
				resourcesFolder = v.Path
				break
			}
		}
		if resourcesFolder != "" {
			v.logger.Info("\nValidating all resource specs in namespace: [%s]", v.namespace)
			err = v.validateResourceSpecsInDir(resourcesFolder)
			if err != nil {
				v.logger.Error("Failed to validate resource specs in directory %s", resourcesFolder)
				errs = append(errs, err)
			}
		}

		if errors.Join(errs...) != nil {
			return errors.New("validate job spec failed")
		}

	default:
		v.logger.Info("\nNo validation mode specified")
		v.logger.Info("Available Modes (in order of precedence):")
		v.logger.Info("├─ Plan Mode (-p, --plan)")
		v.logger.Info("│  └─ Validate specs using a plan file")
		v.logger.Info("├─ Single Spec Mode (-j, --job or -r, --resource)")
		v.logger.Info("│  └─ Validate a single job or resource spec")
		v.logger.Info("├─ Namespace Mode (-ns, --namespace)")
		v.logger.Info("│  └─ Validate all specs in a namespace")
		v.logger.Info("└─ Use -c or --config to specify client configuration file")
		return errors.New("invalid invocation")
	}

	return nil
}

func getAllSpecsInDir(rootDir, targetFileName string) ([]string, error) {
	fs := afero.NewOsFs()

	var matchedFiles []string

	err := afero.Walk(fs, rootDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return fmt.Errorf("error accessing path %s: %w", path, walkErr)
		}
		if !info.IsDir() && filepath.Base(path) == targetFileName {
			matchedFiles = append(matchedFiles, path)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to walk directory %s: %w", rootDir, err)
	}

	return matchedFiles, nil
}

func (v *validateSpecCommand) validateJobSpecsInDir(jobDir string) error {
	jobSchema, err := v.specCompiler.Compile("embed://job.json")
	if err != nil {
		v.logger.Error("Failed to compile job schema validation, Error: %s", err.Error())
		return err
	}
	allSpecsInDir, err := getAllSpecsInDir(jobDir, jobFile)
	if err != nil {
		v.logger.Error("Failed to get all job specs in directory %s, Error: %s", jobDir, err.Error())
		return err
	}
	var me []error
	for _, jobSpecPath := range allSpecsInDir {
		v.logger.Info("Validating job spec: %s", jobSpecPath)
		err := v.validateSpec(jobSchema, jobSpecPath)
		if err != nil {
			me = append(me, err)
		}
	}
	return errors.Join(me...)
}

func (v *validateSpecCommand) validateResourceSpecsInDir(resourceDir string) error {
	resourceSchema, err := v.specCompiler.Compile("embed://resource.json")
	if err != nil {
		v.logger.Error("Failed to compile resource schema validation, Error: %s", err.Error())
		return err
	}
	allSpecsInDir, err := getAllSpecsInDir(resourceDir, resourceFile)
	if err != nil {
		v.logger.Error("Failed to get all resource specs in directory %s, Error: %s", resourceDir, err.Error())
		return err
	}
	var errs []error
	for _, resourceSpecPath := range allSpecsInDir {
		v.logger.Info("Validating resource spec: %s", resourceSpecPath)
		errs = append(errs, v.validateSpec(resourceSchema, resourceSpecPath))
	}
	return errors.Join(errs...)
}

func (v *validateSpecCommand) validateJobSpecs(jobPath string) error {
	jobSchema, err := v.specCompiler.Compile("embed://job.json")
	if err != nil {
		v.logger.Error("Failed to compile job schema validation, Error: %s", err.Error())
		return err
	}
	return v.validateSpec(jobSchema, jobPath)
}

func (v *validateSpecCommand) validateResourceSpecs(resourcePath string) error {
	resourceSchema, err := v.specCompiler.Compile("embed://resource.json")
	if err != nil {
		v.logger.Error("Failed to compile resource schema validation, Error: %s", err.Error())
		return err
	}
	return v.validateSpec(resourceSchema, resourcePath)
}

func (v *validateSpecCommand) sectionBreak() {
	v.logger.Info("\n%s", strings.Repeat("-", 50))
}

func (v *validateSpecCommand) validateJobPlan(jobSchema *jsonschema.Schema, jobs []*plan.JobPlan) error {
	errs := []error{}
	for _, j1 := range jobs {
		v.logger.Info("\t├─ ⏳ Validating job: %s", j1.Path)
		jobSpecPath := path.Join(j1.Path, jobFile)
		errs = append(errs, v.validateSpec(jobSchema, jobSpecPath))
	}
	return errors.Join(errs...)
}

func (v *validateSpecCommand) validateSpec(jobSchema *jsonschema.Schema, specPath string) error {
	spec, err := readSpec(specPath)
	if err != nil {
		v.logger.Error("\t└─ ❌ Error reading spec at : %s, Error: %v", specPath, err)
		return err
	}

	err = jobSchema.Validate(spec)
	if err != nil {
		v.logger.Error("\t│    Invalid spec at : %s", specPath)
		for _, errLine := range strings.Split(err.Error(), "\n")[1:] {
			v.logger.Error("\t│      %s", errLine)
		}
		v.logger.Error("\t└─ ❌ Spec Validation Failed")
		return err
	}
	v.logger.Info("\t└─ ✅ Valid spec")
	return nil
}

func (v *validateSpecCommand) validateResourcePlan(resourceSchema *jsonschema.Schema, resources []*plan.ResourcePlan) error {
	errs := []error{}
	for _, r1 := range resources {
		v.logger.Info("\t├─ ⏳ Validating resource: %s", r1.Path)
		resourceSpecPath := path.Join(r1.Path, resourceFile)
		err := v.validateSpec(resourceSchema, resourceSpecPath)
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (v *validateSpecCommand) validateJobOperations(jobOperations plan.OperationByNamespaces[*plan.JobPlan]) error {
	errs := []error{}
	jobSchema, err := v.specCompiler.Compile("embed://job.json")
	if err != nil {
		v.logger.Error("Failed to compile job schema validation, Error: %s", err.Error())
		return err
	}

	namespacesForCreate := jobOperations.Create.GetAllNamespaces()
	if len(namespacesForCreate) != 0 {
		v.logger.Info("\nValidating JOB CREATE plans")

		for _, namespaceName := range jobOperations.Create.GetAllNamespaces() {
			v.logger.Info("\n [%s]", namespaceName)
			jobs := jobOperations.Create.GetByNamespace(namespaceName)
			if len(jobs) == 0 {
				v.logger.Info("\t└─ No jobs to validate")
				continue
			}
			errs = append(errs, v.validateJobPlan(jobSchema, jobs))
		}
	}

	namespacesForUpdate := jobOperations.Update.GetAllNamespaces()
	if len(namespacesForUpdate) != 0 {
		v.logger.Info("\nValidating JOB UPDATE plans")
		for _, namespaceName := range jobOperations.Update.GetAllNamespaces() {
			v.logger.Info("\n [%s]", namespaceName)
			jobs := jobOperations.Update.GetByNamespace(namespaceName)
			if len(jobs) == 0 {
				v.logger.Info("\t└─ No jobs to validate")
				continue
			}
			errs = append(errs, v.validateJobPlan(jobSchema, jobs))
		}
	}

	return errors.Join(errs...)
}

func (v *validateSpecCommand) validateResourceOperations(resourceOperations plan.OperationByNamespaces[*plan.ResourcePlan]) error {
	errs := []error{}
	resourceSchema, err := v.specCompiler.Compile("embed://resource.json")
	if err != nil {
		v.logger.Error("Failed to compile resource schema validation, Error: %s", err.Error())
		return err
	}
	namespacesForCreate := resourceOperations.Create.GetAllNamespaces()
	if len(namespacesForCreate) != 0 {
		v.logger.Info("\nValidating RESOURCE CREATE plans")
		for _, namespaceName := range resourceOperations.Create.GetAllNamespaces() {
			v.logger.Info("\n [%s]", namespaceName)
			resources := resourceOperations.Create.GetByNamespace(namespaceName)
			if len(resources) == 0 {
				v.logger.Info("\t└─ No resources to validate")
				continue
			}
			errs = append(errs, v.validateResourcePlan(resourceSchema, resources))
		}
	}

	namespacesForUpdate := resourceOperations.Update.GetAllNamespaces()
	if len(namespacesForUpdate) != 0 {
		v.logger.Info("\nValidating RESOURCE UPDATE plans")
		for _, namespaceName := range resourceOperations.Update.GetAllNamespaces() {
			v.logger.Info("\n [%s]", namespaceName)
			resources := resourceOperations.Update.GetByNamespace(namespaceName)
			if len(resources) == 0 {
				v.logger.Info("\t└─ No resources to validate")
				continue
			}
			errs = append(errs, v.validateResourcePlan(resourceSchema, resources))
		}
	}
	return errors.Join(errs...)
}

func (v *validateSpecCommand) validateSpecUsingPlan(p1 *plan.Plan) error {
	v.sectionBreak()
	err1 := v.validateJobOperations(p1.Job)

	v.sectionBreak()
	err2 := v.validateResourceOperations(p1.Resource)

	return errors.Join(err1, err2)
}

func readSpec(filePath string) (any, error) {
	var spec any
	fileSpec, err := os.Open(filePath)
	if err != nil {
		return spec, fmt.Errorf("error opening spec under [%s]: %w", filePath, err)
	}
	defer fileSpec.Close()

	if err = yaml.NewDecoder(fileSpec).Decode(&spec); err != nil {
		return spec, fmt.Errorf("error decoding spec under [%s]: %w", filePath, err)
	}

	return spec, nil
}
