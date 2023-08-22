package job

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/goto/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"

	"github.com/goto/optimus/client/cmd/internal"
	"github.com/goto/optimus/client/cmd/internal/connection"
	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/client/local/specio"
	"github.com/goto/optimus/config"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

const (
	changeNamespaceTimeout = time.Minute * 1
)

type changeNamespaceCommand struct {
	logger     log.Logger
	connection connection.Connection

	configFilePath string
	clientConfig   *config.ClientConfig

	project          string
	oldNamespaceName string
	newNamespaceName string
	host             string
}

// NewChangeNamespaceCommand initializes job namespace change command
func NewChangeNamespaceCommand() *cobra.Command {
	l := logger.NewClientLogger()
	changeNamespace := &changeNamespaceCommand{
		logger: l,
	}
	cmd := &cobra.Command{
		Use:      "change-namespace",
		Short:    "Change namespace of a Job",
		Example:  "optimus job change-namespace <job-name> --old-namespace <old-namespace> --new-namespace <new-namespace>",
		Args:     cobra.MinimumNArgs(1),
		PreRunE:  changeNamespace.PreRunE,
		RunE:     changeNamespace.RunE,
		PostRunE: changeNamespace.PostRunE,
	}
	// Config filepath flag
	cmd.Flags().StringVarP(&changeNamespace.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")
	internal.MarkFlagsRequired(cmd, []string{"old-namespace", "new-namespace"})
	changeNamespace.injectFlags(cmd)

	return cmd
}

func (c *changeNamespaceCommand) injectFlags(cmd *cobra.Command) {
	// Mandatory flags
	cmd.Flags().StringVarP(&c.oldNamespaceName, "old-namespace", "o", "", "current namespace of the job")
	cmd.Flags().StringVarP(&c.newNamespaceName, "new-namespace", "n", "", "namespace to which the job needs to be moved to")

	// Mandatory flags if config is not set
	cmd.Flags().StringVarP(&c.project, "project-name", "p", "", "Name of the optimus project")
	cmd.Flags().StringVar(&c.host, "host", "", "Optimus service endpoint url")
}

func (c *changeNamespaceCommand) PreRunE(_ *cobra.Command, _ []string) error {
	// Load mandatory config
	conf, err := config.LoadClientConfig(c.configFilePath)
	if err != nil {
		return err
	}

	c.clientConfig = conf
	c.connection = connection.New(c.logger, conf)

	return err
}

func (c *changeNamespaceCommand) RunE(_ *cobra.Command, args []string) error {
	jobName := args[0]
	err := c.sendChangeNamespaceRequest(jobName)
	if err != nil {
		return fmt.Errorf("namespace change request failed for job %s: %w", jobName, err)
	}
	c.logger.Info("[OK] Successfully changed namespace and deployed new DAG on Scheduler")
	return nil
}

func (c *changeNamespaceCommand) sendChangeNamespaceRequest(jobName string) error {
	conn, err := c.connection.Create(c.host)
	if err != nil {
		return err
	}
	defer conn.Close()

	// fetch Instance by calling the optimus API
	jobRunServiceClient := pb.NewJobSpecificationServiceClient(conn)
	request := &pb.ChangeJobNamespaceRequest{
		ProjectName:      c.project,
		NamespaceName:    c.oldNamespaceName,
		NewNamespaceName: c.newNamespaceName,
		JobName:          jobName,
	}

	ctx, dialCancel := context.WithTimeout(context.Background(), changeNamespaceTimeout)
	defer dialCancel()

	_, err = jobRunServiceClient.ChangeJobNamespace(ctx, request)
	return err
}

func getJobParentsFilePathList(fileFS afero.Fs, jobDirPath string) ([]string, error) {
	var parentsFilePathList []string
	var errorMsg string
	splitDirPaths := strings.Split(jobDirPath, "/")
	for i := range splitDirPaths {
		pathNearSpecIdx := len(splitDirPaths) - i
		rootToNearSpecPaths := splitDirPaths[:pathNearSpecIdx]
		parentDirPath := filepath.Join(rootToNearSpecPaths...)
		parenFilePath := filepath.Join(parentDirPath, "this.yaml")
		exist, err := afero.Exists(fileFS, parenFilePath)
		if err != nil {
			errorMsg += fmt.Sprintf("Unable to read directory %s to check 'this.yaml', err: %s\n", parenFilePath, err.Error())
			continue
		}
		if exist {
			parentsFilePathList = append(parentsFilePathList, parenFilePath)
		}
	}
	var err error
	if len(errorMsg) > 0 {
		err = errors.New(errorMsg)
	}
	return parentsFilePathList, err
}

func mergeYAMLFiles(fs afero.Fs, sourcePath, targetPath string) error {
	sourceObj, err := readYaml(fs, sourcePath)
	if err != nil {
		return err
	}
	targetObj, err := readYaml(fs, targetPath)
	if err != nil {
		return err
	}
	mergedObj, err := mergeObjects(sourceObj, targetObj)
	if err != nil {
		return err
	}
	mergedFile, err := yaml.Marshal(mergedObj)
	if err != nil {
		return err
	}
	return afero.WriteFile(fs, targetPath, mergedFile, os.ModePerm)
}

func mergeObjects(interfaces ...interface{}) (interface{}, error) {
	merged := make(map[string]interface{})
	for _, intf := range interfaces {
		val := reflect.ValueOf(intf)
		if val.Kind() != reflect.Map {
			return nil, errors.New("automatic merge not possible")
		}
		iter := val.MapRange()
		for iter.Next() {
			key := iter.Key().Interface()
			value := iter.Value().Interface()
			if existingValue, exists := merged[key.(string)]; exists {
				mergedValue := mergeValues(existingValue, value)
				merged[key.(string)] = mergedValue
			} else {
				merged[key.(string)] = value
			}
		}
	}
	return merged, nil
}

func mergeValues(value1, value2 interface{}) interface{} {
	val1 := reflect.ValueOf(value1)
	val2 := reflect.ValueOf(value2)

	if val1.Kind() != val2.Kind() {
		return value2
	}

	switch val1.Kind() {
	case reflect.Map:
		merged := make(map[string]interface{})
		iter1 := val1.MapRange()
		for iter1.Next() {
			key := iter1.Key().Interface()
			value := iter1.Value().Interface()
			merged[key.(string)] = value
		}
		iter2 := val2.MapRange()
		for iter2.Next() {
			key := iter2.Key().Interface()
			value := iter2.Value().Interface()
			if existingValue, exists := merged[key.(string)]; exists {
				mergedValue := mergeValues(existingValue, value)
				merged[key.(string)] = mergedValue
			} else {
				merged[key.(string)] = value
			}
		}
		return merged

	default:
		return value2
	}
}

func findConflicts(incomming, existing interface{}, incomingFileName, existingFileName, path string) []string {
	val1 := reflect.ValueOf(incomming)
	val2 := reflect.ValueOf(existing)
	var diffrences []string

	if val1.Kind() != val2.Kind() {
		diffrences = append(diffrences, fmt.Sprintf("Mismatched types at path %s\n", path))
		return diffrences
	}

	switch val1.Kind() {
	case reflect.Map:
		iter := val1.MapRange()
		for iter.Next() {
			key := iter.Key()
			val1Child := iter.Value()
			val2Child := val2.MapIndex(key)
			newPath := fmt.Sprintf("%s.%v", path, key.Interface())
			if val2Child.IsValid() {
				diffrences = append(diffrences, findConflicts(val1Child.Interface(), val2Child.Interface(), incomingFileName, existingFileName, newPath)...)
			}
		}

	case reflect.Slice, reflect.Array:
		len1 := val1.Len()
		len2 := val2.Len()
		maxLen := len1
		if len2 > maxLen {
			maxLen = len2
		}
		for i := 0; i < maxLen; i++ {
			newPath := fmt.Sprintf("%s[%d]", path, i)
			if i < len1 && i < len2 {
				diffrences = append(diffrences, findConflicts(val1.Index(i).Interface(), val2.Index(i).Interface(), incomingFileName, existingFileName, newPath)...)
			} else if i < len1 {
				diffrences = append(diffrences, fmt.Sprintf("Conflicting Array contents: item %s is missing in %s\n", newPath, existingFileName))
			}
		}

	default:
		if !reflect.DeepEqual(incomming, existing) {
			diffrences = append(diffrences, fmt.Sprintf("Property %s has conflicting values:\n\t- %s: %v\n\t- %s: %v\n", path, incomingFileName, incomming, existingFileName, existing))
		}
	}
	return diffrences
}

func readYaml(fs afero.Fs, filePath string) (interface{}, error) {
	file, err := fs.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening file under [%s]: %w", filePath, err)
	}
	defer file.Close()
	contents, err := afero.ReadAll(file)
	if err != nil {
		return nil, err
	}
	var fileInterface interface{}
	if err := yaml.Unmarshal(contents, &fileInterface); err != nil {
		return nil, fmt.Errorf("error decoding spec under [%s]: %w", filePath, err)
	}
	return fileInterface, nil
}

func areFilesIdentical(fs afero.Fs, filePath1, filePath2 string) bool {
	file1, err := fs.Open(filePath1)
	if err != nil {
		return false
	}
	defer file1.Close()

	file2, err := fs.Open(filePath2)
	if err != nil {
		return false
	}
	defer file2.Close()

	contents1, err := afero.ReadAll(file1)
	if err != nil {
		return false
	}

	contents2, err := afero.ReadAll(file2)
	if err != nil {
		return false
	}
	if len(contents1) != len(contents2) {
		return false
	}

	for i := range contents1 {
		if contents1[i] != contents2[i] {
			return false
		}
	}
	return true
}

func fileCopy(fs afero.Fs, srcFilePath, dstFilePath string) error {
	srcFile, err := fs.Open(srcFilePath)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	// Create or open the destination file for writing
	dstFile, err := fs.Create(dstFilePath)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	// Copy the content from the source file to the destination file
	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		return err
	}
	return nil
}

func (c *changeNamespaceCommand) identifyParentSpecConflicts(fs afero.Fs, oldNamespaceParentSpecs, newNamespaceParentSpecs []string) error {
	var conflicts []string
	for _, oldParentSpec := range oldNamespaceParentSpecs {
		incomingYaml, err := readYaml(fs, oldParentSpec)
		if err != nil {
			return err
		}
		for _, newParentSpec := range newNamespaceParentSpecs {
			existingYaml, err := readYaml(fs, newParentSpec)
			if err != nil {
				return err
			}
			conflicts = append(conflicts, findConflicts(incomingYaml, existingYaml, oldParentSpec, newParentSpec, "")...)
		}
	}
	if len(conflicts) > 0 {
		c.logger.Error("\n[err] Found Following conflicts\n")
		for _, conflict := range conflicts {
			c.logger.Error(conflict)
		}
		return errors.New("found conflicts")
	}
	return nil
}

func (c *changeNamespaceCommand) PostRunE(_ *cobra.Command, args []string) error {
	c.logger.Info("\n[info] Moving job in filesystem")
	jobName := args[0]

	oldNamespaceConfig, err := c.getNamespaceConfig(c.oldNamespaceName)
	if err != nil {
		c.logger.Error("[error] old namespace unregistered in filesystem, err: %s", err.Error())
		return nil
	}

	jobSpecReadWriter, err := specio.NewJobSpecReadWriter(afero.NewOsFs())
	if err != nil {
		c.logger.Error("[error] could not instantiate Spec Read/Writer, err: %s", err.Error())
		return nil
	}

	jobSpec, err := jobSpecReadWriter.ReadByName(oldNamespaceConfig.Job.Path, jobName)
	if err != nil {
		c.logger.Error("[error] unable to find job in old namespace directory, err: %s", err.Error())
		return nil
	}

	fs := afero.NewOsFs()
	newNamespaceConfig, err := c.getNamespaceConfig(c.newNamespaceName)
	if err != nil || newNamespaceConfig.Job.Path == "" {
		c.logger.Warn("[warn] new namespace not recognised for jobs")
		c.logger.Warn("[info] run `optimus job export` on the new namespace repo, to fetch the newly moved job.")

		c.logger.Warn("[info] removing job from old namespace")
		err = fs.RemoveAll(jobSpec.Path)
		if err != nil {
			c.logger.Error("[error] unable to remove job from old namespace , err: %s", err.Error())
			c.logger.Warn("[info] consider deleting source files manually if they exist")
			return nil
		}
		c.logger.Warn("[OK] removed job spec from current namespace directory")
		return nil
	}

	newJobPath := strings.Replace(jobSpec.Path, oldNamespaceConfig.Job.Path, newNamespaceConfig.Job.Path, 1)

	c.logger.Info("\t* Old Path : '%s' \n\t* New Path : '%s' \n", jobSpec.Path, newJobPath)

	c.logger.Info("[info] creating job directry: %s", newJobPath)
	err = fs.MkdirAll(filepath.Dir(newJobPath), os.ModePerm)
	if err != nil {
		c.logger.Error("[err] unable to create path in the new namespace directory, err: %s", err.Error())
		c.logger.Warn("[warn] unable to move job from old namespace")
		c.logger.Warn("[info] consider moving source files manually")
		return nil
	}

	err = fs.Rename(jobSpec.Path, newJobPath)
	if err != nil {
		c.logger.Error("[warn] unable to move job from old namespace, err: %s", err.Error())
		c.logger.Warn("[info] consider moving source files manually")
		return nil
	}
	c.logger.Info("[OK] Job moved successfully")
	c.logger.Info("[info] Identifying and moving parent spec files 'this.yaml'")
	oldNamespaceParentFilePaths, err := getJobParentsFilePathList(fs, jobSpec.Path)
	if err != nil {
		c.logger.Error("[err]  unable to find parent spec files for Job at %s, err: %s", jobSpec.Path, err.Error())
	}
	newNamespaceParentFilePaths, err := getJobParentsFilePathList(fs, newJobPath)
	if err != nil {
		c.logger.Error("[err]  unable to find parent spec files for Job at %s, err: %s", newJobPath, err.Error())
	}

	err = c.identifyParentSpecConflicts(fs, oldNamespaceParentFilePaths, newNamespaceParentFilePaths)
	if err != nil {
		c.logger.Error("[err]  cant merge this.yaml files automatically, err:%s", err.Error())
		c.logger.Warn("[warn] consider merging the following files manually")
		c.logger.Warn("[info] from namespace '%s', all this.yaml at %v", c.oldNamespaceName, oldNamespaceParentFilePaths)
		c.logger.Warn("[info] to   namespace '%s', all this.yaml at %v", c.newNamespaceName, newNamespaceParentFilePaths)
	} else {
		for _, parentFilePath := range oldNamespaceParentFilePaths {
			newParentFilePath := strings.Replace(parentFilePath, oldNamespaceConfig.Job.Path, newNamespaceConfig.Job.Path, 1)
			exists, err := afero.Exists(fs, newParentFilePath)
			if err != nil {
				c.logger.Error("[err]  unable to check if parent spec file exists already in target namespace, err:%s", err.Error())
				c.logger.Warn("[warn] consider merging/Coping the following files manually")
				c.logger.Warn("\tfrom namespace '%s', %v", c.oldNamespaceName, parentFilePath)
				c.logger.Warn("\tto   namespace '%s', %v", c.newNamespaceName, newParentFilePath)
			}
			if exists {
				if areFilesIdentical(fs, parentFilePath, newParentFilePath) {
					c.logger.Info("[info] parent spec files are identical ")
					continue
				}
				c.logger.Info("[info] parent spec file already exists at %s, attempting to merge %s with it", newParentFilePath, parentFilePath)
				err = mergeYAMLFiles(fs, parentFilePath, newParentFilePath)
				if err != nil {
					c.logger.Error("[err] could not merge yaml files, err:%s", err.Error())
					c.logger.Warn("[warn] consider merging the following files manually")
					c.logger.Warn("\tfrom namespace '%s', %v", c.oldNamespaceName, parentFilePath)
					c.logger.Warn("\tto   namespace '%s', %v", c.newNamespaceName, newParentFilePath)
				}
				c.logger.Info("[ok] successfully merged 'this.yaml' from %s to %s ", parentFilePath, newParentFilePath)
			} else {
				c.logger.Info("[info] moving parent spec file %s to new namespace %s", newParentFilePath, parentFilePath)
				err = fileCopy(fs, parentFilePath, newParentFilePath)
				if err != nil {
					c.logger.Error("[warn] unable to move/merge parent spec from old namespace, err: %s", err.Error())
					c.logger.Warn("[info] consider Coping source files manually")
					c.logger.Warn("\tfrom namespace '%s', %v", c.oldNamespaceName, parentFilePath)
					c.logger.Warn("\tto   namespace '%s', %v", c.newNamespaceName, newParentFilePath)
				}
				c.logger.Info("[ok] successfully moved 'this.yaml' from %s to %s ", parentFilePath, newParentFilePath)
			}
		}
	}
	return nil
}

func (c *changeNamespaceCommand) getNamespaceConfig(namespaceName string) (*config.Namespace, error) {
	for _, namespace := range c.clientConfig.Namespaces {
		if namespace.Name == namespaceName {
			return namespace, nil
		}
	}
	return nil, errors.New("namespace not recognised in config")
}
