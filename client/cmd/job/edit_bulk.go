package job

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/goto/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/client/local/model"
	"github.com/goto/optimus/client/local/specio"
)

type bulkEditCommand struct {
	logger log.Logger
}

func NewBulkEditCommand() *cobra.Command {
	bulkEdit := &bulkEditCommand{
		logger: logger.NewClientLogger(),
	}

	cmd := &cobra.Command{
		Use:     "edit-all",
		Example: "optimus job edit-all ",
		RunE:    bulkEdit.RunE,
	}
	return cmd
}

func insertStrings(original []string, index int, toInsert []string) []string {
	if index < 0 || index > len(original) {
		// handle out of range index gracefully
		fmt.Printf("⚠️  index %d out of range, appending at end\n", index)
		index = len(original)
	}

	// Slice trick: split, append, and merge
	result := append(original[:index], append(toInsert, original[index:]...)...)
	return result
}

func editTask(lines []string) []string {
	index := 0
	for i, line := range lines {
		if line == "task:" {
			index = i
		}
	}
	toInsert := []string{
		"  alert:",
		"    sla_miss:",
		"      - auto_threshold: true",
		"        severity: INFO",
		"        team: data_batching",
	}
	lines = insertStrings(lines, index+1, toInsert)
	return lines
}

func editHooks(lines, hookNames []string) []string {
	index := 0
	for _, hookName := range hookNames {
		index = 0
		for i, line := range lines {
			if line == fmt.Sprintf("- name: %s", hookName) {
				index = i
			}
		}
		toInsert := []string{
			"  alert:",
			"    sla_miss:",
			"      - auto_threshold: true",
			"        severity: INFO",
			"        team: data_batching",
		}
		lines = insertStrings(lines, index+1, toInsert)
	}
	return lines
}

func removeTrailingLines(lines []string) []string {
	for len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}

func editFile(filePath string, task bool, hookNames []string) ([]string, error) {
	fileName := path.Join(".", filePath, "job.yaml")

	data, err := os.ReadFile(fileName)
	if err != nil {
		fmt.Println("Error in read file ", err.Error())
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	lines := strings.Split(string(data), "\n")

	if task {
		lines = editTask(lines)
	}
	lines = editHooks(lines, hookNames)

	lines = removeTrailingLines(lines)

	tempFilePath := fileName + ".tmp"
	tempFile, err := os.Create(tempFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	defer tempFile.Close()

	writer := bufio.NewWriter(tempFile)
	for _, l := range lines {
		_, _ = writer.WriteString(l + "\n")
	}
	if err := writer.Flush(); err != nil {
		return nil, fmt.Errorf("failed to write updated content: %w", err)
	}
	if err := os.Remove(fileName); err != nil {
		return nil, fmt.Errorf("failed to remove original file: %w", err)
	}
	if err := os.Rename(tempFilePath, fileName); err != nil {
		return nil, fmt.Errorf("failed to replace original file: %w", err)
	}

	return lines, nil
}

func needsAlerts(job *model.JobSpec) bool {
	if job.Task.Window.Offset != "" {
		return false
	}
	for _, val := range job.Labels {
		if val == "sla-10am" {
			return true
		}
	}
	return false
}

func needsTaskAlert(job *model.JobSpec) bool {
	if job.Task.Alerts == nil || len(job.Task.Alerts.SLAMiss) == 0 {
		return true
	}
	fmt.Println("task alerts already set")
	return false
}

func needsHookAlert(job *model.JobSpec) []string {
	var hookNames []string
	for _, hook := range job.Hooks {
		if hook.Alerts == nil || len(hook.Alerts.SLAMiss) == 0 {
			hookNames = append(hookNames, hook.Name)
		} else {
			fmt.Println("hook alerts already set")
		}
	}
	return hookNames
}

func (r *bulkEditCommand) RunE(_ *cobra.Command, _ []string) error {
	jobSpecReadWriter, err := specio.NewJobSpecReadWriter(afero.NewOsFs(), specio.WithJobSpecParentReading())
	if err != nil {
		return err
	}

	jobSpecs, err := jobSpecReadWriter.ReadAll(".")
	if err != nil {
		return err
	}

	for _, job := range jobSpecs {
		if needsAlerts(job) {
			task := needsTaskAlert(job)
			hookNames := needsHookAlert(job)
			fileName := path.Join(".", job.Path, "job.yaml")
			fmt.Printf("Need Alerts, Job: %s, task: %t , hookNames: %v, path: %s\n", job.Name, task, hookNames, fileName)
			if task || len(hookNames) > 0 {
				_, err := editFile(job.Path, task, hookNames)
				if err != nil {
					fmt.Println("Error in editFile ", err.Error())
				}
			}
		}
	}
	return nil
}
