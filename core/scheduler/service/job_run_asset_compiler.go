package service

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/goto/salt/log"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/internal/lib/interval"
)

type FilesCompiler interface {
	Compile(fileMap map[string]string, context map[string]any) (map[string]string, error)
}

type JobRunAssetsCompiler struct {
	compiler FilesCompiler

	logger log.Logger
}

func NewJobAssetsCompiler(engine FilesCompiler, logger log.Logger) *JobRunAssetsCompiler {
	return &JobRunAssetsCompiler{
		compiler: engine,
		logger:   logger,
	}
}

func (c *JobRunAssetsCompiler) CompileJobRunAssets(_ context.Context, job *scheduler.Job, systemEnvVars map[string]string, interval interval.Interval, contextForTask map[string]interface{}) (map[string]string, error) {
	const (
		bq2bq = "bq2bq"
		mc2mc = "mc2mc"
	)
	inputFiles := map[string]string{}
	maps.Copy(inputFiles, job.Assets)
	method, ok1 := job.Task.Config["LOAD_METHOD"]
	query, ok2 := job.Assets["query.sql"]
	disableMultiQuery := strings.ToLower(job.Task.Config["DISABLE_MULTI_QUERY_GENERATION"])
	if job.Task.Name == mc2mc { // mc2mc plugin uses query file path to locate the query file
		pathOnAsset, found := strings.CutPrefix(job.Task.Config["QUERY_FILE_PATH"], "/data/in/")
		if !found {
			c.logger.Warn(fmt.Sprintf("error compiling assets: query file path is not valid, %s, expect to have prefix %s, fallback to \"query.sql\"", job.Task.Config["QUERY_FILE_PATH"], "/data/in/"))
		} else {
			query, ok2 = job.Assets[pathOnAsset]
		}
	}
	// compile assets exclusive only for bq2bq and mc2mc plugin with replace load method and contains query.sql in asset
	if ok1 && ok2 && method == "REPLACE" && (job.Task.Name == bq2bq || (job.Task.Name == mc2mc && disableMultiQuery != "true")) {
		// check if task needs to override the compilation behaviour
		compiledQuery, err := c.CompileQuery(interval.Start(), interval.End(), query, systemEnvVars)
		if err != nil {
			c.logger.Error("error compiling assets: %s", err.Error())
			return nil, err
		}
		inputFiles["query.sql"] = compiledQuery
	}

	fileMap, err := c.compiler.Compile(inputFiles, contextForTask)
	if err != nil {
		c.logger.Error("error compiling assets: %s", err)
		return nil, err
	}
	return fileMap, nil
}

func (c *JobRunAssetsCompiler) CompileQuery(startTime, endTime time.Time, query string, envs map[string]string) (string, error) {
	// partition window in range
	instanceEnvMap := map[string]interface{}{}
	for name, value := range envs {
		instanceEnvMap[name] = value
	}

	// TODO: making few assumptions here, should be documented
	// assume destination table is time partitioned
	// assume table is partitioned as DAY
	const dayHours = 24
	partitionDelta := time.Hour * time.Duration(dayHours)

	// find destination partitions
	var destinationsPartitions []struct {
		start time.Time
		end   time.Time
	}
	for currentPart := startTime; currentPart.Before(endTime); currentPart = currentPart.Add(partitionDelta) {
		destinationsPartitions = append(destinationsPartitions, struct {
			start time.Time
			end   time.Time
		}{
			start: currentPart,
			end:   currentPart.Add(partitionDelta),
		})
	}

	// check if window size is greater than partition delta(a DAY), if not do nothing
	if endTime.Sub(startTime) <= partitionDelta {
		return query, nil
	}

	// TODO: investigate from this part to the end is never called anywhere
	var parsedQueries []string
	queryMap := map[string]string{"query": query}
	for _, part := range destinationsPartitions {
		instanceEnvMap["DSTART"] = part.start.Format(time.RFC3339)
		instanceEnvMap["DEND"] = part.end.Format(time.RFC3339)
		compiledQueryMap, err := c.compiler.Compile(queryMap, instanceEnvMap)
		if err != nil {
			return "", err
		}
		parsedQueries = append(parsedQueries, compiledQueryMap["query"])
	}

	queryFileReplaceBreakMarker := "\n--*--optimus-break-marker--*--\n"
	return strings.Join(parsedQueries, queryFileReplaceBreakMarker), nil
}
