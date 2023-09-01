package compiler

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"text/template"
	"time"

	"github.com/goto/optimus/internal/errors"
)

const (
	EntityCompiler = "compiler"

	// ISODateFormat https://en.wikipedia.org/wiki/ISO_8601
	ISODateFormat = "2006-01-02"

	ISOTimeFormat = time.RFC3339

	QueryFileName = "query.sql"

	LoadMethod        = "LOAD_METHOD"
	LoadMethodReplace = "REPLACE"

	QueryFileReplaceBreakMarker = "\n--*--optimus-break-marker--*--\n"
)

// Engine compiles a set of defined macros using the provided context
type Engine struct {
	baseTemplate *template.Template
}

func NewEngine() *Engine {
	baseTemplate := template.
		New("optimus_template_engine").
		Funcs(OptimusFuncMap())

	return &Engine{
		baseTemplate: baseTemplate,
	}
}

func (e *Engine) Compile(templateMap map[string]string, context map[string]any) (map[string]string, error) {
	rendered := map[string]string{}

	for name, content := range templateMap {
		tmpl, err := e.baseTemplate.New(name).Parse(content)
		if err != nil {
			msg := fmt.Sprintf("unable to parse content for %s: %s", name, err.Error())
			return nil, errors.InvalidArgument(EntityCompiler, msg)
		}

		var buf bytes.Buffer
		err = tmpl.Execute(&buf, context)
		if err != nil {
			msg := fmt.Sprintf("unable to render content for %s: %s", name, err.Error())
			return nil, errors.InvalidArgument(EntityCompiler, msg)
		}
		rendered[name] = strings.TrimSpace(buf.String())
	}
	return rendered, nil
}

func (e *Engine) CompileString(input string, context map[string]any) (string, error) {
	tmpl, err := e.baseTemplate.New("base").Parse(input)
	if err != nil {
		return "", errors.InvalidArgument(EntityCompiler, "unable to parse string "+input)
	}
	var buf bytes.Buffer
	if err = tmpl.Execute(&buf, context); err != nil {
		return "", errors.InvalidArgument(EntityCompiler, "unable to render string "+input)
	}
	return strings.TrimSpace(buf.String()), nil
}

func (e *Engine) CompileQuery(ctx context.Context, startTime, endTime time.Time, query string, systemEnvVars map[string]string) (string, error) {
	// partition window in range
	instanceEnvMap := map[string]interface{}{}
	for name, value := range systemEnvVars {
		instanceEnvMap[name] = value
	}

	// TODO: making few assumptions here, should be documented
	// assume destination table is time partitioned
	// assume table is partitioned as DAY
	const dayHours = time.Duration(24)
	partitionDelta := time.Hour * dayHours

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
		return "", nil
	}

	var parsedQueries []string
	queryMap := map[string]string{"query": query}
	for _, part := range destinationsPartitions {
		instanceEnvMap["DSTART"] = part.start.Format(time.RFC3339)
		instanceEnvMap["DEND"] = part.end.Format(time.RFC3339)
		compiledQueryMap, err := e.Compile(queryMap, instanceEnvMap)
		if err != nil {
			return "", err
		}
		parsedQueries = append(parsedQueries, compiledQueryMap["query"])
	}

	return strings.Join(parsedQueries, QueryFileReplaceBreakMarker), nil
}
