package resourcemanager

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

type OptimusResourceManager struct {
	name   string
	config config.ResourceManagerConfigOptimus

	httpClient *http.Client
}

// NewOptimusResourceManager initializes job spec repository for Optimus neighbor
func NewOptimusResourceManager(resourceManagerConfig config.ResourceManager) (*OptimusResourceManager, error) {
	var conf config.ResourceManagerConfigOptimus
	if err := mapstructure.Decode(resourceManagerConfig.Config, &conf); err != nil {
		return nil, fmt.Errorf("error decoding resource manger config: %w", err)
	}

	if conf.Host == "" {
		return nil, errors.New("optimus resource manager host is empty")
	}

	httpClient, err := newHTTPClient(conf.Host)
	if err != nil {
		return nil, fmt.Errorf("error initializing http client: %w", err)
	}

	return &OptimusResourceManager{
		name:       resourceManagerConfig.Name,
		config:     conf,
		httpClient: httpClient,
	}, nil
}

func newHTTPClient(host string) (*http.Client, error) {
	httpClient := new(http.Client)

	if strings.HasPrefix(host, "https") {
		certPool, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("error reading system certificate: %w", err)
		}

		httpClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    certPool,
				MinVersion: tls.VersionTLS12,
			},
		}
	}

	return httpClient, nil
}

func (o *OptimusResourceManager) GetHostURL() string {
	return o.config.Host
}

func (o *OptimusResourceManager) GetJobRuns(ctx context.Context, sensorParameters scheduler.JobSensorParameters, criteria *scheduler.JobRunsCriteria) ([]*scheduler.JobRunStatus, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}

	request, err := o.constructGetJobRunsRequest(ctx, sensorParameters, criteria)
	if err != nil {
		return nil, fmt.Errorf("error encountered when constructing get job runs request for %#v, err: %w", sensorParameters, err)
	}

	response, err := o.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("error encountered when sending request: %s, err: %w", request.URL, err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status response: %s", response.Status)
	}

	var jobRunResponse getJobRunsResponse
	decoder := json.NewDecoder(response.Body)
	if err := decoder.Decode(&jobRunResponse); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}
	if len(jobRunResponse) == 0 {
		return nil, errors.New("jobRun not found")
	}
	return toOptimusRuns(jobRunResponse)
}

func (o *OptimusResourceManager) GetJobScheduleInterval(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName) (string, error) {
	if ctx == nil {
		return "", errors.New("context is nil")
	}

	request, err := o.constructGetJobSpecificationsRequest(ctx, tnnt, jobName)
	if err != nil {
		return "", fmt.Errorf("error encountered when constructing get job specifications request for project: %s, job_name: %s, err: %w", tnnt.ProjectName(), jobName, err)
	}

	response, err := o.httpClient.Do(request)
	if err != nil {
		return "", fmt.Errorf("error encountered when sending request: %s, err: %w", request.URL, err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status response: %s", response.Status)
	}

	var jobSpecResponse getJobSpecificationResponse
	decoder := json.NewDecoder(response.Body)
	if err := decoder.Decode(&jobSpecResponse); err != nil {
		return "", fmt.Errorf("error decoding response: %w", err)
	}

	return jobSpecResponse.Spec.Interval, nil
}

func toOptimusRuns(responses getJobRunsResponse) ([]*scheduler.JobRunStatus, error) {
	jobRunStatus := make([]*scheduler.JobRunStatus, len(responses))
	for i, run := range responses {
		scheduledAt, err := time.Parse(run.ScheduledAt, time.RFC3339)
		if err != nil {
			return nil, err
		}
		jobRunStatus[i].ScheduledAt = scheduledAt
		state, err := scheduler.StateFromString(run.State)
		if err != nil {
			return nil, err
		}
		jobRunStatus[i].State = state
	}
	return jobRunStatus, nil
}

func (o *OptimusResourceManager) GetOptimusUpstreams(ctx context.Context, unresolvedDependency *job.Upstream) ([]*job.Upstream, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	request, err := o.constructGetJobSpecificationsRequestFromJobUpstream(ctx, unresolvedDependency)
	if err != nil {
		return nil, fmt.Errorf("error encountered when constructing request: %w", err)
	}

	response, err := o.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("error encountered when sending request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status response: %s", response.Status)
	}

	var jobSpecResponse getJobSpecificationsResponse
	decoder := json.NewDecoder(response.Body)
	if err := decoder.Decode(&jobSpecResponse); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return o.toOptimusDependencies(jobSpecResponse.JobSpecificationResponses, unresolvedDependency)
}

func (o *OptimusResourceManager) constructGetJobSpecificationsRequestFromJobUpstream(ctx context.Context, unresolvedDependency *job.Upstream) (*http.Request, error) {
	var filters []string
	if unresolvedDependency.Name() != "" {
		filters = append(filters, fmt.Sprintf("job_name=%s", unresolvedDependency.Name().String()))
	}
	if unresolvedDependency.ProjectName() != "" {
		filters = append(filters, fmt.Sprintf("project_name=%s", unresolvedDependency.ProjectName().String()))
	}
	if !unresolvedDependency.Resource().IsZero() {
		filters = append(filters, fmt.Sprintf("resource_destination=%s", unresolvedDependency.Resource().String()))
	}

	path := "/api/v1beta1/jobs"
	url := o.config.Host + path + "?" + strings.Join(filters, "&")

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, err
	}

	request.Header.Set("Accept", "application/json")
	for key, value := range o.config.Headers {
		request.Header.Set(key, value)
	}
	return request, nil
}

func (o *OptimusResourceManager) constructGetJobRunsRequest(ctx context.Context, sensorParameters scheduler.JobSensorParameters, criteria *scheduler.JobRunsCriteria) (*http.Request, error) {
	path := fmt.Sprintf("v1beta1/project/%s/job/%s/run", sensorParameters.UpstreamTenant.ProjectName(), sensorParameters.UpstreamJobName)
	filtersMap := map[string]string{
		"start_date":              criteria.StartDate.Format(time.RFC3339),
		"end_date":                criteria.EndDate.Format(time.RFC3339),
		"filter":                  strings.Join(criteria.Filter, ","),
		"downstream_project_name": sensorParameters.SubjectProjectName.String(),
		"downstream_job_name":     sensorParameters.SubjectJobName.String(),
	}
	var queryStrings []string
	for k, v := range filtersMap {
		queryStrings = append(queryStrings, fmt.Sprintf("%s=%s", k, v))
	}

	url := o.config.Host + path + "?" + strings.Join(queryStrings, "&")

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, err
	}

	request.Header.Set("Accept", "application/json")
	for key, value := range o.config.Headers {
		request.Header.Set(key, value)
	}
	return request, nil
}

func (o *OptimusResourceManager) constructGetJobSpecificationsRequest(ctx context.Context, tnnt tenant.Tenant, jobName scheduler.JobName) (*http.Request, error) {
	path := fmt.Sprintf("/api/v1beta1/project/%s/namespace/%s/job/%s", tnnt.ProjectName(), tnnt.NamespaceName(), jobName)
	url := o.config.Host + path

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, err
	}

	request.Header.Set("Accept", "application/json")
	for key, value := range o.config.Headers {
		request.Header.Set(key, value)
	}
	return request, nil
}

func (o *OptimusResourceManager) toOptimusDependencies(responses []*jobSpecificationResponse, unresolvedDependency *job.Upstream) ([]*job.Upstream, error) {
	output := make([]*job.Upstream, len(responses))
	for i, r := range responses {
		dependency, err := o.toOptimusDependency(r, unresolvedDependency)
		if err != nil {
			return nil, err
		}
		output[i] = dependency
	}
	return output, nil
}

func (o *OptimusResourceManager) toOptimusDependency(response *jobSpecificationResponse, unresolvedDependency *job.Upstream) (*job.Upstream, error) {
	jobTenant, err := tenant.NewTenant(response.ProjectName, response.NamespaceName)
	if err != nil {
		return nil, err
	}
	jobName, err := job.NameFrom(response.Job.Name)
	if err != nil {
		return nil, err
	}
	taskName, err := job.TaskNameFrom(response.Job.TaskName)
	if err != nil {
		return nil, err
	}

	var resourceURN resource.URN
	if response.Job.Destination != "" {
		urn, err := resource.ParseURN(response.Job.Destination)
		if err != nil {
			return nil, err
		}
		resourceURN = urn
	}

	return job.NewUpstreamResolved(jobName, o.config.Host, resourceURN, jobTenant, unresolvedDependency.Type(), taskName, true), nil
}
