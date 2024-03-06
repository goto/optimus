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

	"github.com/go-resty/resty/v2"
	"github.com/mitchellh/mapstructure"

	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/tenant"
)

// ResourceManager is repository for external job spec
type ResourceManager interface {
	GetOptimusUpstreams(ctx context.Context, unresolvedDependency *job.Upstream) ([]*job.Upstream, error)
}

type OptimusResourceManager struct {
	name   string
	config config.ResourceManagerConfigOptimus

	httpClient *resty.Client
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

func newHTTPClient(host string) (*resty.Client, error) {
	client := resty.New()

	if strings.HasPrefix(host, "https") {
		certPool, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("error reading system certificate: %w", err)
		}

		transport := &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    certPool,
				MinVersion: tls.VersionTLS12,
			},
		}

		client.SetTransport(transport)
	}

	return client, nil
}

func (o *OptimusResourceManager) GetOptimusUpstreams(ctx context.Context, unresolvedDependency *job.Upstream) ([]*job.Upstream, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}

	request := o.constructRequest(ctx)
	url := o.constructURL(unresolvedDependency)

	response, err := request.Get(url)
	if err != nil {
		return nil, fmt.Errorf("error encountered when sending request: %w", err)
	}

	if response.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("unexpected status response [%s] with body: %s", response.Status(), response.String())
	}

	var jobSpecResponse getJobSpecificationsResponse
	if err := json.Unmarshal(response.Body(), &jobSpecResponse); err != nil {
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	return o.toOptimusDependencies(jobSpecResponse.JobSpecificationResponses, unresolvedDependency)
}

func (o *OptimusResourceManager) constructRequest(ctx context.Context) *resty.Request {
	request := o.httpClient.NewRequest().SetContext(ctx)

	request.SetHeader("Accept", "application/json")
	for key, value := range o.config.Headers {
		request.SetHeader(key, value)
	}

	return request
}

func (o *OptimusResourceManager) constructURL(unresolvedDependency *job.Upstream) string {
	var filters []string
	if unresolvedDependency.Name() != "" {
		filters = append(filters, fmt.Sprintf("job_name=%s", unresolvedDependency.Name().String()))
	}
	if unresolvedDependency.ProjectName() != "" {
		filters = append(filters, fmt.Sprintf("project_name=%s", unresolvedDependency.ProjectName().String()))
	}
	if unresolvedDependency.Resource() != "" {
		filters = append(filters, fmt.Sprintf("resource_destination=%s", unresolvedDependency.Resource().String()))
	}

	path := "/api/v1beta1/jobs"
	url := o.config.Host + path + "?" + strings.Join(filters, "&")
	return url
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
	resourceURN := job.ResourceURN(response.Job.Destination)
	return job.NewUpstreamResolved(jobName, o.config.Host, resourceURN, jobTenant, unresolvedDependency.Type(), taskName, true), nil
}
