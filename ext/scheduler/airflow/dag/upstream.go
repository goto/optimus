package dag

import (
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

type Upstreams struct {
	HTTP       []*scheduler.HTTPUpstreams
	Upstreams  []Upstream
	ThirdParty []ThirdPartyUpstream
}

func (u Upstreams) Empty() bool {
	if len(u.HTTP) == 0 && len(u.Upstreams) == 0 && len(u.ThirdParty) == 0 {
		return true
	}
	return false
}

type Upstream struct {
	JobName  string
	Tenant   tenant.Tenant
	Host     string
	TaskName string
}

type ThirdPartyUpstream struct {
	Type string
	URN  string
}

func SetupUpstreams(upstreams scheduler.Upstreams, host string) Upstreams {
	var ups []Upstream
	var thirdUps []ThirdPartyUpstream
	for _, u := range upstreams.UpstreamJobs {
		if u.ThirdPartyType != "" {
			thirdUps = append(thirdUps, ThirdPartyUpstream{
				Type: u.ThirdPartyType,
				URN:  u.DestinationURN.GetName(),
			})
			continue
		}
		var upstreamHost string
		if !u.External {
			upstreamHost = host
		} else {
			upstreamHost = u.Host
		}
		upstream := Upstream{
			JobName:  u.JobName,
			Tenant:   u.Tenant,
			Host:     upstreamHost,
			TaskName: u.TaskName,
		}
		ups = append(ups, upstream)
	}
	return Upstreams{
		HTTP:       upstreams.HTTP,
		Upstreams:  ups,
		ThirdParty: thirdUps,
	}
}
