package alertmanager

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
)

func TestBuildPotentialSLABreachPayload(t *testing.T) {
	am := &AlertManager{endpoint: "http://alertmanager"}

	t.Run("routing severity is the max across groups and environment is set for critical", func(t *testing.T) {
		payload := am.buildPotentialSLABreachPayload(&scheduler.PotentialSLABreachAttrs{
			TeamName: "team-a",
			Projects: []scheduler.SLABreachProject{
				{
					Name: "proj-1",
					Groups: []scheduler.SLABreachGroup{
						{Name: "sla-8am", Severity: CriticalSeverity, Targets: []scheduler.SLABreachTarget{
							{JobName: "t1", Causes: []scheduler.UpstreamAttrs{{JobName: "c1", RelativeLevel: 2, Status: "NOT_STARTED"}}},
						}},
						{Name: "sla-8-30am", Severity: WarningSeverity, Targets: []scheduler.SLABreachTarget{
							{JobName: "t2", Causes: []scheduler.UpstreamAttrs{{JobName: "c2", RelativeLevel: 1, Status: "RUNNING_LATE"}}},
						}},
					},
				},
			},
		})

		assert.Equal(t, OptimusPotentialSLABreachTemplate, payload.Template)
		assert.Equal(t, "team-a", payload.Labels[DefaultChannelLabel])
		assert.Equal(t, CriticalSeverity, payload.Labels[SeverityLabel])
		assert.Equal(t, "production", payload.Labels[EnvironmentLabel])
		assert.Equal(t, "team-a", payload.Data["team"])
		assert.Equal(t, "proj-1", payload.Project)

		projects, ok := payload.Data["projects"].([]map[string]interface{})
		assert.True(t, ok)
		assert.Len(t, projects, 1)
		groups, ok := projects[0]["groups"].([]map[string]interface{})
		assert.True(t, ok)
		assert.Len(t, groups, 2)
		assert.Equal(t, CriticalSeverity, groups[0]["severity"])
		assert.Equal(t, WarningSeverity, groups[1]["severity"])
	})

	t.Run("defaults severity to warning when no groups have severity", func(t *testing.T) {
		payload := am.buildPotentialSLABreachPayload(&scheduler.PotentialSLABreachAttrs{
			TeamName: "team-b",
			Projects: []scheduler.SLABreachProject{
				{Name: "proj-1", Groups: []scheduler.SLABreachGroup{
					{Name: "g1", Severity: "", Targets: []scheduler.SLABreachTarget{{JobName: "t1"}}},
				}},
			},
		})
		assert.Equal(t, DefaultSeverity, payload.Labels[SeverityLabel])
		_, hasEnv := payload.Labels[EnvironmentLabel]
		assert.False(t, hasEnv)
	})
}
