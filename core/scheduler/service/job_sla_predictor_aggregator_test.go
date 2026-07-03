package service // nolint: testpackage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
)

func TestTeamBreachAggregator(t *testing.T) {
	t.Run("consolidates per team -> project -> group -> target and dedupes causes", func(t *testing.T) {
		agg := newTeamBreachAggregator()

		// team-a: p1/g1(CRITICAL)/t1 with cause c1 (added twice -> deduped) and c2
		agg.add("team-a", "p1", "g1", "CRITICAL", "t1", scheduler.UpstreamAttrs{JobName: "c1", RelativeLevel: 2, Status: "NOT_STARTED"})
		agg.add("team-a", "p1", "g1", "CRITICAL", "t1", scheduler.UpstreamAttrs{JobName: "c1", RelativeLevel: 2, Status: "NOT_STARTED"})
		agg.add("team-a", "p1", "g1", "CRITICAL", "t1", scheduler.UpstreamAttrs{JobName: "c2", RelativeLevel: 1, Status: "RUNNING_LATE"})
		// team-a: second group in same project
		agg.add("team-a", "p1", "g2", "WARNING", "t2", scheduler.UpstreamAttrs{JobName: "c3"})
		// team-a: second project
		agg.add("team-a", "p2", "g1", "CRITICAL", "t3", scheduler.UpstreamAttrs{JobName: "c4"})
		// team-b: separate team
		agg.add("team-b", "p1", "g1", "CRITICAL", "t1", scheduler.UpstreamAttrs{JobName: "c5"})

		out := agg.build()
		assert.Len(t, out, 2)

		// insertion order is preserved: team-a first
		teamA := out[0]
		assert.Equal(t, "team-a", teamA.TeamName)
		assert.Len(t, teamA.Projects, 2)

		p1 := teamA.Projects[0]
		assert.Equal(t, "p1", p1.Name)
		assert.Len(t, p1.Groups, 2)

		g1 := p1.Groups[0]
		assert.Equal(t, "g1", g1.Name)
		assert.Equal(t, "CRITICAL", g1.Severity)
		assert.Len(t, g1.Targets, 1)
		assert.Equal(t, "t1", g1.Targets[0].JobName)
		assert.Len(t, g1.Targets[0].Causes, 2) // c1 deduped, c1 + c2

		g2 := p1.Groups[1]
		assert.Equal(t, "g2", g2.Name)
		assert.Equal(t, "WARNING", g2.Severity)

		assert.Equal(t, "p2", teamA.Projects[1].Name)
		assert.Equal(t, "team-b", out[1].TeamName)
	})
}

func TestDeriveGroupName(t *testing.T) {
	assert.Equal(t, "default", deriveGroupName(nil))
	assert.Equal(t, "default", deriveGroupName(map[string]string{}))
	// keys are sorted for stability
	assert.Equal(t, "a=1, b=2", deriveGroupName(map[string]string{"b": "2", "a": "1"}))
}
