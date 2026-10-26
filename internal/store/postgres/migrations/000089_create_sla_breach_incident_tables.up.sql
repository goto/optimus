-- Root-cause SLA breach alerting use Publisher-Subscriber model

-- sla_breach_root_cause is the publisher state: one row per offending entity (project, job, scheduled run).
CREATE TABLE IF NOT EXISTS sla_breach_root_cause (
    id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    project_name   VARCHAR(100) NOT NULL,
    job_name       TEXT NOT NULL,
    scheduled_at   TIMESTAMPTZ NOT NULL,

    -- full set of reasons for this incident, e.g.
    -- {"reasons": [{"reason": "RUNTIME_DELAY", "status": "active", "induced_delay_seconds": 2400}]}
    details        JSONB NOT NULL DEFAULT '{"reasons": []}'::jsonb,

    max_induced_delay_seconds INT NOT NULL DEFAULT 0,

    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT sla_breach_root_cause_entity_unique UNIQUE (project_name, job_name, scheduled_at)
);

-- the subscriber treats incidents not refreshed within its staleness window as resolved
CREATE INDEX IF NOT EXISTS sla_breach_root_cause_updated_at_idx
    ON sla_breach_root_cause (updated_at DESC);

CREATE TABLE IF NOT EXISTS sla_breach_impact (
    root_cause_id    UUID NOT NULL REFERENCES sla_breach_root_cause (id) ON DELETE CASCADE,

    impacted_project VARCHAR(100) NOT NULL,
    impacted_job     TEXT NOT NULL,
    scheduled_at     TIMESTAMPTZ NOT NULL,

    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT sla_breach_impact_unique UNIQUE (root_cause_id, impacted_project, impacted_job, scheduled_at)
);

CREATE INDEX IF NOT EXISTS sla_breach_impact_impacted_idx
    ON sla_breach_impact (impacted_project, impacted_job, scheduled_at);