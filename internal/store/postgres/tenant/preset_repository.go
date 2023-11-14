package tenant

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

type PresetRepository struct {
	db *pgxpool.Pool
}

const (
	presetColumns           = `id, project_name, name, description, window_truncate_to, window_delay, window_size, window_location, created_at, updated_at`
	getPresetsByProjectName = `select ` + presetColumns + ` from preset where project_name = $1`
)

func NewPresetRepository(db *pgxpool.Pool) *PresetRepository {
	return &PresetRepository{
		db: db,
	}
}

type Preset struct {
	ID uuid.UUID

	ProjectName string
	Name        string
	Description string

	TruncateTo string
	Delay      string
	Size       string
	Location   string

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (p PresetRepository) Create(ctx context.Context, projectName tenant.ProjectName, preset tenant.Preset) error {
	insertStatement := `INSERT INTO preset
	(project_name, name, description, window_truncate_to, window_offset, window_size, created_at, updated_at)
VALUES
	($1, $2, $3, $4, $5, $6, NOW(), NOW())
`

	_, err := p.db.Exec(ctx, insertStatement, projectName,
		preset.Name(), preset.Description(),
		preset.Config().TruncateTo, preset.Config().Delay, preset.Config().Size, preset.Config().Location,
	)

	return errors.WrapIfErr(tenant.EntityProject, "error inserting preset", err)
}

func (p PresetRepository) Read(ctx context.Context, projectName tenant.ProjectName) ([]tenant.Preset, error) {
	rows, err := p.db.Query(ctx, getPresetsByProjectName, projectName)
	if err != nil {
		return nil, errors.Wrap(tenant.EntityProject, "error reading presets under project name", err)
	}
	defer rows.Close()

	existings, err := p.scanRows(rows)
	if err != nil {
		return nil, err
	}

	output := make([]tenant.Preset, len(existings))
	for i, existing := range existings {
		preset, err := tenant.NewPreset(existing.Name, existing.Description, existing.Size, existing.Delay, existing.Location, existing.TruncateTo)
		if err != nil {
			return nil, err
		}

		output[i] = preset
	}

	return output, nil
}

func (p PresetRepository) Update(ctx context.Context, projectName tenant.ProjectName, preset tenant.Preset) error {
	updateStatement := `UPDATE preset
SET
	description = $1,
	window_truncate_to = $2,
	window_delay = $3,
	window_size = $4,
	window_location = $5,
	updated_at = NOW()
WHERE
	project_name = $6
	AND name = $7
`

	result, err := p.db.Exec(ctx, updateStatement,
		preset.Description(), preset.Config().TruncateTo, preset.Config().Delay, preset.Config().Size, preset.Config().Location,
		projectName, preset.Name(),
	)
	if err != nil {
		return errors.Wrap(tenant.EntityProject, "error updating record", err)
	}

	if result.RowsAffected() == 0 {
		return errors.NotFound(tenant.EntityProject, "no row is updated")
	}

	return nil
}

func (p PresetRepository) Delete(ctx context.Context, projectName tenant.ProjectName, presetName string) error {
	deleteStatement := `DELETE FROM preset where project_name = $1 and name = $2`
	_, err := p.db.Exec(ctx, deleteStatement, projectName, presetName)
	return errors.WrapIfErr(tenant.EntityProject, "error deleting preset", err)
}

func (PresetRepository) scanRows(rows pgx.Rows) ([]*Preset, error) {
	var presets []*Preset
	for rows.Next() {
		var preset Preset
		err := rows.Scan(
			&preset.ID,
			&preset.ProjectName,
			&preset.Name,
			&preset.Description,
			&preset.TruncateTo,
			&preset.Delay,
			&preset.Size,
			&preset.Location,
			&preset.CreatedAt,
			&preset.UpdatedAt,
		)
		if err != nil {
			return nil, errors.Wrap(tenant.EntityProject, "error scanning rows", err)
		}

		presets = append(presets, &preset)
	}

	return presets, nil
}
