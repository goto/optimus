package tenant

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type LocationRepository struct {
	db *pgxpool.Pool
}

const (
	locationColumns = `id, project_name, name, project, dataset, created_at, updated_at`
)

func NewLocationRepository(db *pgxpool.Pool) *LocationRepository {
	return &LocationRepository{
		db: db,
	}
}

type Location struct {
	ID uuid.UUID

	ProjectName string
	Name        string
	Dataset     string
	Project     string

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (r *LocationRepository) Create(ctx context.Context, projectName tenant.ProjectName, location tenant.Location) error {
	insertStatement := `
	INSERT INTO location
		(project_name, name, dataset, project, created_at, updated_at)
	VALUES
		($1, $2, $3, $4, NOW(), NOW())
	`

	_, err := r.db.Exec(ctx, insertStatement, projectName, location.Name(), location.Dataset(), location.Project())
	if err != nil {
		return errors.WrapIfErr(tenant.EntityProject, "error inserting location", err)
	}

	return nil
}

func (r *LocationRepository) Get(ctx context.Context, projectName tenant.ProjectName) ([]tenant.Location, error) {
	query := fmt.Sprintf(`
		SELECT
			%s
		FROM
			location
		WHERE
			project_name = $1
	`, locationColumns)

	rows, err := r.db.Query(ctx, query, projectName)
	if err != nil {
		return nil, errors.Wrap(tenant.EntityProject, "error reading project locations", err)
	}
	defer rows.Close()

	locations, err := r.scanRowsToLocations(rows)
	if err != nil {
		return nil, errors.Wrap(tenant.EntityProject, "error scanning location rows", err)
	}

	return locations, nil
}

func (r *LocationRepository) Update(ctx context.Context, projectName tenant.ProjectName, location tenant.Location) error {
	query := `
		UPDATE
			location	
		SET
			project = $1,
			dataset = $2,
			updated_at = NOW()
		WHERE
			project_name = $3
			AND name = $4
	`

	result, err := r.db.Exec(ctx, query, location.Project(), location.Dataset(), projectName, location.Name())
	if err != nil {
		return errors.Wrap(tenant.EntityProject, "error updating location record", err)
	}

	if result.RowsAffected() == 0 {
		return errors.NotFound(tenant.EntityProject, "no location row is updated")
	}

	return nil
}

func (r *LocationRepository) Delete(ctx context.Context, projectName tenant.ProjectName, locationName string) error {
	query := `
		DELETE FROM
			location
		WHERE
			project_name = $1 and name = $2
	`

	_, err := r.db.Exec(ctx, query, projectName, locationName)
	return errors.WrapIfErr(tenant.EntityProject, "error deleting location", err)
}

func (*LocationRepository) scanRowsToLocations(rows pgx.Rows) ([]tenant.Location, error) {
	locations := []tenant.Location{}
	for rows.Next() {
		var loc Location
		err := rows.Scan(
			&loc.ID,
			&loc.ProjectName,
			&loc.Name,
			&loc.Project,
			&loc.Dataset,
			&loc.CreatedAt,
			&loc.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}

		location := tenant.NewLocation(loc.Name, loc.Project, loc.Dataset)
		locations = append(locations, location)
	}

	return locations, nil
}
