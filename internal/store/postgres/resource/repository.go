package resource

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

const (
	columnsToStore  = `full_name, kind, store, status, urn, project_name, namespace_name, metadata, spec, created_at, updated_at`
	resourceColumns = `id, ` + columnsToStore
)

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{
		db: pool,
	}
}

func (r Repository) Create(ctx context.Context, resourceModel *resource.Resource) error {
	res := FromResourceToModel(resourceModel)

	insertResource := `INSERT INTO resource (` + columnsToStore + `) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now(), now())`
	_, err := r.db.Exec(ctx, insertResource, res.FullName, res.Kind, res.Store, res.Status, res.URN,
		res.ProjectName, res.NamespaceName, res.Metadata, res.Spec)
	return errors.WrapIfErr(tenant.EntityNamespace, "error creating resource to database", err)
}

func (r Repository) Update(ctx context.Context, resourceModel *resource.Resource) error {
	res := FromResourceToModel(resourceModel)

	updateResource := `UPDATE resource SET kind=$1, status=$2, urn=$3, metadata=$4, spec=$5, updated_at=now() 
                WHERE full_name=$6 AND store=$7 AND project_name = $8 And namespace_name = $9`
	tag, err := r.db.Exec(ctx, updateResource, res.Kind, res.Status, res.URN,
		res.Metadata, res.Spec, res.FullName, res.Store, res.ProjectName, res.NamespaceName)
	if err != nil {
		return errors.Wrap(resource.EntityResource, "error updating resource to database", err)
	}

	if tag.RowsAffected() == 0 {
		return errors.NotFound(resource.EntityResource, "no resource to update for "+res.FullName)
	}
	return nil
}

func (r Repository) Delete(ctx context.Context, resourceModel *resource.Resource) error {
	res := FromResourceToModel(resourceModel)

	deleteResourceQuery := `UPDATE resource SET status=$5, updated_at=NOW()
                WHERE full_name=$1 AND store=$2 AND project_name = $3 And namespace_name = $4`
	tag, err := r.db.Exec(ctx, deleteResourceQuery, res.FullName, res.Store, res.ProjectName, res.NamespaceName, resource.StatusDeleted)
	if err != nil {
		return errors.Wrap(resource.EntityResource, "error updating resource to database", err)
	}

	if tag.RowsAffected() == 0 {
		return errors.NotFound(resource.EntityResource, "no resource to delete for "+res.FullName)
	}
	return nil
}

func (r Repository) ChangeNamespace(ctx context.Context, res *resource.Resource, newTenant tenant.Tenant) error {
	resourceInDestinationNamespace, err := r.ReadByFullName(ctx, newTenant, res.Store(), res.FullName(), false)
	if err != nil && !errors.IsErrorType(err, errors.ErrNotFound) {
		return err
	}

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return errors.Wrap(resource.EntityResource, "error begin db transaction", err)
	}

	if err = r.hardDelete(ctx, tx, resourceInDestinationNamespace); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}

	updateResource := `UPDATE resource SET namespace_name=$1, updated_at=now()
	WHERE full_name=$2 AND store=$3 AND project_name = $4 And namespace_name = $5`
	tag, err := tx.Exec(ctx, updateResource,
		newTenant.NamespaceName(), res.FullName(), res.Store(),
		res.Tenant().ProjectName(), res.Tenant().NamespaceName())
	if err != nil {
		_ = tx.Rollback(ctx)
		return errors.Wrap(resource.EntityResource, "error changing tenant for resource:"+res.FullName(), err)
	}
	if tag.RowsAffected() == 0 {
		_ = tx.Rollback(ctx)
		return errors.NotFound(resource.EntityResource, "no resource to changing tenant for ")
	}

	err = tx.Commit(ctx)
	return errors.WrapIfErr(resource.EntityResource, "error commit db transaction", err)
}

func (Repository) hardDelete(ctx context.Context, tx pgx.Tx, res *resource.Resource) error {
	if res == nil || !res.IsDeleted() {
		return nil
	}

	deleteResourceQuery := `DELETE FROM resource 
       WHERE project_name = $1 AND namespace_name = $2 AND store = $3 AND full_name = $4 AND status = $5`
	args := []any{res.Tenant().ProjectName(), res.Tenant().NamespaceName(), res.Store(), res.Name(), resource.StatusDeleted}
	_, err := tx.Exec(ctx, deleteResourceQuery, args...)
	return errors.WrapIfErr(resource.EntityResource, "failed do hard delete", err)
}

func (r Repository) ReadByFullName(ctx context.Context, tnnt tenant.Tenant, store resource.Store, fullName string, onlyActive bool) (*resource.Resource, error) {
	var res Resource
	getResource := `SELECT ` + resourceColumns + ` FROM resource WHERE full_name = $1 AND store = $2 AND
	project_name = $3 AND namespace_name = $4`
	args := []any{fullName, store, tnnt.ProjectName(), tnnt.NamespaceName()}

	if onlyActive {
		getResource += ` AND status NOT IN ($5, $6)`
		args = append(args, resource.StatusDeleted, resource.StatusToDelete)
	}

	err := r.db.QueryRow(ctx, getResource, args...).
		Scan(&res.ID, &res.FullName, &res.Kind, &res.Store, &res.Status, &res.URN,
			&res.ProjectName, &res.NamespaceName, &res.Metadata, &res.Spec, &res.CreatedAt, &res.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NotFound(resource.EntityResource, fmt.Sprintf("no resource: '%s' found for project:%s, namespace:%s ", fullName, tnnt.ProjectName(), tnnt.NamespaceName()))
		}

		return nil, errors.Wrap(resource.EntityResource, fmt.Sprintf("error reading resource: '%s' found for project:%s, namespace:%s ", fullName, tnnt.ProjectName(), tnnt.NamespaceName()), err)
	}

	return FromModelToResource(&res)
}

func (r Repository) ReadAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store, onlyActive bool) ([]*resource.Resource, error) {
	getAllResources := `SELECT ` + resourceColumns + ` FROM resource WHERE project_name = $1 and namespace_name = $2 and store = $3`
	args := []any{tnnt.ProjectName(), tnnt.NamespaceName(), store}

	if onlyActive {
		getAllResources += ` AND status NOT IN ($4, $5)`
		args = append(args, resource.StatusDeleted, resource.StatusToDelete)
	}

	rows, err := r.db.Query(ctx, getAllResources, args...)
	if err != nil {
		return nil, errors.Wrap(resource.EntityResource, "error in ReadAll", err)
	}
	defer rows.Close()

	var resources []*resource.Resource
	for rows.Next() {
		var res Resource
		err = rows.Scan(&res.ID, &res.FullName, &res.Kind, &res.Store, &res.Status, &res.URN,
			&res.ProjectName, &res.NamespaceName, &res.Metadata, &res.Spec, &res.CreatedAt, &res.UpdatedAt)
		if err != nil {
			return nil, errors.Wrap(resource.EntityResource, "error in GetAll", err)
		}

		resourceModel, err := FromModelToResource(&res)
		if err != nil {
			return nil, err
		}
		resources = append(resources, resourceModel)
	}

	return resources, nil
}

func (r Repository) GetResources(ctx context.Context, tnnt tenant.Tenant, store resource.Store, names []string) ([]*resource.Resource, error) {
	getAllResources := `SELECT ` + resourceColumns + ` FROM resource WHERE project_name = $1 and namespace_name = $2 and 
store = $3 AND full_name = any ($4) AND status NOT IN ($5, $6)`
	rows, err := r.db.Query(ctx, getAllResources, tnnt.ProjectName(), tnnt.NamespaceName(), store, names, resource.StatusDeleted, resource.StatusToDelete)
	if err != nil {
		return nil, errors.Wrap(resource.EntityResource, "error in ReadAll", err)
	}
	defer rows.Close()

	var resources []*resource.Resource
	for rows.Next() {
		var res Resource
		err = rows.Scan(&res.ID, &res.FullName, &res.Kind, &res.Store, &res.Status, &res.URN,
			&res.ProjectName, &res.NamespaceName, &res.Metadata, &res.Spec, &res.CreatedAt, &res.UpdatedAt)
		if err != nil {
			return nil, errors.Wrap(resource.EntityResource, "error in GetAll", err)
		}

		resourceModel, err := FromModelToResource(&res)
		if err != nil {
			return nil, err
		}
		resources = append(resources, resourceModel)
	}

	return resources, nil
}

func (r Repository) UpdateStatus(ctx context.Context, resources ...*resource.Resource) error {
	batch := pgx.Batch{}
	for _, res := range resources {
		updateStatus := `UPDATE resource SET status = $1 WHERE project_name = $2 AND namespace_name = $3 AND store = $4 AND full_name = $5`
		batch.Queue(updateStatus, res.Status(), res.Tenant().ProjectName(), res.Tenant().NamespaceName(), res.Store(), res.FullName())
	}

	results := r.db.SendBatch(ctx, &batch)
	defer results.Close()

	multiErr := errors.NewMultiError("error updating resources status")
	for i := range resources {
		tag, err := results.Exec()
		multiErr.Append(err)
		if tag.RowsAffected() == 0 {
			multiErr.Append(errors.InternalError(resource.EntityResource, "error updating status for "+resources[i].FullName(), nil))
		}
	}

	return multiErr.ToErr()
}

func (r Repository) GetChangelogs(ctx context.Context, projectName tenant.ProjectName, resourceName resource.Name) ([]*resource.ChangeLog, error) {
	getChangeLogQuery := `
		SELECT
			changes, change_type, created_at
		FROM
			changelog
		WHERE
			project_name = $1 AND name = $2 AND entity_type = $3
		ORDER BY
			created_at DESC;`

	rows, err := r.db.Query(ctx, getChangeLogQuery, projectName, resourceName, "resource")
	if err != nil {
		return nil, errors.Wrap(resource.EntityResource, fmt.Sprintf("error while fetching changeLog for resource [%s/%s]", projectName.String(), resourceName.String()), err)
	}
	defer rows.Close()

	me := errors.NewMultiError("get change log errors")
	var changeLog []*resource.ChangeLog
	for rows.Next() {
		log, err := FromChangelogRow(rows)
		if err != nil {
			me.Append(err)
			continue
		}
		changeLog = append(changeLog, fromStorageChangelog(log))
	}

	return changeLog, me.ToErr()
}
