package resource

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/utils"
)

const (
	columnsToStore  = `full_name, kind, store, status, urn, project_name, namespace_name, metadata, spec, deprecation, created_at, updated_at`
	resourceColumns = `id, ` + columnsToStore

	changelogColumnsToStore = `entity_type, name, project_name, change_type, changes, created_at`
	changelogColumnsToFetch = `changes, change_type, created_at`
)

var changelogMetrics = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "resource_repository_changelog_metrics_error",
	Help: "success or failure metrics",
}, []string{"project", "namespace", "name", "msg"})

var changelogIgnoredFields = map[string]struct{}{
	"ID":        {},
	"CreatedAt": {},
	"UpdatedAt": {},
}

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

	insertResource := `INSERT INTO resource (` + columnsToStore + `) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now(), now())`
	_, err := r.db.Exec(ctx, insertResource, res.FullName, res.Kind, res.Store, res.Status, res.URN,
		res.ProjectName, res.NamespaceName, res.Metadata, res.Spec, res.Deprecated)
	return errors.WrapIfErr(tenant.EntityNamespace, "error creating resource to database", err)
}

func (r Repository) Update(ctx context.Context, resourceModel *resource.Resource) error {
	// we should fetch include `deleted`, because it also used for re-create `deleted` state resource
	const excludeDeleted = false
	existing, err := r.ReadByFullName(ctx, resourceModel.Tenant(), resourceModel.Store(), resourceModel.FullName(), excludeDeleted)
	if err != nil {
		return err
	}

	if err := r.doUpdate(ctx, resourceModel); err != nil {
		return err
	}

	if err := r.computeAndPersistChangeLog(ctx, existing, resourceModel); err != nil {
		// do not return the error
		changelogMetrics.WithLabelValues(
			resourceModel.Tenant().ProjectName().String(),
			resourceModel.Tenant().NamespaceName().String(),
			resourceModel.Name().String(),
			err.Error(),
		).Inc()
	}

	return nil
}

func (r Repository) computeAndPersistChangeLog(ctx context.Context, existing, incoming *resource.Resource) error {
	existingModel := FromResourceToModel(existing)
	incomingModel := FromResourceToModel(incoming)

	changes, err := getResourceDiffs(existingModel, incomingModel)
	if err != nil {
		return err
	}

	if len(changes) == 0 {
		return nil
	}

	return r.insertChangelog(ctx, incoming.Tenant().ProjectName(), incoming.Name(), changes, resource.ChangelogChangeTypeUpdate)
}

func (r Repository) insertChangelog(ctx context.Context, projectName tenant.ProjectName, resourceName resource.Name, changes []Change, changeType string) error {
	changesEncoded, err := json.Marshal(changes)
	if err != nil {
		return err
	}

	insertChangeLogQuery := `INSERT INTO changelog (` + changelogColumnsToStore + `) VALUES ($1, $2, $3, $4, $5, NOW());`

	res, err := r.db.Exec(ctx, insertChangeLogQuery, resource.EntityResource, resourceName, projectName,
		changeType, string(changesEncoded))
	if err != nil {
		return errors.Wrap(resource.EntityResourceChangelog, "unable to insert resource changelog", err)
	}

	if res.RowsAffected() == 0 {
		return errors.InternalError(resource.EntityResourceChangelog, "unable to insert resource changelog: rows affected 0", nil)
	}

	return err
}

func getResourceDiffs(existing, incoming *Resource) ([]Change, error) {
	var changes []Change
	diff, err := utils.GetDiffs(*existing, *incoming, nil)
	if err != nil {
		return changes, err
	}

	for _, d := range diff {
		if _, ignored := changelogIgnoredFields[d.Field]; ignored {
			continue
		}

		changes = append(changes, Change{
			Property: d.Field,
			Diff:     d.Diff,
		})
	}

	return changes, nil
}

func (r Repository) doUpdate(ctx context.Context, resourceModel *resource.Resource) error {
	res := FromResourceToModel(resourceModel)

	updateResource := `UPDATE resource SET kind=$1, status=$2, urn=$3, metadata=$4, spec=$5, deprecation=$10, updated_at=now() 
                WHERE full_name=$6 AND store=$7 AND project_name = $8 And namespace_name = $9`
	tag, err := r.db.Exec(ctx, updateResource, res.Kind, res.Status, res.URN,
		res.Metadata, res.Spec, res.FullName, res.Store, res.ProjectName, res.NamespaceName, res.Deprecated)
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
			&res.ProjectName, &res.NamespaceName, &res.Metadata, &res.Spec, &res.Deprecated, &res.CreatedAt, &res.UpdatedAt)
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
			&res.ProjectName, &res.NamespaceName, &res.Metadata, &res.Spec, &res.Deprecated, &res.CreatedAt, &res.UpdatedAt)
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
			&res.ProjectName, &res.NamespaceName, &res.Metadata, &res.Spec, &res.Deprecated, &res.CreatedAt, &res.UpdatedAt)
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
		SELECT ` + changelogColumnsToFetch + ` FROM changelog
		WHERE project_name = $1 AND name = $2 AND entity_type = $3;`

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

func (r Repository) ReadByURN(ctx context.Context, tnnt tenant.Tenant, urn resource.URN) (*resource.Resource, error) {
	query := `
		SELECT ` + resourceColumns + ` FROM resource
		WHERE urn = $1 AND project_name = $2 AND namespace_name = $3 AND status NOT IN ($4, $5)
		LIMIT 1
	`
	args := []any{urn.String(), tnnt.ProjectName(), tnnt.NamespaceName(), resource.StatusDeleted, resource.StatusToDelete}

	var res Resource
	err := r.db.QueryRow(ctx, query, args...).
		Scan(&res.ID, &res.FullName, &res.Kind, &res.Store, &res.Status, &res.URN,
			&res.ProjectName, &res.NamespaceName, &res.Metadata, &res.Spec, &res.Deprecated, &res.CreatedAt, &res.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NotFound(resource.EntityResource, fmt.Sprintf("no resource with urn: '%s' found for project:%s, namespace:%s ", urn, tnnt.ProjectName(), tnnt.NamespaceName()))
		}

		return nil, errors.Wrap(resource.EntityResource, fmt.Sprintf("error reading resource with urn: '%s' found for project:%s, namespace:%s ", urn, tnnt.ProjectName(), tnnt.NamespaceName()), err)
	}

	return FromModelToResource(&res)
}

func (r Repository) GetResourcesByURNs(ctx context.Context, tnnt tenant.Tenant, urns []resource.URN) ([]*resource.Resource, error) {
	query := `
		SELECT ` + resourceColumns + ` FROM resource
		WHERE urn = any ($1) AND project_name = $2 AND namespace_name = $3 AND status NOT IN ($4, $5)
		LIMIT 1
	`
	urnsParam := make([]string, 0, len(urns))
	for _, urn := range urns {
		urnsParam = append(urnsParam, urn.String())
	}
	args := []any{urnsParam, tnnt.ProjectName(), tnnt.NamespaceName(), resource.StatusDeleted, resource.StatusToDelete}

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, errors.Wrap(resource.EntityResource, fmt.Sprintf("error reading resource with urn: '%s' found for project:%s, namespace:%s ", urns, tnnt.ProjectName(), tnnt.NamespaceName()), err)
	}

	defer rows.Close()

	var resources []*resource.Resource
	for rows.Next() {
		var res Resource
		err = rows.Scan(&res.ID, &res.FullName, &res.Kind, &res.Store, &res.Status, &res.URN,
			&res.ProjectName, &res.NamespaceName, &res.Metadata, &res.Spec, &res.Deprecated, &res.CreatedAt, &res.UpdatedAt)
		if err != nil {
			return nil, errors.Wrap(resource.EntityResource, "error in GetResourcesByURNs", err)
		}

		resourceModel, err := FromModelToResource(&res)
		if err != nil {
			return nil, err
		}
		resources = append(resources, resourceModel)
	}

	return resources, nil
}
