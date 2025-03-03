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

type NamespaceRepository struct {
	db *pgxpool.Pool
}

const (
	namespaceColumns = `id, name, config, variables, project_name, created_at, updated_at`
)

type Namespace struct {
	ID        uuid.UUID
	Name      string
	Config    map[string]string
	Variables map[string]string

	ProjectName string

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (n *Namespace) toTenantNamespace() (*tenant.Namespace, error) {
	projName, err := tenant.ProjectNameFrom(n.ProjectName)
	if err != nil {
		return nil, err
	}

	return tenant.NewNamespace(n.Name, projName, n.Config, n.Variables)
}

func (n *NamespaceRepository) Save(ctx context.Context, namespace *tenant.Namespace) error {
	_, err := n.get(ctx, namespace.ProjectName(), namespace.Name())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			insertNamespace := `INSERT INTO namespace (name, config, variables, project_name, created_at, updated_at)
VALUES ($1, $2, $3, $4, now(), now())`

			_, err = n.db.Exec(ctx, insertNamespace, namespace.Name(), namespace.GetConfigs(), namespace.GetVariables(), namespace.ProjectName())
			return errors.WrapIfErr(tenant.EntityNamespace, "unable to save namespace", err)
		}
		return errors.Wrap(tenant.EntityNamespace, "unable to save namespace", err)
	}

	updateNamespaceQuery := `UPDATE namespace n SET config=$1, variables=$2, updated_at=now() WHERE n.name = $3 AND n.project_name=$4`
	_, err = n.db.Exec(ctx, updateNamespaceQuery, namespace.GetConfigs(), namespace.GetVariables(), namespace.Name(), namespace.ProjectName())
	return errors.WrapIfErr(tenant.EntityProject, "unable to update namespace", err)
}

func (n *NamespaceRepository) GetByName(ctx context.Context, projectName tenant.ProjectName, name tenant.NamespaceName) (*tenant.Namespace, error) {
	ns, err := n.get(ctx, projectName, name)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.NotFound(tenant.EntityNamespace, "no record for "+name.String())
		}
		return nil, errors.Wrap(tenant.EntityNamespace, "error while getting project", err)
	}
	return ns.toTenantNamespace()
}

func (n *NamespaceRepository) get(ctx context.Context, projName tenant.ProjectName, name tenant.NamespaceName) (Namespace, error) {
	var namespace Namespace

	getNamespaceByNameQuery := `SELECT ` + namespaceColumns + ` FROM namespace WHERE project_name = $1 AND name = $2 AND deleted_at IS NULL`
	err := n.db.QueryRow(ctx, getNamespaceByNameQuery, projName, name).
		Scan(&namespace.ID, &namespace.Name, &namespace.Config, &namespace.Variables, &namespace.ProjectName, &namespace.CreatedAt, &namespace.UpdatedAt)
	if err != nil {
		return Namespace{}, err
	}
	return namespace, nil
}

func (n *NamespaceRepository) GetAll(ctx context.Context, projectName tenant.ProjectName) ([]*tenant.Namespace, error) {
	var namespaces []*tenant.Namespace

	getAllNamespaceInProject := `SELECT ` + namespaceColumns + ` FROM namespace n
WHERE project_name = $1 AND deleted_at IS NULL`
	rows, err := n.db.Query(ctx, getAllNamespaceInProject, projectName)
	if err != nil {
		return nil, errors.Wrap(tenant.EntityNamespace, "error in GetAll", err)
	}
	defer rows.Close()

	for rows.Next() {
		var ns Namespace
		err = rows.Scan(&ns.ID, &ns.Name, &ns.Config, &ns.Variables, &ns.ProjectName, &ns.CreatedAt, &ns.UpdatedAt)
		if err != nil {
			return nil, errors.Wrap(tenant.EntityNamespace, "error in GetAll", err)
		}

		namespace, err := ns.toTenantNamespace()
		if err != nil {
			return nil, err
		}
		namespaces = append(namespaces, namespace)
	}

	return namespaces, nil
}

func NewNamespaceRepository(pool *pgxpool.Pool) *NamespaceRepository {
	return &NamespaceRepository{
		db: pool,
	}
}
