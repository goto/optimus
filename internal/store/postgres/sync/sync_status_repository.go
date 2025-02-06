package sync

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

const (
	syncColumnsToStore = `project_name, entity_type, identifier, last_update_time, last_sync_attempt, remarks`
	syncColumns        = `id, ` + syncColumnsToStore

	entitySyncStatus = "SYNC_STATUS"
)

type StatusRepository struct {
	db *pgxpool.Pool
}

func NewStatusSyncRepository(pool *pgxpool.Pool) *StatusRepository {
	return &StatusRepository{db: pool}
}

func (s StatusRepository) create(ctx context.Context, projectName tenant.ProjectName, entityType, identifier string, remarks map[string]string, success bool) error {
	insertQuery := `INSERT INTO sync_status (` + syncColumnsToStore + `)VALUES ($1, $2, $3, $4 , $5 , $6)`
	var lastUpdateTime *time.Time
	lastSyncAttempt := time.Now()
	if success {
		lastUpdateTime = &lastSyncAttempt
	}
	remarksByte, err := json.Marshal(remarks)
	if err != nil {
		return errors.Wrap(entitySyncStatus, "unable to serialise remarks", err)
	}
	_, err = s.db.Exec(ctx, insertQuery, projectName, entityType, identifier, lastUpdateTime, lastSyncAttempt, remarksByte)
	if err != nil {
		return errors.Wrap(entitySyncStatus, "unable to insert status entry", err)
	}
	return nil
}

func (s StatusRepository) Upsert(ctx context.Context, projectName tenant.ProjectName, entityType, identifier string, remarks map[string]string, success bool) error {
	var updateSyncSuccess string
	if success {
		updateSyncSuccess = ", last_update_time = NOW() "
	}
	remarksByte, err := json.Marshal(remarks)
	if err != nil {
		return errors.Wrap(entitySyncStatus, "unable to serialise remarks", err)
	}
	updateQuery := `update sync_status set last_sync_attempt = NOW(), remarks = $4 ` + updateSyncSuccess + ` where  project_name=$1 and entity_type=$2 and identifier=$3`
	tag, err := s.db.Exec(ctx, updateQuery, projectName, entityType, identifier, remarksByte)
	if err != nil {
		return errors.Wrap(entitySyncStatus, "unable to update status entry", err)
	}
	if tag.RowsAffected() == 0 {
		return s.create(ctx, projectName, entityType, identifier, remarks, success)
	}
	return nil
}

func (s StatusRepository) GetLastUpdateTime(ctx context.Context, projectName tenant.ProjectName, entityType string, identifiers []string) (map[string]time.Time, error) {
	lastUpdateMap := make(map[string]time.Time)
	getQuery := "select identifier, last_update_time from  sync_status where  project_name=$1 and entity_type=$2 and identifier in ('" + strings.Join(identifiers, "', '") + "')"
	rows, err := s.db.Query(ctx, getQuery, projectName, entityType)
	if err != nil {
		return nil, errors.Wrap(entitySyncStatus, "error while getting last sync update status", err)
	}

	for rows.Next() {
		var identifier string
		var lastUpdate time.Time
		err := rows.Scan(&identifier, &lastUpdate)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return lastUpdateMap, nil
			}
			return nil, errors.Wrap(entitySyncStatus, "error while getting last sync update status", err)
		}

		lastUpdateMap[identifier] = lastUpdate
	}
	return lastUpdateMap, nil
}

func (s StatusRepository) UpdateBatch(ctx context.Context, projectName tenant.ProjectName, entityType string, identifiers []string) error {
	if len(identifiers) < 1 {
		return nil
	}
	updateQuery := "update sync_status set last_update_time = NOW() where  project_name=$1 and entity_type=$2 and identifier in ('" + strings.Join(identifiers, "', '") + "')"
	tag, err := s.db.Exec(ctx, updateQuery, projectName, entityType)
	if err != nil {
		return errors.Wrap(entitySyncStatus, "unable to update status entry", err)
	}
	if tag.RowsAffected() == 0 {
		return errors.NotFound(entitySyncStatus, "unable to update sync times")
	}
	return nil
}

func (s StatusRepository) UpdateBulk(ctx context.Context, projectName tenant.ProjectName, entityType string, syncStatus []resource.SyncStatus) error {
	if len(syncStatus) < 1 {
		return nil
	}

	me := errors.NewMultiError("update bulk")

	for _, i := range syncStatus {
		remarks := make(map[string]string)
		if !i.Success {
			remarks["error"] = i.ErrorMsg
		}
		me.Append(s.Upsert(ctx, projectName, entityType, i.ResourceName, remarks, i.Success))
	}

	return me.ToErr()
}
