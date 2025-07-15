package maxcompute

import (
	"fmt"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/goto/salt/log"

	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/utils"
)

type McTables interface {
	BatchLoadTables(tableNames []string) ([]McTableInstance, error)
}

type McTableInstance interface {
	Load() error
	ColumnMaskInfos() ([]odps.ColumnMaskInfo, error)
}

type maskingPolicyTracker struct {
	ColumnName string
	ToCreate   []string
	ToDelete   []string
}

type maskingPolicyTaskTracker struct {
	ColumnName    string
	MaskingPolicy string
	Action        string
	SQLTask       string
}

type MaskingPolicyHandle struct {
	mcSQLExecutor McSQLExecutor
	mcTable       McTables
	logger        log.Logger
}

func NewMaskingPolicyHandle(mcSQLExecutor McSQLExecutor, mcTables McTables, logger log.Logger) TableMaskingPolicyHandle {
	return &MaskingPolicyHandle{
		mcSQLExecutor: mcSQLExecutor,
		mcTable:       mcTables,
		logger:        logger,
	}
}

func (h MaskingPolicyHandle) Process(tableName string, schema Schema) error {
	newMaskPolicies := h.getNewMaskPolicies(schema)
	existingMaskPolicies, err := h.getExistingMaskPolicies(tableName)
	if err != nil {
		return err
	}

	tracker := compareMaskPolicies(newMaskPolicies, existingMaskPolicies)

	return h.applyMaskPolicyChanges(tracker, tableName)
}

func (MaskingPolicyHandle) getNewMaskPolicies(schema Schema) []odps.ColumnMaskInfo {
	newMaskPolicies := []odps.ColumnMaskInfo{}
	for _, column := range schema {
		if column.MaskPolicy == "" && column.UnmaskPolicy == "" {
			continue
		}

		maskPolicyNames := []string{}
		if column.MaskPolicy != "" {
			maskPolicyNames = append(maskPolicyNames, column.MaskPolicy)
		}
		if column.UnmaskPolicy != "" {
			maskPolicyNames = append(maskPolicyNames, column.UnmaskPolicy)
		}

		newMaskPolicies = append(newMaskPolicies, odps.ColumnMaskInfo{
			Name:           column.Name,
			PolicyNameList: maskPolicyNames,
		})
	}

	return newMaskPolicies
}

func (h MaskingPolicyHandle) getExistingMaskPolicies(tableName string) ([]odps.ColumnMaskInfo, error) {
	tables, err := h.mcTable.BatchLoadTables([]string{tableName})
	if err != nil {
		return nil, errors.InternalError(EntityTable, "error while get table on maxcompute", err)
	}
	existing := tables[0]

	err = existing.Load()
	if err != nil {
		return nil, errors.InternalError(EntityTable, "error while loading table from maxcompute", err)
	}

	existingMaskPolicies, err := existing.ColumnMaskInfos()
	if err != nil {
		return nil, errors.InternalError(EntityTable, "error while getting column mask info from maxcompute", err)
	}
	return existingMaskPolicies, nil
}

func (h MaskingPolicyHandle) applyMaskPolicyChanges(trackersPerColumn []maskingPolicyTracker, tableName string) error {
	sqlTasks := []maskingPolicyTaskTracker{}
	schemaName := h.mcSQLExecutor.CurrentSchemaName()

	for _, t := range trackersPerColumn {
		// order matters: we want to attach new masking policies before removing old ones so there's no downtime
		for _, policy := range t.ToCreate {
			sqlTasks = append(sqlTasks, maskingPolicyTaskTracker{
				ColumnName:    t.ColumnName,
				MaskingPolicy: policy,
				Action:        "bind",
				SQLTask:       fmt.Sprintf("APPLY DATA MASKING POLICY %s BIND TO TABLE %s.%s COLUMN %s;", policy, schemaName, tableName, t.ColumnName),
			})
		}

		for _, policy := range t.ToDelete {
			sqlTasks = append(sqlTasks, maskingPolicyTaskTracker{
				ColumnName:    t.ColumnName,
				MaskingPolicy: policy,
				Action:        "unbind",
				SQLTask:       fmt.Sprintf("APPLY DATA MASKING POLICY %s UNBIND FROM TABLE %s.%s COLUMN %s;", policy, schemaName, tableName, t.ColumnName),
			})
		}
	}

	me := errors.NewMultiError("error when applying masking policies")
	for _, task := range sqlTasks {
		ins, err := h.mcSQLExecutor.ExecSQlWithHints(task.SQLTask, nil)
		if err != nil {
			err = parseMaskingPolicyError(err)
			h.logger.Info("[masking-policy: error] action %s for masking policy %s to table %s.%s column %s failed: %s", task.Action, task.MaskingPolicy, task.ColumnName, schemaName, tableName, err.Error())
			me.Append(fmt.Errorf("failed to %s masking policy %s for column %s: %s", task.Action, task.MaskingPolicy, task.ColumnName, err.Error()))
			continue
		}

		err = ins.WaitForSuccess()
		if err != nil {
			err = parseMaskingPolicyError(err)
			h.logger.Info("[masking-policy: error] action %s for masking policy %s to table %s.%s column %s failed: %s", task.Action, task.MaskingPolicy, task.ColumnName, schemaName, tableName, err.Error())
			me.Append(fmt.Errorf("failed to %s masking policy %s to column %s: %s", task.Action, task.MaskingPolicy, task.ColumnName, err.Error()))
			continue
		}

		h.logger.Info("[masking-policy: success] %s for masking policy %s to table %s.%s column %s succeeded", task.Action, task.MaskingPolicy, task.ColumnName, schemaName, tableName, err.Error())
	}

	return me.ToErr()
}

func compareMaskPolicies(newColumnMasks, existingColumnMasks []odps.ColumnMaskInfo) []maskingPolicyTracker {
	existingMap := make(map[string]odps.ColumnMaskInfo)
	for _, column := range existingColumnMasks {
		existingMap[column.Name] = column
	}

	var trackers []maskingPolicyTracker
	for _, column := range newColumnMasks {
		tracker := maskingPolicyTracker{ColumnName: column.Name}
		if existing, found := existingMap[column.Name]; found {
			tracker.ToCreate, tracker.ToDelete = utils.CompareStringSlices(column.PolicyNameList, existing.PolicyNameList)
			delete(existingMap, column.Name)
		} else {
			tracker.ToCreate = column.PolicyNameList
		}
		trackers = append(trackers, tracker)
	}

	for _, column := range existingMap {
		trackers = append(trackers, maskingPolicyTracker{
			ColumnName: column.Name,
			ToDelete:   column.PolicyNameList,
		})
	}

	return trackers
}

func parseMaskingPolicyError(err error) error {
	if err == nil {
		return nil
	}

	if strings.Contains(err.Error(), "ExceptionBase:") {
		exceptionBaseIdx := strings.Index(err.Error(), "ExceptionBase:") + len("ExceptionBase:")
		actualErrorMsg := strings.TrimSpace(err.Error()[exceptionBaseIdx:])
		return fmt.Errorf("error from maxCompute: %s", actualErrorMsg)
	}

	return err
}
