package maxcompute_test

import (
	"errors"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/ext/store/maxcompute"
)

func TestMaskingPolicyHandle(t *testing.T) {
	accessID, accessKey, endpoint := "access_id", "access_key", "http://service.ap-southeast-5.maxcompute.aliyun.com/api"
	projectName, schemaName, tableName := "proj", "schema", "test"
	odpsInstance := odps.NewInstance(odps.NewOdps(account.NewAliyunAccount(accessID, accessKey), endpoint), projectName, "")

	tbl := maxcompute.Table{
		Name:     tableName,
		Database: schemaName,
		Project:  projectName,
		Schema: maxcompute.Schema{
			{Name: "column1", MaskPolicy: "mask_policy", UnmaskPolicy: "unmask_policy"},
			{Name: "column2"},
			{Name: "column3"},
		},
	}

	t.Run("Process", func(t *testing.T) {
		t.Run("returns error when not able to get table", func(t *testing.T) {
			mockTables := new(mockMcTables)
			mockTables.On("BatchLoadTables", []string{tableName}).Return(nil, errors.New("error fetching table detail"))
			defer mockTables.AssertExpectations(t)

			mockSQLExecutor := new(mockOdpsIns)

			handle := maxcompute.NewMaskingPolicyHandle(mockSQLExecutor, mockTables)
			err := handle.Process(&tbl)

			assert.Error(t, err)
			assert.Contains(t, err.Error(), "error while get table on maxcompute")
		})

		t.Run("returns error when not able to load table", func(t *testing.T) {
			mockTableIns := new(mockTableInstance)
			mockTableIns.On("Load").Return(errors.New("error fetching table detail"))
			defer mockTableIns.AssertExpectations(t)

			mockTables := new(mockMcTables)
			mockTables.On("BatchLoadTables", []string{tableName}).Return([]maxcompute.McTableInstance{mockTableIns}, nil)
			defer mockTables.AssertExpectations(t)

			mockSQLExecutor := new(mockOdpsIns)

			handle := maxcompute.NewMaskingPolicyHandle(mockSQLExecutor, mockTables)
			err := handle.Process(&tbl)

			assert.Error(t, err)
			assert.Contains(t, err.Error(), "error while loading table from maxcompute")
		})

		t.Run("returns error when parsing masking information returns error", func(t *testing.T) {
			mockTableIns := new(mockTableInstance)
			mockTableIns.On("Load").Return(nil)
			mockTableIns.On("ColumnMaskInfos").Return(nil, errors.New("error fetching column mask info"))
			defer mockTableIns.AssertExpectations(t)

			mockTables := new(mockMcTables)
			mockTables.On("BatchLoadTables", []string{tableName}).Return([]maxcompute.McTableInstance{mockTableIns}, nil)
			defer mockTables.AssertExpectations(t)

			mockSQLExecutor := new(mockOdpsIns)

			handle := maxcompute.NewMaskingPolicyHandle(mockSQLExecutor, mockTables)
			err := handle.Process(&tbl)

			assert.Error(t, err)
			assert.Contains(t, err.Error(), "error while getting column mask info from maxcompute")
		})

		t.Run("returns error when executing SQL to apply policy changes", func(t *testing.T) {
			existingMaskingInfo := []odps.ColumnMaskInfo{
				{Name: "column2", PolicyNameList: []string{"old_mask_policy", "old_unmask_policy"}},
			}
			mockTableIns := new(mockTableInstance)
			mockTableIns.On("Load").Return(nil)
			mockTableIns.On("ColumnMaskInfos").Return(existingMaskingInfo, nil)
			defer mockTableIns.AssertExpectations(t)

			mockTables := new(mockMcTables)
			mockTables.On("BatchLoadTables", []string{tableName}).Return([]maxcompute.McTableInstance{mockTableIns}, nil)
			defer mockTables.AssertExpectations(t)

			mockSQLExecutor := new(mockOdpsIns)
			mockSQLExecutor.On("CurrentSchemaName").Return(schemaName)
			mockSQLExecutor.On("ExecSQlWithHints", mock.Anything, mock.Anything).Return(odpsInstance, errors.New("cannot apply"))
			defer mockSQLExecutor.AssertExpectations(t)

			handle := maxcompute.NewMaskingPolicyHandle(mockSQLExecutor, mockTables)
			err := handle.Process(&tbl)

			assert.Error(t, err)
			assert.Contains(t, err.Error(), "error when applying masking policies")
		})

		t.Run("returns error when waiting for masking policy updates", func(t *testing.T) {
			existingMaskingInfo := []odps.ColumnMaskInfo{
				{Name: "column2", PolicyNameList: []string{"old_mask_policy", "old_unmask_policy"}},
			}
			mockTableIns := new(mockTableInstance)
			mockTableIns.On("Load").Return(nil)
			mockTableIns.On("ColumnMaskInfos").Return(existingMaskingInfo, nil)
			defer mockTableIns.AssertExpectations(t)

			mockTables := new(mockMcTables)
			mockTables.On("BatchLoadTables", []string{tableName}).Return([]maxcompute.McTableInstance{mockTableIns}, nil)
			defer mockTables.AssertExpectations(t)

			mockSQLExecutor := new(mockOdpsIns)
			mockSQLExecutor.On("CurrentSchemaName").Return(schemaName)
			mockSQLExecutor.On("ExecSQlWithHints", "APPLY DATA MASKING POLICY old_mask_policy UNBIND FROM TABLE schema.test COLUMN column2;", mock.Anything).Return(odpsInstance, nil)
			mockSQLExecutor.On("ExecSQlWithHints", "APPLY DATA MASKING POLICY old_unmask_policy UNBIND FROM TABLE schema.test COLUMN column2;", mock.Anything).Return(odpsInstance, nil)
			mockSQLExecutor.On("ExecSQlWithHints", "APPLY DATA MASKING POLICY mask_policy BIND TO TABLE schema.test COLUMN column1;", mock.Anything).Return(odpsInstance, nil)
			mockSQLExecutor.On("ExecSQlWithHints", "APPLY DATA MASKING POLICY unmask_policy BIND TO TABLE schema.test COLUMN column1;", mock.Anything).Return(odpsInstance, nil)
			defer mockSQLExecutor.AssertExpectations(t)

			handle := maxcompute.NewMaskingPolicyHandle(mockSQLExecutor, mockTables)
			err := handle.Process(&tbl)

			assert.Error(t, err)
			assert.Contains(t, err.Error(), "error when applying masking policies")
		})
	})
}

type mockMcTables struct {
	mock.Mock
}

func (m *mockMcTables) BatchLoadTables(tableNames []string) ([]maxcompute.McTableInstance, error) {
	args := m.Called(tableNames)
	if args.Get(0) != nil {
		return args.Get(0).([]maxcompute.McTableInstance), args.Error(1)
	}
	return nil, args.Error(1)
}

type mockTableInstance struct {
	mock.Mock
}

func (m *mockTableInstance) Load() error {
	return m.Called().Error(0)
}

func (m *mockTableInstance) ColumnMaskInfos() ([]odps.ColumnMaskInfo, error) {
	args := m.Called()
	if args.Get(0) != nil {
		return args.Get(0).([]odps.ColumnMaskInfo), args.Error(1)
	}
	return nil, args.Error(1)
}
