package upstreamidentifier_test

import (
	"context"
	"errors"
	"testing"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/ext/store/bigquery"
	upstreamidentifier "github.com/goto/optimus/plugin/upstream_identifier"
)

func TestNewBQUpstreamIdentifier(t *testing.T) {
	logger := log.NewNoop()
	parserFunc := func(string) []string { return nil }
	bqExtractorFunc := func(context.Context, []bigquery.ResourceURN) (map[bigquery.ResourceURN]string, error) {
		return nil, nil // nolint: nilnil
	}
	evaluatorFunc := func(map[string]string) string { return "" }
	t.Run("should return error when logger is nil", func(t *testing.T) {
		bqUpstreamIdentifier, err := upstreamidentifier.NewBQUpstreamIdentifier(nil, parserFunc, bqExtractorFunc, evaluatorFunc)
		assert.ErrorContains(t, err, "logger is nil")
		assert.Nil(t, bqUpstreamIdentifier)
	})
	t.Run("should return error when parser is nil", func(t *testing.T) {
		bqUpstreamIdentifier, err := upstreamidentifier.NewBQUpstreamIdentifier(logger, nil, bqExtractorFunc, evaluatorFunc)
		assert.ErrorContains(t, err, "parserFunc is nil")
		assert.Nil(t, bqUpstreamIdentifier)
	})
	t.Run("should return error when extractor is nil", func(t *testing.T) {
		bqUpstreamIdentifier, err := upstreamidentifier.NewBQUpstreamIdentifier(logger, parserFunc, nil, evaluatorFunc)
		assert.ErrorContains(t, err, "bqExtractorFunc is nil")
		assert.Nil(t, bqUpstreamIdentifier)
	})
	t.Run("should return error when no evaluators", func(t *testing.T) {
		bqUpstreamIdentifier, err := upstreamidentifier.NewBQUpstreamIdentifier(logger, parserFunc, bqExtractorFunc)
		assert.ErrorContains(t, err, "evaluatorFuncs is needed")
		assert.Nil(t, bqUpstreamIdentifier)
	})
	t.Run("should return error when one of evaluator is nil", func(t *testing.T) {
		bqUpstreamIdentifier, err := upstreamidentifier.NewBQUpstreamIdentifier(logger, parserFunc, bqExtractorFunc, nil)
		assert.ErrorContains(t, err, "non-nil evaluatorFuncs is needed")
		assert.Nil(t, bqUpstreamIdentifier)
	})
	t.Run("should return success", func(t *testing.T) {
		bqUpstreamIdentifier, err := upstreamidentifier.NewBQUpstreamIdentifier(logger, parserFunc, bqExtractorFunc, evaluatorFunc)
		assert.NoError(t, err)
		assert.NotNil(t, bqUpstreamIdentifier)
	})
}

func TestIdentifyResources(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNoop()
	assets := map[string]string{
		"./query.sql": "select 1 from `project1.dataset1.name1`",
	}
	t.Run("should return empty resources when evaluator couldn't evaluate the asset", func(t *testing.T) {
		evaluatorFunc := new(EvalAssetFunc)
		defer evaluatorFunc.AssertExpectations(t)
		parserFunc := new(ParserFunc)
		defer parserFunc.AssertExpectations(t)
		bqExtractorFunc := new(BQExtractorFunc)
		defer bqExtractorFunc.AssertExpectations(t)

		evaluatorFunc.On("Execute", assets).Return("")
		bqUpstreamIdentifier, err := upstreamidentifier.NewBQUpstreamIdentifier(logger, parserFunc.Execute, bqExtractorFunc.Execute, evaluatorFunc.Execute)
		assert.NoError(t, err)
		assert.NotNil(t, bqUpstreamIdentifier)

		resourceURNs, err := bqUpstreamIdentifier.IdentifyResources(ctx, assets)
		assert.NoError(t, err)
		assert.Empty(t, resourceURNs)
	})
	t.Run("should return empty resource and error when extractor fail to extract", func(t *testing.T) {
		evaluatorFunc := new(EvalAssetFunc)
		defer evaluatorFunc.AssertExpectations(t)
		parserFunc := new(ParserFunc)
		defer parserFunc.AssertExpectations(t)
		bqExtractorFunc := new(BQExtractorFunc)
		defer bqExtractorFunc.AssertExpectations(t)

		evaluatorFunc.On("Execute", assets).Return(assets["./query.sql"])
		parserFunc.On("Execute", assets["./query.sql"]).Return([]string{"project1.dataset1.name1"})
		bqExtractorFunc.On("Execute", ctx, mock.Anything).Return(nil, errors.New("some error"))

		bqUpstreamIdentifier, err := upstreamidentifier.NewBQUpstreamIdentifier(logger, parserFunc.Execute, bqExtractorFunc.Execute, evaluatorFunc.Execute)
		assert.NoError(t, err)
		assert.NotNil(t, bqUpstreamIdentifier)

		resourceURNs, err := bqUpstreamIdentifier.IdentifyResources(ctx, assets)
		assert.Error(t, err)
		assert.Empty(t, resourceURNs)
	})
	t.Run("should skip the urn if parser passed with wrong urn formt", func(t *testing.T) {
		evaluatorFunc := new(EvalAssetFunc)
		defer evaluatorFunc.AssertExpectations(t)
		parserFunc := new(ParserFunc)
		defer parserFunc.AssertExpectations(t)
		bqExtractorFunc := new(BQExtractorFunc)
		defer bqExtractorFunc.AssertExpectations(t)

		evaluatorFunc.On("Execute", assets).Return(assets["./query.sql"])
		parserFunc.On("Execute", assets["./query.sql"]).Return([]string{"project1;dataset1.name1"})
		// bq extractor should not be executed since the result of parser is empty

		bqUpstreamIdentifier, err := upstreamidentifier.NewBQUpstreamIdentifier(logger, parserFunc.Execute, bqExtractorFunc.Execute, evaluatorFunc.Execute)
		assert.NoError(t, err)
		assert.NotNil(t, bqUpstreamIdentifier)

		resourceURNs, err := bqUpstreamIdentifier.IdentifyResources(ctx, assets)
		assert.NoError(t, err)
		assert.Empty(t, resourceURNs)
	})
	t.Run("should detect circular reference", func(t *testing.T) {
		// project1.dataset1.name1 -view-> select 1 from `project1.dataset1.name2`
		// project1.dataset1.name2 -view-> select 1 from `project1.dataset1.name1` join `project1.dataset1.name3` on true
		// project1.dataset1.name3 -table-
		evaluatorFunc := new(EvalAssetFunc)
		defer evaluatorFunc.AssertExpectations(t)
		parserFunc := new(ParserFunc)
		defer parserFunc.AssertExpectations(t)
		bqExtractorFunc := new(BQExtractorFunc)
		defer bqExtractorFunc.AssertExpectations(t)

		resourceURN1, _ := bigquery.NewResourceURN("project1", "dataset1", "name1")
		resourceURN2, _ := bigquery.NewResourceURN("project1", "dataset1", "name2")
		resourceURN3, _ := bigquery.NewResourceURN("project1", "dataset1", "name3")
		sqlView1 := "select 1 from `project1.dataset1.name2`"
		sqlView2 := "select 1 from `project1.dataset1.name1` join `project1.dataset1.name3` on true"

		evaluatorFunc.On("Execute", assets).Return(assets["./query.sql"])
		parserFunc.On("Execute", assets["./query.sql"]).Return([]string{"project1.dataset1.name1"})
		bqExtractorFunc.On("Execute", ctx, []bigquery.ResourceURN{resourceURN1}).Return(map[bigquery.ResourceURN]string{resourceURN1: sqlView1}, nil)

		parserFunc.On("Execute", sqlView1).Return([]string{"project1.dataset1.name2"})
		bqExtractorFunc.On("Execute", ctx, []bigquery.ResourceURN{resourceURN2}).Return(map[bigquery.ResourceURN]string{resourceURN2: sqlView2}, nil)

		parserFunc.On("Execute", sqlView2).Return([]string{"project1.dataset1.name1", "project1.dataset1.name3"})
		bqExtractorFunc.On("Execute", ctx, []bigquery.ResourceURN{resourceURN1, resourceURN3}).Return(map[bigquery.ResourceURN]string{resourceURN1: sqlView1, resourceURN3: ""}, nil)

		parserFunc.On("Execute", "").Return([]string{})

		bqUpstreamIdentifier, err := upstreamidentifier.NewBQUpstreamIdentifier(logger, parserFunc.Execute, bqExtractorFunc.Execute, evaluatorFunc.Execute)
		assert.NoError(t, err)
		assert.NotNil(t, bqUpstreamIdentifier)

		resourceURNs, err := bqUpstreamIdentifier.IdentifyResources(ctx, assets)
		assert.ErrorContains(t, err, "circular reference is detected")
		assert.Empty(t, resourceURNs)
	})
	t.Run("should generate unique resources", func(t *testing.T) {
		// project1.dataset1.name1 -view-> select 1 from `project1.dataset1.name2` join `project1.dataset1.name3` on true
		// project1.dataset1.name2 -view-> select 1 from `project1.dataset1.name3`
		// project1.dataset1.name3 -table-
		evaluatorFunc := new(EvalAssetFunc)
		defer evaluatorFunc.AssertExpectations(t)
		parserFunc := new(ParserFunc)
		defer parserFunc.AssertExpectations(t)
		bqExtractorFunc := new(BQExtractorFunc)
		defer bqExtractorFunc.AssertExpectations(t)

		resourceURN1, _ := bigquery.NewResourceURN("project1", "dataset1", "name1")
		resourceURN2, _ := bigquery.NewResourceURN("project1", "dataset1", "name2")
		resourceURN3, _ := bigquery.NewResourceURN("project1", "dataset1", "name3")
		sqlView1 := "select 1 from `project1.dataset1.name2`"
		sqlView2 := "select 1 from `project1.dataset1.name3`"

		evaluatorFunc.On("Execute", assets).Return(assets["./query.sql"])
		parserFunc.On("Execute", assets["./query.sql"]).Return([]string{"project1.dataset1.name1"})
		bqExtractorFunc.On("Execute", ctx, []bigquery.ResourceURN{resourceURN1}).Return(map[bigquery.ResourceURN]string{resourceURN1: sqlView1}, nil)

		parserFunc.On("Execute", sqlView1).Return([]string{"project1.dataset1.name2", "project1.dataset1.name3"})
		bqExtractorFunc.On("Execute", ctx, []bigquery.ResourceURN{resourceURN2, resourceURN3}).Return(map[bigquery.ResourceURN]string{resourceURN2: sqlView2, resourceURN3: ""}, nil)

		parserFunc.On("Execute", sqlView2).Return([]string{"project1.dataset1.name3"})
		bqExtractorFunc.On("Execute", ctx, []bigquery.ResourceURN{resourceURN3}).Return(map[bigquery.ResourceURN]string{resourceURN3: ""}, nil)

		parserFunc.On("Execute", "").Return([]string{})

		bqUpstreamIdentifier, err := upstreamidentifier.NewBQUpstreamIdentifier(logger, parserFunc.Execute, bqExtractorFunc.Execute, evaluatorFunc.Execute)
		assert.NoError(t, err)
		assert.NotNil(t, bqUpstreamIdentifier)

		urn1, err := resource.ParseURN("bigquery://project1:dataset1.name1")
		assert.NoError(t, err)
		urn2, err := resource.ParseURN("bigquery://project1:dataset1.name2")
		assert.NoError(t, err)
		urn3, err := resource.ParseURN("bigquery://project1:dataset1.name3")
		assert.NoError(t, err)

		expectedResourceURNs := []resource.URN{urn1, urn2, urn3}
		resourceURNs, err := bqUpstreamIdentifier.IdentifyResources(ctx, assets)
		assert.NoError(t, err)
		assert.NotEmpty(t, resourceURNs)
		assert.ElementsMatch(t, resourceURNs, expectedResourceURNs)
	})
}

// BQExtractorFunc is an autogenerated mock type for the BQExtractorFunc type
type BQExtractorFunc struct {
	mock.Mock
}

// Execute provides a mock function with given fields: _a0, _a1
func (_m *BQExtractorFunc) Execute(_a0 context.Context, _a1 []bigquery.ResourceURN) (map[bigquery.ResourceURN]string, error) {
	ret := _m.Called(_a0, _a1)

	var r0 map[bigquery.ResourceURN]string
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, []bigquery.ResourceURN) (map[bigquery.ResourceURN]string, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(context.Context, []bigquery.ResourceURN) map[bigquery.ResourceURN]string); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[bigquery.ResourceURN]string)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, []bigquery.ResourceURN) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// EvalAssetFunc is an autogenerated mock type for the EvalAssetFunc type
type EvalAssetFunc struct {
	mock.Mock
}

// Execute provides a mock function with given fields: assets
func (_m *EvalAssetFunc) Execute(assets map[string]string) string {
	ret := _m.Called(assets)

	var r0 string
	if rf, ok := ret.Get(0).(func(map[string]string) string); ok {
		r0 = rf(assets)
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// ParserFunc is an autogenerated mock type for the ParserFunc type
type ParserFunc struct {
	mock.Mock
}

// Execute provides a mock function with given fields: rawResource
func (_m *ParserFunc) Execute(rawResource string) []string {
	ret := _m.Called(rawResource)

	var r0 []string
	if rf, ok := ret.Get(0).(func(string) []string); ok {
		r0 = rf(rawResource)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]string)
		}
	}

	return r0
}
