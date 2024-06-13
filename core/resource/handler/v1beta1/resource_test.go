package v1beta1_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/resource/handler/v1beta1"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/writer"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

func TestResourceHandler(t *testing.T) {
	logger := log.NewNoop()
	ctx := context.Background()
	tnnt, _ := tenant.NewTenant("proj", "ns")

	t.Run("DeployResourceSpecification", func(t *testing.T) {
		t.Run("returns error when client sends error", func(t *testing.T) {
			service := new(resourceService)
			handler := v1beta1.NewResourceHandler(logger, service)

			stream := new(resourceStreamMock)
			stream.On("Context").Return(ctx)
			stream.On("Recv").Return(nil, errors.New("req timeout")).Once()
			stream.On("Send", mock.Anything).Return(nil)

			err := handler.DeployResourceSpecification(stream)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "req timeout")
		})
		t.Run("returns error when tenant is invalid", func(t *testing.T) {
			service := new(resourceService)
			handler := v1beta1.NewResourceHandler(logger, service)

			req := &pb.DeployResourceSpecificationRequest{
				ProjectName:   "",
				DatastoreName: "bigquery",
				Resources:     nil,
				NamespaceName: "ns",
			}

			argMatcher := mock.MatchedBy(func(req *pb.DeployResourceSpecificationResponse) bool {
				return req.LogStatus.Message == "invalid tenant information request project [] namespace [ns]: invalid argument for entity project: project name is empty"
			})
			stream := new(resourceStreamMock)
			stream.On("Context").Return(ctx)
			stream.On("Recv").Return(req, nil).Once()
			stream.On("Recv").Return(nil, io.EOF).Once()
			stream.On("Send", argMatcher).Return(nil).Once()

			err := handler.DeployResourceSpecification(stream)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error when deploying: [ns]")
		})
		t.Run("returns error when store is invalid", func(t *testing.T) {
			service := new(resourceService)
			handler := v1beta1.NewResourceHandler(logger, service)

			req := &pb.DeployResourceSpecificationRequest{
				ProjectName:   "proj",
				DatastoreName: "unknown",
				Resources:     nil,
				NamespaceName: "ns",
			}

			argMatcher := mock.MatchedBy(func(req *pb.DeployResourceSpecificationResponse) bool {
				return req.LogStatus.Message == "invalid store name [unknown]: invalid argument for entity resource: unknown store unknown"
			})
			stream := new(resourceStreamMock)
			stream.On("Context").Return(ctx)
			stream.On("Recv").Return(req, nil).Once()
			stream.On("Recv").Return(nil, io.EOF).Once()
			stream.On("Send", argMatcher).Return(nil).Once()

			err := handler.DeployResourceSpecification(stream)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error when deploying: [ns]")
		})
		t.Run("returns error log when conversion fails", func(t *testing.T) {
			service := new(resourceService)
			service.On("Deploy", ctx, mock.Anything, resource.Bigquery, mock.Anything, mock.Anything).Return(nil)
			defer service.AssertExpectations(t)

			handler := v1beta1.NewResourceHandler(logger, service)

			res1 := pb.ResourceSpecification{
				Version: 1,
				Name:    "proj.set.name1",
				Type:    "table",
				Spec:    nil,
				Assets:  nil,
				Labels:  nil,
			}

			req := &pb.DeployResourceSpecificationRequest{
				ProjectName:   "proj",
				DatastoreName: "bigquery",
				Resources:     []*pb.ResourceSpecification{&res1},
				NamespaceName: "ns",
			}

			argMatcher := mock.MatchedBy(func(req *pb.DeployResourceSpecificationResponse) bool {
				return req.LogStatus.Message == "invalid argument for entity resource: empty resource spec for proj.set.name1: cannot adapt resource proj.set.name1"
			})
			stream := new(resourceStreamMock)
			stream.On("Context").Return(ctx)
			stream.On("Recv").Return(req, nil).Once()
			stream.On("Recv").Return(nil, io.EOF).Once()
			stream.On("Send", argMatcher).Return(nil).Once()
			stream.On("Send", mock.Anything).Return(nil)

			err := handler.DeployResourceSpecification(stream)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error when deploying: [ns]")
		})
		t.Run("returns error log when service returns error", func(t *testing.T) {
			service := new(resourceService)
			service.On("Deploy", mock.Anything, tnnt, resource.Bigquery, mock.Anything, mock.Anything).
				Return(errors.New("error in batch"))
			defer service.AssertExpectations(t)

			handler := v1beta1.NewResourceHandler(logger, service)

			spec, _ := structpb.NewStruct(map[string]any{"description": "spec"})
			res1 := pb.ResourceSpecification{
				Version: 1,
				Name:    "proj.set.name1",
				Type:    "table",
				Spec:    spec,
				Assets:  nil,
				Labels:  nil,
			}

			req := &pb.DeployResourceSpecificationRequest{
				ProjectName:   "proj",
				DatastoreName: "bigquery",
				Resources:     []*pb.ResourceSpecification{&res1},
				NamespaceName: "ns",
			}

			argMatcher := mock.MatchedBy(func(req *pb.DeployResourceSpecificationResponse) bool {
				return req.LogStatus.Message == "failed to update resources: error in batch"
			})
			stream := new(resourceStreamMock)
			stream.On("Context").Return(ctx)
			stream.On("Recv").Return(req, nil).Once()
			stream.On("Recv").Return(nil, io.EOF).Once()
			stream.On("Send", argMatcher).Return(nil).Once()
			stream.On("Send", mock.Anything).Return(nil).Once()

			err := handler.DeployResourceSpecification(stream)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error when deploying: [ns]")
		})
		t.Run("successfully updates the resources", func(t *testing.T) {
			service := new(resourceService)
			service.On("Deploy", mock.Anything, tnnt, resource.Bigquery, mock.Anything, mock.Anything).Return(nil)
			defer service.AssertExpectations(t)

			handler := v1beta1.NewResourceHandler(logger, service)

			spec, _ := structpb.NewStruct(map[string]any{"description": "spec"})
			res1 := pb.ResourceSpecification{
				Version: 1,
				Name:    "proj.set.name1",
				Type:    "table",
				Spec:    spec,
				Assets:  nil,
				Labels:  nil,
			}

			req := &pb.DeployResourceSpecificationRequest{
				ProjectName:   "proj",
				DatastoreName: "bigquery",
				Resources:     []*pb.ResourceSpecification{&res1},
				NamespaceName: "ns",
			}

			argMatcher := mock.MatchedBy(func(req *pb.DeployResourceSpecificationResponse) bool {
				return req.LogStatus.Message == "[1] resources with namespace [ns] are deployed successfully"
			})
			stream := new(resourceStreamMock)
			stream.On("Context").Return(ctx)
			stream.On("Recv").Return(req, nil).Once()
			stream.On("Recv").Return(nil, io.EOF).Once()
			stream.On("Send", argMatcher).Return(nil).Once()

			err := handler.DeployResourceSpecification(stream)
			assert.Nil(t, err)
		})
	})
	t.Run("ListResourceSpecification", func(t *testing.T) {
		t.Run("returns error when store is invalid", func(t *testing.T) {
			service := new(resourceService)
			handler := v1beta1.NewResourceHandler(logger, service)

			req := &pb.ListResourceSpecificationRequest{
				ProjectName:   "proj",
				DatastoreName: "",
				NamespaceName: "ns",
			}

			_, err := handler.ListResourceSpecification(ctx, req)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "invalid list resource request")
		})
		t.Run("returns error when tenant is invalid", func(t *testing.T) {
			service := new(resourceService)
			handler := v1beta1.NewResourceHandler(logger, service)

			req := &pb.ListResourceSpecificationRequest{
				ProjectName:   "",
				DatastoreName: "bigquery",
				NamespaceName: "ns",
			}

			_, err := handler.ListResourceSpecification(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"project: project name is empty: failed to list resource for bigquery")
		})
		t.Run("returns error when service returns error", func(t *testing.T) {
			service := new(resourceService)
			service.On("GetAll", ctx, mock.Anything, resource.Bigquery).
				Return(nil, errors.New("error in getAll"))
			defer service.AssertExpectations(t)

			handler := v1beta1.NewResourceHandler(logger, service)

			req := &pb.ListResourceSpecificationRequest{
				ProjectName:   "proj",
				DatastoreName: "bigquery",
				NamespaceName: "ns",
			}

			_, err := handler.ListResourceSpecification(ctx, req)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "failed to list resource for bigquery")
		})
		t.Run("returns error when unable to convert", func(t *testing.T) {
			service := new(resourceService)
			service.On("GetAll", ctx, mock.Anything, resource.Bigquery).
				Return([]*resource.Resource{{}}, nil)
			defer service.AssertExpectations(t)

			handler := v1beta1.NewResourceHandler(logger, service)

			req := &pb.ListResourceSpecificationRequest{
				ProjectName:   "proj",
				DatastoreName: "bigquery",
				NamespaceName: "ns",
			}

			_, err := handler.ListResourceSpecification(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"resource: missing resource metadata: failed to parse resource ")
		})
		t.Run("lists the resources successfully", func(t *testing.T) {
			spec := map[string]any{"a": "b"}
			dbRes, err := resource.NewResource("proj.set.table", "table", resource.Bigquery, tnnt,
				&resource.Metadata{}, spec)
			assert.Nil(t, err)

			service := new(resourceService)
			service.On("GetAll", ctx, mock.Anything, resource.Bigquery).
				Return([]*resource.Resource{dbRes}, nil)
			defer service.AssertExpectations(t)

			handler := v1beta1.NewResourceHandler(logger, service)

			req := &pb.ListResourceSpecificationRequest{
				ProjectName:   "proj",
				DatastoreName: "bigquery",
				NamespaceName: "ns",
			}

			res, err := handler.ListResourceSpecification(ctx, req)
			assert.Nil(t, err)

			assert.Equal(t, 1, len(res.Resources))
			assert.Equal(t, dbRes.FullName(), res.Resources[0].Name)
		})
	})
	t.Run("CreateResource", func(t *testing.T) {
		t.Run("returns error when tenant is invalid", func(t *testing.T) {
			service := new(resourceService)
			handler := v1beta1.NewResourceHandler(logger, service)

			createReq := &pb.CreateResourceRequest{
				ProjectName:   "",
				DatastoreName: "bigquery",
				Resource:      nil,
				NamespaceName: "",
			}

			_, err := handler.CreateResource(ctx, createReq)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"project: project name is empty: failed to create resource")
		})
		t.Run("returns error when store is invalid", func(t *testing.T) {
			service := new(resourceService)
			handler := v1beta1.NewResourceHandler(logger, service)

			createReq := &pb.CreateResourceRequest{
				ProjectName:   "proj",
				DatastoreName: "",
				Resource:      nil,
				NamespaceName: "ns",
			}

			_, err := handler.CreateResource(ctx, createReq)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"resource: unknown store : invalid create resource request")
		})
		t.Run("returns error when spec is invalid", func(t *testing.T) {
			service := new(resourceService)
			handler := v1beta1.NewResourceHandler(logger, service)

			createReq := &pb.CreateResourceRequest{
				ProjectName:   "proj",
				DatastoreName: "bigquery",
				Resource: &pb.ResourceSpecification{
					Version: 0,
					Name:    "project.set.table",
					Type:    "table",
				},
				NamespaceName: "ns",
			}

			_, err := handler.CreateResource(ctx, createReq)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"resource: empty resource spec for project.set.table: failed to create resource")
		})
		t.Run("returns error when resource is nil", func(t *testing.T) {
			service := new(resourceService)
			handler := v1beta1.NewResourceHandler(logger, service)

			createReq := &pb.CreateResourceRequest{
				ProjectName:   "proj",
				DatastoreName: "bigquery",
				Resource:      nil,
				NamespaceName: "ns",
			}

			_, err := handler.CreateResource(ctx, createReq)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"resource: empty resource: failed to create resource")
		})
		t.Run("returns error when kind is empty", func(t *testing.T) {
			service := new(resourceService)
			handler := v1beta1.NewResourceHandler(logger, service)

			spec, _ := structpb.NewStruct(map[string]interface{}{"a": "b"})
			createReq := &pb.CreateResourceRequest{
				ProjectName:   "proj",
				DatastoreName: "bigquery",
				Resource: &pb.ResourceSpecification{
					Name:    "project.dataset.table",
					Version: 0,
					Type:    "",
					Spec:    spec,
				},
				NamespaceName: "ns",
			}

			_, err := handler.CreateResource(ctx, createReq)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "empty resource type for project.dataset.table")
		})
		t.Run("returns error when name is invalid", func(t *testing.T) {
			service := new(resourceService)
			handler := v1beta1.NewResourceHandler(logger, service)

			spec, _ := structpb.NewStruct(map[string]interface{}{"a": "b"})
			createReq := &pb.CreateResourceRequest{
				ProjectName:   "proj",
				DatastoreName: "bigquery",
				Resource: &pb.ResourceSpecification{
					Version: 0,
					Name:    "",
					Type:    "table",
					Spec:    spec,
				},
				NamespaceName: "ns",
			}

			_, err := handler.CreateResource(ctx, createReq)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"resource: resource name is empty: failed to create resource")
		})
		t.Run("returns error when service returns error", func(t *testing.T) {
			service := new(resourceService)
			service.On("Create", ctx, mock.Anything).Return(errors.New("validation failure"))
			defer service.AssertExpectations(t)

			handler := v1beta1.NewResourceHandler(logger, service)

			spec, _ := structpb.NewStruct(map[string]interface{}{"a": "b"})
			createReq := &pb.CreateResourceRequest{
				ProjectName:   "proj",
				DatastoreName: "bigquery",
				Resource: &pb.ResourceSpecification{
					Version: 0,
					Name:    "proj.set.table",
					Type:    "table",
					Spec:    spec,
				},
				NamespaceName: "ns",
			}

			_, err := handler.CreateResource(ctx, createReq)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = Internal desc = validation failure: failed to "+
				"create resource proj.set.table")
		})
		t.Run("creates the resource successfully", func(t *testing.T) {
			service := new(resourceService)
			service.On("Create", ctx, mock.Anything).Return(nil)
			defer service.AssertExpectations(t)

			handler := v1beta1.NewResourceHandler(logger, service)

			spec, _ := structpb.NewStruct(map[string]interface{}{"description": "test"})
			createReq := &pb.CreateResourceRequest{
				ProjectName:   "proj",
				DatastoreName: "bigquery",
				Resource: &pb.ResourceSpecification{
					Version: 0,
					Name:    "proj.set.table",
					Type:    "table",
					Spec:    spec,
				},
				NamespaceName: "ns",
			}

			_, err := handler.CreateResource(ctx, createReq)
			assert.Nil(t, err)
		})
	})
	t.Run("ReadResource", func(t *testing.T) {
		t.Run("returns error when name is invalid", func(t *testing.T) {
			service := new(resourceService)
			handler := v1beta1.NewResourceHandler(logger, service)

			req := &pb.ReadResourceRequest{
				ResourceName:  "",
				ProjectName:   "proj",
				DatastoreName: "bigquery",
				NamespaceName: "ns",
			}

			_, err := handler.ReadResource(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"resource: empty resource name: invalid read resource request")
		})
		t.Run("returns error when store is invalid", func(t *testing.T) {
			service := new(resourceService)
			handler := v1beta1.NewResourceHandler(logger, service)

			req := &pb.ReadResourceRequest{
				ProjectName:   "proj",
				DatastoreName: "",
				ResourceName:  "proj.set.name",
				NamespaceName: "ns",
			}

			_, err := handler.ReadResource(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"resource: unknown store : invalid read resource request")
		})
		t.Run("returns error when tenant is invalid", func(t *testing.T) {
			service := new(resourceService)
			handler := v1beta1.NewResourceHandler(logger, service)

			req := &pb.ReadResourceRequest{
				ProjectName:   "",
				DatastoreName: "bigquery",
				ResourceName:  "proj.set.name",
				NamespaceName: "",
			}

			_, err := handler.ReadResource(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"project: project name is empty: failed to read resource proj.set.name")
		})
		t.Run("returns error when service returns error", func(t *testing.T) {
			service := new(resourceService)
			name := "proj.set.table"
			service.On("Get", ctx, mock.Anything, resource.Bigquery, name).Return(nil, errors.New("failure"))
			defer service.AssertExpectations(t)

			handler := v1beta1.NewResourceHandler(logger, service)

			req := &pb.ReadResourceRequest{
				ProjectName:   "proj",
				DatastoreName: "bigquery",
				ResourceName:  name,
				NamespaceName: "ns",
			}

			_, err := handler.ReadResource(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = Internal desc = failure: failed to read "+
				"resource proj.set.table")
		})
		t.Run("returns error when metadata missing in db resource", func(t *testing.T) {
			service := new(resourceService)
			name := "proj.set.table"
			service.On("Get", ctx, mock.Anything, resource.Bigquery, name).Return(&resource.Resource{}, nil)
			defer service.AssertExpectations(t)

			handler := v1beta1.NewResourceHandler(logger, service)

			req := &pb.ReadResourceRequest{
				ProjectName:   "proj",
				DatastoreName: "bigquery",
				ResourceName:  "proj.set.table",
				NamespaceName: "ns",
			}

			_, err := handler.ReadResource(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"resource: missing resource metadata: failed to read resource proj.set.table")
		})
		t.Run("returns error when error in spec to ", func(t *testing.T) {
			invalidKey := "a\xc5z"
			specWithInvalidUTF := map[string]any{invalidKey: "value"}
			dbRes, err := resource.NewResource("proj.set.table", "table", resource.Bigquery, tnnt,
				&resource.Metadata{}, specWithInvalidUTF)
			assert.Nil(t, err)
			service := new(resourceService)
			name := "proj.set.table"
			service.On("Get", ctx, mock.Anything, resource.Bigquery, name).Return(dbRes, nil)
			defer service.AssertExpectations(t)

			handler := v1beta1.NewResourceHandler(logger, service)

			req := &pb.ReadResourceRequest{
				ProjectName:   "proj",
				DatastoreName: "bigquery",
				ResourceName:  "proj.set.table",
				NamespaceName: "ns",
			}

			_, err = handler.ReadResource(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"resource: unable to convert spec to proto struct: failed to read resource proj.set.table")
		})
		t.Run("returns the resource successfully", func(t *testing.T) {
			spec := map[string]any{"a": "b"}
			dbRes, err := resource.NewResource("proj.set.table", "table", resource.Bigquery, tnnt,
				&resource.Metadata{}, spec)
			assert.Nil(t, err)

			service := new(resourceService)
			name := "proj.set.table"
			service.On("Get", ctx, mock.Anything, resource.Bigquery, name).Return(dbRes, nil)
			defer service.AssertExpectations(t)

			handler := v1beta1.NewResourceHandler(logger, service)

			req := &pb.ReadResourceRequest{
				ProjectName:   "proj",
				DatastoreName: "bigquery",
				ResourceName:  "proj.set.table",
				NamespaceName: "ns",
			}

			res, err := handler.ReadResource(ctx, req)
			assert.Nil(t, err)

			assert.Equal(t, "proj.set.table", res.Resource.Name)
			assert.Equal(t, dbRes.Kind(), res.Resource.Type)
		})
	})
	t.Run("UpdateResource", func(t *testing.T) {
		t.Run("returns error when tenant is invalid", func(t *testing.T) {
			service := new(resourceService)
			handler := v1beta1.NewResourceHandler(logger, service)

			req := &pb.UpdateResourceRequest{
				ProjectName:   "",
				DatastoreName: "bigquery",
				Resource:      nil,
				NamespaceName: "",
			}

			_, err := handler.UpdateResource(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"project: project name is empty: failed to update resource")
		})
		t.Run("returns error when store is invalid", func(t *testing.T) {
			service := new(resourceService)
			handler := v1beta1.NewResourceHandler(logger, service)

			req := &pb.UpdateResourceRequest{
				ProjectName:   "proj",
				DatastoreName: "",
				Resource:      nil,
				NamespaceName: "ns",
			}

			_, err := handler.UpdateResource(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"resource: unknown store : invalid update resource request")
		})
		t.Run("returns error when resource is nil", func(t *testing.T) {
			service := new(resourceService)
			handler := v1beta1.NewResourceHandler(logger, service)

			req := &pb.UpdateResourceRequest{
				ProjectName:   "proj",
				DatastoreName: "bigquery",
				Resource:      nil,
				NamespaceName: "ns",
			}

			_, err := handler.UpdateResource(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"resource: empty resource: failed to update resource")
		})
		t.Run("returns error when kind is empty", func(t *testing.T) {
			service := new(resourceService)
			handler := v1beta1.NewResourceHandler(logger, service)

			spec, _ := structpb.NewStruct(map[string]interface{}{"a": "b"})
			req := &pb.UpdateResourceRequest{
				ProjectName:   "proj",
				DatastoreName: "bigquery",
				Resource: &pb.ResourceSpecification{
					Name:    "proj.ds.table1",
					Version: 0,
					Type:    "",
					Spec:    spec,
				},
				NamespaceName: "ns",
			}

			_, err := handler.UpdateResource(ctx, req)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "empty resource type for proj.ds.table1")
		})
		t.Run("returns error when service returns error", func(t *testing.T) {
			service := new(resourceService)
			service.On("Update", ctx, mock.Anything, mock.Anything).Return(errors.New("validation failure"))
			defer service.AssertExpectations(t)

			handler := v1beta1.NewResourceHandler(logger, service)

			spec, _ := structpb.NewStruct(map[string]interface{}{"a": "b"})
			req := &pb.UpdateResourceRequest{
				ProjectName:   "proj",
				DatastoreName: "bigquery",
				Resource: &pb.ResourceSpecification{
					Version: 0,
					Name:    "proj.set.table",
					Type:    "table",
					Spec:    spec,
				},
				NamespaceName: "ns",
			}

			_, err := handler.UpdateResource(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = Internal desc = validation failure: failed to "+
				"update resource proj.set.table")
		})
		t.Run("updates the resource successfully", func(t *testing.T) {
			service := new(resourceService)
			service.On("Update", ctx, mock.Anything, mock.Anything).Return(nil)
			defer service.AssertExpectations(t)

			handler := v1beta1.NewResourceHandler(logger, service)

			spec, _ := structpb.NewStruct(map[string]interface{}{"description": "test"})
			req := &pb.UpdateResourceRequest{
				ProjectName:   "proj",
				DatastoreName: "bigquery",
				Resource: &pb.ResourceSpecification{
					Version: 0,
					Name:    "proj.set.table",
					Type:    "table",
					Spec:    spec,
				},
				NamespaceName: "ns",
			}

			_, err := handler.UpdateResource(ctx, req)
			assert.Nil(t, err)
		})
	})
	t.Run("UpsertResource", func(t *testing.T) {
		t.Run("returns error when tenant is invalid", func(t *testing.T) {
			service := new(resourceService)
			handler := v1beta1.NewResourceHandler(logger, service)

			req := &pb.UpsertResourceRequest{
				ProjectName:   "",
				DatastoreName: "bigquery",
				Resource:      nil,
				NamespaceName: "",
			}

			_, err := handler.UpsertResource(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"project: project name is empty: failed to upsert resource")
		})
		t.Run("returns error when store is invalid", func(t *testing.T) {
			service := new(resourceService)
			handler := v1beta1.NewResourceHandler(logger, service)

			req := &pb.UpsertResourceRequest{
				ProjectName:   "proj",
				DatastoreName: "",
				Resource:      nil,
				NamespaceName: "ns",
			}

			_, err := handler.UpsertResource(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"resource: unknown store : invalid upsert resource request")
		})
		t.Run("returns error when resource is nil", func(t *testing.T) {
			service := new(resourceService)
			handler := v1beta1.NewResourceHandler(logger, service)

			req := &pb.UpsertResourceRequest{
				ProjectName:   "proj",
				DatastoreName: "bigquery",
				Resource:      nil,
				NamespaceName: "ns",
			}

			_, err := handler.UpsertResource(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"resource: empty resource: failed to upsert resource")
		})
		t.Run("returns error when kind is empty", func(t *testing.T) {
			service := new(resourceService)
			handler := v1beta1.NewResourceHandler(logger, service)

			spec, _ := structpb.NewStruct(map[string]interface{}{"a": "b"})
			req := &pb.UpsertResourceRequest{
				ProjectName:   "proj",
				DatastoreName: "bigquery",
				Resource: &pb.ResourceSpecification{
					Name:    "proj.ds.table1",
					Version: 0,
					Type:    "",
					Spec:    spec,
				},
				NamespaceName: "ns",
			}

			_, err := handler.UpsertResource(ctx, req)
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "empty resource type for proj.ds.table1")
		})
		t.Run("returns error when service returns error", func(t *testing.T) {
			service := new(resourceService)
			service.On("Upsert", ctx, mock.Anything, mock.Anything).Return(errors.New("validation failure"))
			defer service.AssertExpectations(t)

			handler := v1beta1.NewResourceHandler(logger, service)

			spec, _ := structpb.NewStruct(map[string]interface{}{"a": "b"})
			req := &pb.UpsertResourceRequest{
				ProjectName:   "proj",
				DatastoreName: "bigquery",
				Resource: &pb.ResourceSpecification{
					Version: 0,
					Name:    "proj.set.table",
					Type:    "table",
					Spec:    spec,
				},
				NamespaceName: "ns",
			}

			_, err := handler.UpsertResource(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = Internal desc = validation failure: failed to "+
				"upsert resource proj.set.table")
		})
		t.Run("upsert the resource successfully", func(t *testing.T) {
			service := new(resourceService)
			service.On("Upsert", ctx, mock.Anything, mock.Anything).Return(nil)
			defer service.AssertExpectations(t)

			handler := v1beta1.NewResourceHandler(logger, service)

			spec, _ := structpb.NewStruct(map[string]interface{}{"description": "test"})
			req := &pb.UpsertResourceRequest{
				ProjectName:   "proj",
				DatastoreName: "bigquery",
				Resource: &pb.ResourceSpecification{
					Version: 0,
					Name:    "proj.set.table",
					Type:    "table",
					Spec:    spec,
				},
				NamespaceName: "ns",
			}

			_, err := handler.UpsertResource(ctx, req)
			assert.Nil(t, err)
		})
	})
	t.Run("ApplyResource", func(t *testing.T) {
		t.Run("returns error when tenant is invalid", func(t *testing.T) {
			service := new(resourceService)
			handler := v1beta1.NewResourceHandler(logger, service)

			req := &pb.ApplyResourcesRequest{
				ProjectName:   "",
				DatastoreName: "bigquery",
				ResourceNames: nil,
				NamespaceName: "",
			}

			_, err := handler.ApplyResources(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"project: project name is empty: invalid tenant details")
		})
		t.Run("returns error when store is invalid", func(t *testing.T) {
			service := new(resourceService)
			handler := v1beta1.NewResourceHandler(logger, service)

			req := &pb.ApplyResourcesRequest{
				ProjectName:   "proj",
				DatastoreName: "",
				ResourceNames: nil,
				NamespaceName: "ns",
			}

			_, err := handler.ApplyResources(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"resource: unknown store : invalid datastore Name")
		})
		t.Run("returns error when resource names are empty", func(t *testing.T) {
			service := new(resourceService)
			handler := v1beta1.NewResourceHandler(logger, service)

			req := &pb.ApplyResourcesRequest{
				ProjectName:   "proj",
				DatastoreName: "bigquery",
				ResourceNames: nil,
				NamespaceName: "ns",
			}

			_, err := handler.ApplyResources(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = InvalidArgument desc = invalid argument for entity "+
				"resource: empty resource names: unable to apply resources")
		})
		t.Run("returns error when service returns error", func(t *testing.T) {
			names := []string{"project.dataset.test_table"}

			service := new(resourceService)
			service.On("SyncResources", ctx, tnnt, resource.Bigquery, names).Return(nil, errors.New("something went wrong"))
			defer service.AssertExpectations(t)

			handler := v1beta1.NewResourceHandler(logger, service)

			req := &pb.ApplyResourcesRequest{
				ProjectName:   "proj",
				NamespaceName: "ns",
				DatastoreName: "bigquery",
				ResourceNames: names,
			}

			_, err := handler.ApplyResources(ctx, req)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "rpc error: code = Internal desc = something went wrong: "+
				"unable to sync to datastore")
		})
		t.Run("syncs the resources successfully", func(t *testing.T) {
			names := []string{"project.dataset.test_table"}

			service := new(resourceService)
			service.On("SyncResources", ctx, tnnt, resource.Bigquery, names).Return(
				&resource.SyncResponse{ResourceNames: names}, nil)
			defer service.AssertExpectations(t)

			handler := v1beta1.NewResourceHandler(logger, service)

			req := &pb.ApplyResourcesRequest{
				ProjectName:   "proj",
				NamespaceName: "ns",
				DatastoreName: "bigquery",
				ResourceNames: names,
			}

			resp, err := handler.ApplyResources(ctx, req)
			assert.Nil(t, err)

			assert.Equal(t, "success", resp.Statuses[0].Status)
			assert.Equal(t, names[0], resp.Statuses[0].ResourceName)
		})
	})
	t.Run("DeleteResource", func(t *testing.T) {
		resourceName := "project.dataset.test_table"
		spec := map[string]any{"a": "b"}
		existing, _ := resource.NewResource(resourceName, "table", resource.Bigquery, tnnt, &resource.Metadata{}, spec)

		t.Run("success", func(t *testing.T) {
			var (
				service = new(resourceService)
				handler = v1beta1.NewResourceHandler(logger, service)
				req     = &pb.DeleteResourceRequest{
					ProjectName:   tnnt.ProjectName().String(),
					NamespaceName: tnnt.NamespaceName().String(),
					DatastoreName: resource.Bigquery.String(),
					ResourceName:  existing.FullName(),
					Force:         false,
				}
			)
			defer service.AssertExpectations(t)

			var downstreamJobs []string
			deleteReq := &resource.DeleteRequest{
				Tenant:    tnnt,
				Datastore: resource.Bigquery,
				FullName:  req.GetResourceName(),
				Force:     req.GetForce(),
			}
			deleteRes := &resource.DeleteResponse{DownstreamJobs: downstreamJobs, Resource: existing}
			service.On("Delete", ctx, deleteReq).Return(deleteRes, nil)

			res, err := handler.DeleteResource(ctx, req)
			assert.NoError(t, err)
			assert.NotNil(t, res)
			assert.ElementsMatch(t, res.DownstreamJobs, downstreamJobs)
		})
		t.Run("success with force", func(t *testing.T) {
			var (
				service = new(resourceService)
				handler = v1beta1.NewResourceHandler(logger, service)
				req     = &pb.DeleteResourceRequest{
					ProjectName:   tnnt.ProjectName().String(),
					NamespaceName: tnnt.NamespaceName().String(),
					DatastoreName: resource.Bigquery.String(),
					ResourceName:  existing.FullName(),
					Force:         true,
				}
			)
			defer service.AssertExpectations(t)

			downstreamJobs := []string{"proj/JobA"}
			deleteReq := &resource.DeleteRequest{
				Tenant:    tnnt,
				Datastore: resource.Bigquery,
				FullName:  req.GetResourceName(),
				Force:     req.GetForce(),
			}
			deleteRes := &resource.DeleteResponse{DownstreamJobs: downstreamJobs, Resource: existing}
			service.On("Delete", ctx, deleteReq).Return(deleteRes, nil)

			res, err := handler.DeleteResource(ctx, req)
			assert.NoError(t, err)
			assert.NotNil(t, res)
			assert.NotNil(t, res.DownstreamJobs)
			assert.ElementsMatch(t, res.DownstreamJobs, downstreamJobs)
		})
		t.Run("return error when delete", func(t *testing.T) {
			var (
				service = new(resourceService)
				handler = v1beta1.NewResourceHandler(logger, service)
				req     = &pb.DeleteResourceRequest{
					ProjectName:   tnnt.ProjectName().String(),
					NamespaceName: tnnt.NamespaceName().String(),
					DatastoreName: resource.Bigquery.String(),
					ResourceName:  existing.FullName(),
					Force:         true,
				}
			)
			defer service.AssertExpectations(t)

			downstreamJobs := []string{"proj/JobA"}
			deleteReq := &resource.DeleteRequest{
				Tenant:    tnnt,
				Datastore: resource.Bigquery,
				FullName:  req.GetResourceName(),
				Force:     req.GetForce(),
			}
			deleteRes := &resource.DeleteResponse{DownstreamJobs: downstreamJobs, Resource: existing}
			service.On("Delete", ctx, deleteReq).Return(deleteRes, context.DeadlineExceeded)

			res, err := handler.DeleteResource(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, res)
		})
		t.Run("return error when resource store unknown", func(t *testing.T) {
			var (
				service = new(resourceService)
				handler = v1beta1.NewResourceHandler(logger, service)
				req     = &pb.DeleteResourceRequest{
					ProjectName:   tnnt.ProjectName().String(),
					NamespaceName: tnnt.NamespaceName().String(),
					DatastoreName: "unknown",
					ResourceName:  existing.FullName(),
					Force:         true,
				}
			)
			defer service.AssertExpectations(t)

			res, err := handler.DeleteResource(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, res)
		})
		t.Run("return error when tenant invalid", func(t *testing.T) {
			var (
				service = new(resourceService)
				handler = v1beta1.NewResourceHandler(logger, service)
				req     = &pb.DeleteResourceRequest{
					ProjectName:   tnnt.ProjectName().String(),
					NamespaceName: "",
					DatastoreName: "unknown",
					ResourceName:  existing.FullName(),
					Force:         true,
				}
			)
			defer service.AssertExpectations(t)

			res, err := handler.DeleteResource(ctx, req)
			assert.Error(t, err)
			assert.Nil(t, res)
		})
	})
}

type resourceService struct {
	mock.Mock
}

func (r *resourceService) Create(ctx context.Context, res *resource.Resource) error {
	args := r.Called(ctx, res)
	return args.Error(0)
}

func (r *resourceService) Update(ctx context.Context, res *resource.Resource, logWriter writer.LogWriter) error {
	args := r.Called(ctx, res, logWriter)
	return args.Error(0)
}

func (r *resourceService) Upsert(ctx context.Context, res *resource.Resource, logWriter writer.LogWriter) error {
	args := r.Called(ctx, res, logWriter)
	return args.Error(0)
}

func (r *resourceService) Delete(ctx context.Context, req *resource.DeleteRequest) (*resource.DeleteResponse, error) {
	args := r.Called(ctx, req)
	var jobs *resource.DeleteResponse
	if args.Get(0) != nil {
		jobs = args.Get(0).(*resource.DeleteResponse)
	}
	return jobs, args.Error(1)
}

func (r *resourceService) Get(ctx context.Context, tnnt tenant.Tenant, store resource.Store, resourceName string) (*resource.Resource, error) {
	args := r.Called(ctx, tnnt, store, resourceName)
	var rs *resource.Resource
	if args.Get(0) != nil {
		rs = args.Get(0).(*resource.Resource)
	}
	return rs, args.Error(1)
}

func (r *resourceService) GetAll(ctx context.Context, tnnt tenant.Tenant, store resource.Store) ([]*resource.Resource, error) {
	args := r.Called(ctx, tnnt, store)
	var resources []*resource.Resource
	if args.Get(0) != nil {
		resources = args.Get(0).([]*resource.Resource)
	}
	return resources, args.Error(1)
}

func (r *resourceService) Deploy(ctx context.Context, tnnt tenant.Tenant, store resource.Store, resources []*resource.Resource, logWriter writer.LogWriter) error {
	args := r.Called(ctx, tnnt, store, resources, logWriter)
	return args.Error(0)
}

func (r *resourceService) ChangeNamespace(ctx context.Context, datastore resource.Store, resourceFullName string, oldTenant, newTenant tenant.Tenant) error {
	return r.Called(ctx, datastore, resourceFullName, oldTenant, newTenant).Error(0)
}

func (r *resourceService) SyncResources(ctx context.Context, tnnt tenant.Tenant, store resource.Store, names []string) (*resource.SyncResponse, error) {
	args := r.Called(ctx, tnnt, store, names)
	var resources *resource.SyncResponse
	if args.Get(0) != nil {
		resources = args.Get(0).(*resource.SyncResponse)
	}
	return resources, args.Error(1)
}

type resourceStreamMock struct {
	mock.Mock
}

func (r *resourceStreamMock) Context() context.Context {
	args := r.Called()
	return args.Get(0).(context.Context)
}

func (r *resourceStreamMock) Send(response *pb.DeployResourceSpecificationResponse) error {
	args := r.Called(response)
	return args.Error(0)
}

func (r *resourceStreamMock) Recv() (*pb.DeployResourceSpecificationRequest, error) {
	args := r.Called()
	var rs *pb.DeployResourceSpecificationRequest
	if args.Get(0) != nil {
		rs = args.Get(0).(*pb.DeployResourceSpecificationRequest)
	}
	return rs, args.Error(1)
}

func (*resourceStreamMock) SetHeader(metadata.MD) error {
	panic("not supported")
}

func (*resourceStreamMock) SendHeader(metadata.MD) error {
	panic("not supported")
}

func (*resourceStreamMock) SetTrailer(metadata.MD) {
	panic("not supported")
}

func (*resourceStreamMock) SendMsg(interface{}) error {
	panic("not supported")
}

func (*resourceStreamMock) RecvMsg(interface{}) error {
	panic("not supported")
}
