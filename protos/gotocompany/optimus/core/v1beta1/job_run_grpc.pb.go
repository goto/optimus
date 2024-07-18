// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             (unknown)
// source: gotocompany/optimus/core/v1beta1/job_run.proto

package optimus

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// JobRunServiceClient is the client API for JobRunService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type JobRunServiceClient interface {
	// JobRunInput is used to fetch task/hook compiled configuration and assets.
	JobRunInput(ctx context.Context, in *JobRunInputRequest, opts ...grpc.CallOption) (*JobRunInputResponse, error)
	// JobRun returns the current and past run status of jobs on a given range
	JobRun(ctx context.Context, in *JobRunRequest, opts ...grpc.CallOption) (*JobRunResponse, error)
	// RegisterJobEvent notifies optimus service about an event related to job
	RegisterJobEvent(ctx context.Context, in *RegisterJobEventRequest, opts ...grpc.CallOption) (*RegisterJobEventResponse, error)
	// UploadToScheduler comiles jobSpec from database into DAGs and uploads the generated DAGs to scheduler
	UploadToScheduler(ctx context.Context, in *UploadToSchedulerRequest, opts ...grpc.CallOption) (*UploadToSchedulerResponse, error)
	// GetInterval gets interval on specific job given reference time.
	GetInterval(ctx context.Context, in *GetIntervalRequest, opts ...grpc.CallOption) (*GetIntervalResponse, error)
	// GetInterval gets interval on specific job given reference time.
	GetJobUpstreamRun(ctx context.Context, in *GetJobUpstreamRunRequest, opts ...grpc.CallOption) (*GetJobUpstreamRunResponse, error)
}

type jobRunServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewJobRunServiceClient(cc grpc.ClientConnInterface) JobRunServiceClient {
	return &jobRunServiceClient{cc}
}

func (c *jobRunServiceClient) JobRunInput(ctx context.Context, in *JobRunInputRequest, opts ...grpc.CallOption) (*JobRunInputResponse, error) {
	out := new(JobRunInputResponse)
	err := c.cc.Invoke(ctx, "/gotocompany.optimus.core.v1beta1.JobRunService/JobRunInput", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *jobRunServiceClient) JobRun(ctx context.Context, in *JobRunRequest, opts ...grpc.CallOption) (*JobRunResponse, error) {
	out := new(JobRunResponse)
	err := c.cc.Invoke(ctx, "/gotocompany.optimus.core.v1beta1.JobRunService/JobRun", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *jobRunServiceClient) RegisterJobEvent(ctx context.Context, in *RegisterJobEventRequest, opts ...grpc.CallOption) (*RegisterJobEventResponse, error) {
	out := new(RegisterJobEventResponse)
	err := c.cc.Invoke(ctx, "/gotocompany.optimus.core.v1beta1.JobRunService/RegisterJobEvent", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *jobRunServiceClient) UploadToScheduler(ctx context.Context, in *UploadToSchedulerRequest, opts ...grpc.CallOption) (*UploadToSchedulerResponse, error) {
	out := new(UploadToSchedulerResponse)
	err := c.cc.Invoke(ctx, "/gotocompany.optimus.core.v1beta1.JobRunService/UploadToScheduler", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *jobRunServiceClient) GetInterval(ctx context.Context, in *GetIntervalRequest, opts ...grpc.CallOption) (*GetIntervalResponse, error) {
	out := new(GetIntervalResponse)
	err := c.cc.Invoke(ctx, "/gotocompany.optimus.core.v1beta1.JobRunService/GetInterval", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *jobRunServiceClient) GetJobUpstreamRun(ctx context.Context, in *GetJobUpstreamRunRequest, opts ...grpc.CallOption) (*GetJobUpstreamRunResponse, error) {
	out := new(GetJobUpstreamRunResponse)
	err := c.cc.Invoke(ctx, "/gotocompany.optimus.core.v1beta1.JobRunService/GetJobUpstreamRun", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// JobRunServiceServer is the server API for JobRunService service.
// All implementations must embed UnimplementedJobRunServiceServer
// for forward compatibility
type JobRunServiceServer interface {
	// JobRunInput is used to fetch task/hook compiled configuration and assets.
	JobRunInput(context.Context, *JobRunInputRequest) (*JobRunInputResponse, error)
	// JobRun returns the current and past run status of jobs on a given range
	JobRun(context.Context, *JobRunRequest) (*JobRunResponse, error)
	// RegisterJobEvent notifies optimus service about an event related to job
	RegisterJobEvent(context.Context, *RegisterJobEventRequest) (*RegisterJobEventResponse, error)
	// UploadToScheduler comiles jobSpec from database into DAGs and uploads the generated DAGs to scheduler
	UploadToScheduler(context.Context, *UploadToSchedulerRequest) (*UploadToSchedulerResponse, error)
	// GetInterval gets interval on specific job given reference time.
	GetInterval(context.Context, *GetIntervalRequest) (*GetIntervalResponse, error)
	// GetInterval gets interval on specific job given reference time.
	GetJobUpstreamRun(context.Context, *GetJobUpstreamRunRequest) (*GetJobUpstreamRunResponse, error)
	mustEmbedUnimplementedJobRunServiceServer()
}

// UnimplementedJobRunServiceServer must be embedded to have forward compatible implementations.
type UnimplementedJobRunServiceServer struct {
}

func (UnimplementedJobRunServiceServer) JobRunInput(context.Context, *JobRunInputRequest) (*JobRunInputResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method JobRunInput not implemented")
}
func (UnimplementedJobRunServiceServer) JobRun(context.Context, *JobRunRequest) (*JobRunResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method JobRun not implemented")
}
func (UnimplementedJobRunServiceServer) RegisterJobEvent(context.Context, *RegisterJobEventRequest) (*RegisterJobEventResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RegisterJobEvent not implemented")
}
func (UnimplementedJobRunServiceServer) UploadToScheduler(context.Context, *UploadToSchedulerRequest) (*UploadToSchedulerResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UploadToScheduler not implemented")
}
func (UnimplementedJobRunServiceServer) GetInterval(context.Context, *GetIntervalRequest) (*GetIntervalResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetInterval not implemented")
}
func (UnimplementedJobRunServiceServer) GetJobUpstreamRun(context.Context, *GetJobUpstreamRunRequest) (*GetJobUpstreamRunResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetJobUpstreamRun not implemented")
}
func (UnimplementedJobRunServiceServer) mustEmbedUnimplementedJobRunServiceServer() {}

// UnsafeJobRunServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to JobRunServiceServer will
// result in compilation errors.
type UnsafeJobRunServiceServer interface {
	mustEmbedUnimplementedJobRunServiceServer()
}

func RegisterJobRunServiceServer(s grpc.ServiceRegistrar, srv JobRunServiceServer) {
	s.RegisterService(&JobRunService_ServiceDesc, srv)
}

func _JobRunService_JobRunInput_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(JobRunInputRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobRunServiceServer).JobRunInput(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gotocompany.optimus.core.v1beta1.JobRunService/JobRunInput",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobRunServiceServer).JobRunInput(ctx, req.(*JobRunInputRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _JobRunService_JobRun_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(JobRunRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobRunServiceServer).JobRun(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gotocompany.optimus.core.v1beta1.JobRunService/JobRun",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobRunServiceServer).JobRun(ctx, req.(*JobRunRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _JobRunService_RegisterJobEvent_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RegisterJobEventRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobRunServiceServer).RegisterJobEvent(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gotocompany.optimus.core.v1beta1.JobRunService/RegisterJobEvent",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobRunServiceServer).RegisterJobEvent(ctx, req.(*RegisterJobEventRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _JobRunService_UploadToScheduler_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UploadToSchedulerRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobRunServiceServer).UploadToScheduler(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gotocompany.optimus.core.v1beta1.JobRunService/UploadToScheduler",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobRunServiceServer).UploadToScheduler(ctx, req.(*UploadToSchedulerRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _JobRunService_GetInterval_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetIntervalRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobRunServiceServer).GetInterval(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gotocompany.optimus.core.v1beta1.JobRunService/GetInterval",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobRunServiceServer).GetInterval(ctx, req.(*GetIntervalRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _JobRunService_GetJobUpstreamRun_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetJobUpstreamRunRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobRunServiceServer).GetJobUpstreamRun(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/gotocompany.optimus.core.v1beta1.JobRunService/GetJobUpstreamRun",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobRunServiceServer).GetJobUpstreamRun(ctx, req.(*GetJobUpstreamRunRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// JobRunService_ServiceDesc is the grpc.ServiceDesc for JobRunService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var JobRunService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "gotocompany.optimus.core.v1beta1.JobRunService",
	HandlerType: (*JobRunServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "JobRunInput",
			Handler:    _JobRunService_JobRunInput_Handler,
		},
		{
			MethodName: "JobRun",
			Handler:    _JobRunService_JobRun_Handler,
		},
		{
			MethodName: "RegisterJobEvent",
			Handler:    _JobRunService_RegisterJobEvent_Handler,
		},
		{
			MethodName: "UploadToScheduler",
			Handler:    _JobRunService_UploadToScheduler_Handler,
		},
		{
			MethodName: "GetInterval",
			Handler:    _JobRunService_GetInterval_Handler,
		},
		{
			MethodName: "GetJobUpstreamRun",
			Handler:    _JobRunService_GetJobUpstreamRun_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "gotocompany/optimus/core/v1beta1/job_run.proto",
}
