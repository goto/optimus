// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.0
// 	protoc        (unknown)
// source: gotocompany/optimus/core/v1beta1/replay.proto

package optimus

import (
	_ "github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2/options"
	_ "google.golang.org/genproto/googleapis/api/annotations"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type ListReplayRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ProjectName string `protobuf:"bytes,1,opt,name=project_name,json=projectName,proto3" json:"project_name,omitempty"`
}

func (x *ListReplayRequest) Reset() {
	*x = ListReplayRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_gotocompany_optimus_core_v1beta1_replay_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListReplayRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListReplayRequest) ProtoMessage() {}

func (x *ListReplayRequest) ProtoReflect() protoreflect.Message {
	mi := &file_gotocompany_optimus_core_v1beta1_replay_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListReplayRequest.ProtoReflect.Descriptor instead.
func (*ListReplayRequest) Descriptor() ([]byte, []int) {
	return file_gotocompany_optimus_core_v1beta1_replay_proto_rawDescGZIP(), []int{0}
}

func (x *ListReplayRequest) GetProjectName() string {
	if x != nil {
		return x.ProjectName
	}
	return ""
}

type ListReplayResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Replays []*ReplaySpecification `protobuf:"bytes,1,rep,name=replays,proto3" json:"replays,omitempty"`
}

func (x *ListReplayResponse) Reset() {
	*x = ListReplayResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_gotocompany_optimus_core_v1beta1_replay_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListReplayResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListReplayResponse) ProtoMessage() {}

func (x *ListReplayResponse) ProtoReflect() protoreflect.Message {
	mi := &file_gotocompany_optimus_core_v1beta1_replay_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListReplayResponse.ProtoReflect.Descriptor instead.
func (*ListReplayResponse) Descriptor() ([]byte, []int) {
	return file_gotocompany_optimus_core_v1beta1_replay_proto_rawDescGZIP(), []int{1}
}

func (x *ListReplayResponse) GetReplays() []*ReplaySpecification {
	if x != nil {
		return x.Replays
	}
	return nil
}

type ReplaySpecification struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id          string                 `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	JobName     string                 `protobuf:"bytes,2,opt,name=job_name,json=jobName,proto3" json:"job_name,omitempty"`
	StartTime   *timestamppb.Timestamp `protobuf:"bytes,3,opt,name=start_time,json=startTime,proto3" json:"start_time,omitempty"`
	EndTime     *timestamppb.Timestamp `protobuf:"bytes,4,opt,name=end_time,json=endTime,proto3" json:"end_time,omitempty"`
	Description string                 `protobuf:"bytes,5,opt,name=description,proto3" json:"description,omitempty"`
	Status      string                 `protobuf:"bytes,6,opt,name=status,proto3" json:"status,omitempty"`
}

func (x *ReplaySpecification) Reset() {
	*x = ReplaySpecification{}
	if protoimpl.UnsafeEnabled {
		mi := &file_gotocompany_optimus_core_v1beta1_replay_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ReplaySpecification) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReplaySpecification) ProtoMessage() {}

func (x *ReplaySpecification) ProtoReflect() protoreflect.Message {
	mi := &file_gotocompany_optimus_core_v1beta1_replay_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReplaySpecification.ProtoReflect.Descriptor instead.
func (*ReplaySpecification) Descriptor() ([]byte, []int) {
	return file_gotocompany_optimus_core_v1beta1_replay_proto_rawDescGZIP(), []int{2}
}

func (x *ReplaySpecification) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *ReplaySpecification) GetJobName() string {
	if x != nil {
		return x.JobName
	}
	return ""
}

func (x *ReplaySpecification) GetStartTime() *timestamppb.Timestamp {
	if x != nil {
		return x.StartTime
	}
	return nil
}

func (x *ReplaySpecification) GetEndTime() *timestamppb.Timestamp {
	if x != nil {
		return x.EndTime
	}
	return nil
}

func (x *ReplaySpecification) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *ReplaySpecification) GetStatus() string {
	if x != nil {
		return x.Status
	}
	return ""
}

type ReplayRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ProjectName   string                 `protobuf:"bytes,1,opt,name=project_name,json=projectName,proto3" json:"project_name,omitempty"`
	JobName       string                 `protobuf:"bytes,2,opt,name=job_name,json=jobName,proto3" json:"job_name,omitempty"`
	NamespaceName string                 `protobuf:"bytes,3,opt,name=namespace_name,json=namespaceName,proto3" json:"namespace_name,omitempty"`
	StartTime     *timestamppb.Timestamp `protobuf:"bytes,4,opt,name=start_time,json=startTime,proto3" json:"start_time,omitempty"`
	EndTime       *timestamppb.Timestamp `protobuf:"bytes,5,opt,name=end_time,json=endTime,proto3" json:"end_time,omitempty"`
	Parallel      bool                   `protobuf:"varint,6,opt,name=parallel,proto3" json:"parallel,omitempty"`
	Description   string                 `protobuf:"bytes,7,opt,name=description,proto3" json:"description,omitempty"`
	JobConfig     string                 `protobuf:"bytes,8,opt,name=job_config,json=jobConfig,proto3" json:"job_config,omitempty"`
}

func (x *ReplayRequest) Reset() {
	*x = ReplayRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_gotocompany_optimus_core_v1beta1_replay_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ReplayRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReplayRequest) ProtoMessage() {}

func (x *ReplayRequest) ProtoReflect() protoreflect.Message {
	mi := &file_gotocompany_optimus_core_v1beta1_replay_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReplayRequest.ProtoReflect.Descriptor instead.
func (*ReplayRequest) Descriptor() ([]byte, []int) {
	return file_gotocompany_optimus_core_v1beta1_replay_proto_rawDescGZIP(), []int{3}
}

func (x *ReplayRequest) GetProjectName() string {
	if x != nil {
		return x.ProjectName
	}
	return ""
}

func (x *ReplayRequest) GetJobName() string {
	if x != nil {
		return x.JobName
	}
	return ""
}

func (x *ReplayRequest) GetNamespaceName() string {
	if x != nil {
		return x.NamespaceName
	}
	return ""
}

func (x *ReplayRequest) GetStartTime() *timestamppb.Timestamp {
	if x != nil {
		return x.StartTime
	}
	return nil
}

func (x *ReplayRequest) GetEndTime() *timestamppb.Timestamp {
	if x != nil {
		return x.EndTime
	}
	return nil
}

func (x *ReplayRequest) GetParallel() bool {
	if x != nil {
		return x.Parallel
	}
	return false
}

func (x *ReplayRequest) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *ReplayRequest) GetJobConfig() string {
	if x != nil {
		return x.JobConfig
	}
	return ""
}

type ReplayResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
}

func (x *ReplayResponse) Reset() {
	*x = ReplayResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_gotocompany_optimus_core_v1beta1_replay_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ReplayResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReplayResponse) ProtoMessage() {}

func (x *ReplayResponse) ProtoReflect() protoreflect.Message {
	mi := &file_gotocompany_optimus_core_v1beta1_replay_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReplayResponse.ProtoReflect.Descriptor instead.
func (*ReplayResponse) Descriptor() ([]byte, []int) {
	return file_gotocompany_optimus_core_v1beta1_replay_proto_rawDescGZIP(), []int{4}
}

func (x *ReplayResponse) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

var File_gotocompany_optimus_core_v1beta1_replay_proto protoreflect.FileDescriptor

var file_gotocompany_optimus_core_v1beta1_replay_proto_rawDesc = []byte{
	0x0a, 0x2d, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2f, 0x6f, 0x70,
	0x74, 0x69, 0x6d, 0x75, 0x73, 0x2f, 0x63, 0x6f, 0x72, 0x65, 0x2f, 0x76, 0x31, 0x62, 0x65, 0x74,
	0x61, 0x31, 0x2f, 0x72, 0x65, 0x70, 0x6c, 0x61, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12,
	0x20, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74,
	0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61,
	0x31, 0x1a, 0x1c, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x61, 0x6e,
	0x6e, 0x6f, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a,
	0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66,
	0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x1a, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x2d, 0x67, 0x65, 0x6e, 0x2d, 0x6f, 0x70, 0x65,
	0x6e, 0x61, 0x70, 0x69, 0x76, 0x32, 0x2f, 0x6f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2f, 0x61,
	0x6e, 0x6e, 0x6f, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x22, 0x36, 0x0a, 0x11, 0x4c, 0x69, 0x73, 0x74, 0x52, 0x65, 0x70, 0x6c, 0x61, 0x79, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x21, 0x0a, 0x0c, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74,
	0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x70, 0x72, 0x6f,
	0x6a, 0x65, 0x63, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x22, 0x65, 0x0a, 0x12, 0x4c, 0x69, 0x73, 0x74,
	0x52, 0x65, 0x70, 0x6c, 0x61, 0x79, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x4f,
	0x0a, 0x07, 0x72, 0x65, 0x70, 0x6c, 0x61, 0x79, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32,
	0x35, 0x2e, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70,
	0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74,
	0x61, 0x31, 0x2e, 0x52, 0x65, 0x70, 0x6c, 0x61, 0x79, 0x53, 0x70, 0x65, 0x63, 0x69, 0x66, 0x69,
	0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x07, 0x72, 0x65, 0x70, 0x6c, 0x61, 0x79, 0x73, 0x22,
	0xec, 0x01, 0x0a, 0x13, 0x52, 0x65, 0x70, 0x6c, 0x61, 0x79, 0x53, 0x70, 0x65, 0x63, 0x69, 0x66,
	0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x19, 0x0a, 0x08, 0x6a, 0x6f, 0x62, 0x5f, 0x6e,
	0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x6a, 0x6f, 0x62, 0x4e, 0x61,
	0x6d, 0x65, 0x12, 0x39, 0x0a, 0x0a, 0x73, 0x74, 0x61, 0x72, 0x74, 0x5f, 0x74, 0x69, 0x6d, 0x65,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61,
	0x6d, 0x70, 0x52, 0x09, 0x73, 0x74, 0x61, 0x72, 0x74, 0x54, 0x69, 0x6d, 0x65, 0x12, 0x35, 0x0a,
	0x08, 0x65, 0x6e, 0x64, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x07, 0x65, 0x6e, 0x64,
	0x54, 0x69, 0x6d, 0x65, 0x12, 0x20, 0x0a, 0x0b, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74,
	0x69, 0x6f, 0x6e, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x64, 0x65, 0x73, 0x63, 0x72,
	0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x16, 0x0a, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73,
	0x18, 0x06, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x22, 0xc3,
	0x02, 0x0a, 0x0d, 0x52, 0x65, 0x70, 0x6c, 0x61, 0x79, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x12, 0x21, 0x0a, 0x0c, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x4e,
	0x61, 0x6d, 0x65, 0x12, 0x19, 0x0a, 0x08, 0x6a, 0x6f, 0x62, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x6a, 0x6f, 0x62, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x25,
	0x0a, 0x0e, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63,
	0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x39, 0x0a, 0x0a, 0x73, 0x74, 0x61, 0x72, 0x74, 0x5f, 0x74,
	0x69, 0x6d, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67,
	0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65,
	0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x09, 0x73, 0x74, 0x61, 0x72, 0x74, 0x54, 0x69, 0x6d, 0x65,
	0x12, 0x35, 0x0a, 0x08, 0x65, 0x6e, 0x64, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x05, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x07,
	0x65, 0x6e, 0x64, 0x54, 0x69, 0x6d, 0x65, 0x12, 0x1a, 0x0a, 0x08, 0x70, 0x61, 0x72, 0x61, 0x6c,
	0x6c, 0x65, 0x6c, 0x18, 0x06, 0x20, 0x01, 0x28, 0x08, 0x52, 0x08, 0x70, 0x61, 0x72, 0x61, 0x6c,
	0x6c, 0x65, 0x6c, 0x12, 0x20, 0x0a, 0x0b, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69,
	0x6f, 0x6e, 0x18, 0x07, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69,
	0x70, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x1d, 0x0a, 0x0a, 0x6a, 0x6f, 0x62, 0x5f, 0x63, 0x6f, 0x6e,
	0x66, 0x69, 0x67, 0x18, 0x08, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x6a, 0x6f, 0x62, 0x43, 0x6f,
	0x6e, 0x66, 0x69, 0x67, 0x22, 0x20, 0x0a, 0x0e, 0x52, 0x65, 0x70, 0x6c, 0x61, 0x79, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x32, 0xda, 0x02, 0x0a, 0x0d, 0x52, 0x65, 0x70, 0x6c, 0x61,
	0x79, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x9e, 0x01, 0x0a, 0x06, 0x52, 0x65, 0x70,
	0x6c, 0x61, 0x79, 0x12, 0x2f, 0x2e, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e,
	0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76,
	0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x52, 0x65, 0x70, 0x6c, 0x61, 0x79, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x1a, 0x30, 0x2e, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61,
	0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e,
	0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x52, 0x65, 0x70, 0x6c, 0x61, 0x79, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x31, 0x82, 0xd3, 0xe4, 0x93, 0x02, 0x2b, 0x22, 0x26,
	0x2f, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2f, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74,
	0x2f, 0x7b, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x7d, 0x2f,
	0x72, 0x65, 0x70, 0x6c, 0x61, 0x79, 0x3a, 0x01, 0x2a, 0x12, 0xa7, 0x01, 0x0a, 0x0a, 0x4c, 0x69,
	0x73, 0x74, 0x52, 0x65, 0x70, 0x6c, 0x61, 0x79, 0x12, 0x33, 0x2e, 0x67, 0x6f, 0x74, 0x6f, 0x63,
	0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63,
	0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x4c, 0x69, 0x73, 0x74,
	0x52, 0x65, 0x70, 0x6c, 0x61, 0x79, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x34, 0x2e,
	0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69,
	0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31,
	0x2e, 0x4c, 0x69, 0x73, 0x74, 0x52, 0x65, 0x70, 0x6c, 0x61, 0x79, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x22, 0x2e, 0x82, 0xd3, 0xe4, 0x93, 0x02, 0x28, 0x12, 0x26, 0x2f, 0x76, 0x31,
	0x62, 0x65, 0x74, 0x61, 0x31, 0x2f, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x2f, 0x7b, 0x70,
	0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x7d, 0x2f, 0x72, 0x65, 0x70,
	0x6c, 0x61, 0x79, 0x42, 0x95, 0x01, 0x0a, 0x1e, 0x63, 0x6f, 0x6d, 0x2e, 0x67, 0x6f, 0x74, 0x6f,
	0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x6e, 0x2e, 0x6f,
	0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x42, 0x14, 0x52, 0x65, 0x70, 0x6c, 0x61, 0x79, 0x53, 0x65,
	0x72, 0x76, 0x69, 0x63, 0x65, 0x4d, 0x61, 0x6e, 0x61, 0x67, 0x65, 0x72, 0x50, 0x01, 0x5a, 0x1e,
	0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x67, 0x6f, 0x74, 0x6f, 0x2f,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x6e, 0x2f, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x92, 0x41,
	0x3a, 0x12, 0x05, 0x32, 0x03, 0x30, 0x2e, 0x31, 0x1a, 0x0e, 0x31, 0x32, 0x37, 0x2e, 0x30, 0x2e,
	0x30, 0x2e, 0x31, 0x3a, 0x39, 0x31, 0x30, 0x30, 0x22, 0x04, 0x2f, 0x61, 0x70, 0x69, 0x2a, 0x01,
	0x01, 0x72, 0x18, 0x0a, 0x16, 0x4f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x20, 0x52, 0x65, 0x70,
	0x6c, 0x61, 0x79, 0x20, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x62, 0x06, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x33,
}

var (
	file_gotocompany_optimus_core_v1beta1_replay_proto_rawDescOnce sync.Once
	file_gotocompany_optimus_core_v1beta1_replay_proto_rawDescData = file_gotocompany_optimus_core_v1beta1_replay_proto_rawDesc
)

func file_gotocompany_optimus_core_v1beta1_replay_proto_rawDescGZIP() []byte {
	file_gotocompany_optimus_core_v1beta1_replay_proto_rawDescOnce.Do(func() {
		file_gotocompany_optimus_core_v1beta1_replay_proto_rawDescData = protoimpl.X.CompressGZIP(file_gotocompany_optimus_core_v1beta1_replay_proto_rawDescData)
	})
	return file_gotocompany_optimus_core_v1beta1_replay_proto_rawDescData
}

var file_gotocompany_optimus_core_v1beta1_replay_proto_msgTypes = make([]protoimpl.MessageInfo, 5)
var file_gotocompany_optimus_core_v1beta1_replay_proto_goTypes = []interface{}{
	(*ListReplayRequest)(nil),     // 0: gotocompany.optimus.core.v1beta1.ListReplayRequest
	(*ListReplayResponse)(nil),    // 1: gotocompany.optimus.core.v1beta1.ListReplayResponse
	(*ReplaySpecification)(nil),   // 2: gotocompany.optimus.core.v1beta1.ReplaySpecification
	(*ReplayRequest)(nil),         // 3: gotocompany.optimus.core.v1beta1.ReplayRequest
	(*ReplayResponse)(nil),        // 4: gotocompany.optimus.core.v1beta1.ReplayResponse
	(*timestamppb.Timestamp)(nil), // 5: google.protobuf.Timestamp
}
var file_gotocompany_optimus_core_v1beta1_replay_proto_depIdxs = []int32{
	2, // 0: gotocompany.optimus.core.v1beta1.ListReplayResponse.replays:type_name -> gotocompany.optimus.core.v1beta1.ReplaySpecification
	5, // 1: gotocompany.optimus.core.v1beta1.ReplaySpecification.start_time:type_name -> google.protobuf.Timestamp
	5, // 2: gotocompany.optimus.core.v1beta1.ReplaySpecification.end_time:type_name -> google.protobuf.Timestamp
	5, // 3: gotocompany.optimus.core.v1beta1.ReplayRequest.start_time:type_name -> google.protobuf.Timestamp
	5, // 4: gotocompany.optimus.core.v1beta1.ReplayRequest.end_time:type_name -> google.protobuf.Timestamp
	3, // 5: gotocompany.optimus.core.v1beta1.ReplayService.Replay:input_type -> gotocompany.optimus.core.v1beta1.ReplayRequest
	0, // 6: gotocompany.optimus.core.v1beta1.ReplayService.ListReplay:input_type -> gotocompany.optimus.core.v1beta1.ListReplayRequest
	4, // 7: gotocompany.optimus.core.v1beta1.ReplayService.Replay:output_type -> gotocompany.optimus.core.v1beta1.ReplayResponse
	1, // 8: gotocompany.optimus.core.v1beta1.ReplayService.ListReplay:output_type -> gotocompany.optimus.core.v1beta1.ListReplayResponse
	7, // [7:9] is the sub-list for method output_type
	5, // [5:7] is the sub-list for method input_type
	5, // [5:5] is the sub-list for extension type_name
	5, // [5:5] is the sub-list for extension extendee
	0, // [0:5] is the sub-list for field type_name
}

func init() { file_gotocompany_optimus_core_v1beta1_replay_proto_init() }
func file_gotocompany_optimus_core_v1beta1_replay_proto_init() {
	if File_gotocompany_optimus_core_v1beta1_replay_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_gotocompany_optimus_core_v1beta1_replay_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ListReplayRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_gotocompany_optimus_core_v1beta1_replay_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ListReplayResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_gotocompany_optimus_core_v1beta1_replay_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ReplaySpecification); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_gotocompany_optimus_core_v1beta1_replay_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ReplayRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_gotocompany_optimus_core_v1beta1_replay_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ReplayResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_gotocompany_optimus_core_v1beta1_replay_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   5,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_gotocompany_optimus_core_v1beta1_replay_proto_goTypes,
		DependencyIndexes: file_gotocompany_optimus_core_v1beta1_replay_proto_depIdxs,
		MessageInfos:      file_gotocompany_optimus_core_v1beta1_replay_proto_msgTypes,
	}.Build()
	File_gotocompany_optimus_core_v1beta1_replay_proto = out.File
	file_gotocompany_optimus_core_v1beta1_replay_proto_rawDesc = nil
	file_gotocompany_optimus_core_v1beta1_replay_proto_goTypes = nil
	file_gotocompany_optimus_core_v1beta1_replay_proto_depIdxs = nil
}
