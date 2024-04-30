// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.0
// 	protoc        (unknown)
// source: gotocompany/optimus/integration/v1beta1/event.proto

package optimus

import (
	v1beta1 "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
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

type ChangeImpact int32

const (
	ChangeImpact_CHANGE_IMPACT_TYPE_UNSPECIFIED ChangeImpact = 0
	ChangeImpact_CHANGE_IMPACT_TYPE_INTERNAL    ChangeImpact = 1
	ChangeImpact_CHANGE_IMPACT_TYPE_BEHAVIOUR   ChangeImpact = 2
)

// Enum value maps for ChangeImpact.
var (
	ChangeImpact_name = map[int32]string{
		0: "CHANGE_IMPACT_TYPE_UNSPECIFIED",
		1: "CHANGE_IMPACT_TYPE_INTERNAL",
		2: "CHANGE_IMPACT_TYPE_BEHAVIOUR",
	}
	ChangeImpact_value = map[string]int32{
		"CHANGE_IMPACT_TYPE_UNSPECIFIED": 0,
		"CHANGE_IMPACT_TYPE_INTERNAL":    1,
		"CHANGE_IMPACT_TYPE_BEHAVIOUR":   2,
	}
)

func (x ChangeImpact) Enum() *ChangeImpact {
	p := new(ChangeImpact)
	*p = x
	return p
}

func (x ChangeImpact) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (ChangeImpact) Descriptor() protoreflect.EnumDescriptor {
	return file_gotocompany_optimus_integration_v1beta1_event_proto_enumTypes[0].Descriptor()
}

func (ChangeImpact) Type() protoreflect.EnumType {
	return &file_gotocompany_optimus_integration_v1beta1_event_proto_enumTypes[0]
}

func (x ChangeImpact) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use ChangeImpact.Descriptor instead.
func (ChangeImpact) EnumDescriptor() ([]byte, []int) {
	return file_gotocompany_optimus_integration_v1beta1_event_proto_rawDescGZIP(), []int{0}
}

type OptimusChangeEvent_EventType int32

const (
	OptimusChangeEvent_EVENT_TYPE_TYPE_UNSPECIFIED  OptimusChangeEvent_EventType = 0
	OptimusChangeEvent_EVENT_TYPE_RESOURCE_CREATE   OptimusChangeEvent_EventType = 1
	OptimusChangeEvent_EVENT_TYPE_RESOURCE_UPDATE   OptimusChangeEvent_EventType = 2
	OptimusChangeEvent_EVENT_TYPE_RESOURCE_DELETE   OptimusChangeEvent_EventType = 11
	OptimusChangeEvent_EVENT_TYPE_JOB_CREATE        OptimusChangeEvent_EventType = 3
	OptimusChangeEvent_EVENT_TYPE_JOB_UPDATE        OptimusChangeEvent_EventType = 4
	OptimusChangeEvent_EVENT_TYPE_JOB_DELETE        OptimusChangeEvent_EventType = 5
	OptimusChangeEvent_EVENT_TYPE_JOB_WAIT_UPSTREAM OptimusChangeEvent_EventType = 6
	OptimusChangeEvent_EVENT_TYPE_JOB_IN_PROGRESS   OptimusChangeEvent_EventType = 7
	OptimusChangeEvent_EVENT_TYPE_JOB_SUCCESS       OptimusChangeEvent_EventType = 8
	OptimusChangeEvent_EVENT_TYPE_JOB_FAILURE       OptimusChangeEvent_EventType = 9
	OptimusChangeEvent_EVENT_TYPE_JOB_STATE_CHANGE  OptimusChangeEvent_EventType = 10
)

// Enum value maps for OptimusChangeEvent_EventType.
var (
	OptimusChangeEvent_EventType_name = map[int32]string{
		0:  "EVENT_TYPE_TYPE_UNSPECIFIED",
		1:  "EVENT_TYPE_RESOURCE_CREATE",
		2:  "EVENT_TYPE_RESOURCE_UPDATE",
		11: "EVENT_TYPE_RESOURCE_DELETE",
		3:  "EVENT_TYPE_JOB_CREATE",
		4:  "EVENT_TYPE_JOB_UPDATE",
		5:  "EVENT_TYPE_JOB_DELETE",
		6:  "EVENT_TYPE_JOB_WAIT_UPSTREAM",
		7:  "EVENT_TYPE_JOB_IN_PROGRESS",
		8:  "EVENT_TYPE_JOB_SUCCESS",
		9:  "EVENT_TYPE_JOB_FAILURE",
		10: "EVENT_TYPE_JOB_STATE_CHANGE",
	}
	OptimusChangeEvent_EventType_value = map[string]int32{
		"EVENT_TYPE_TYPE_UNSPECIFIED":  0,
		"EVENT_TYPE_RESOURCE_CREATE":   1,
		"EVENT_TYPE_RESOURCE_UPDATE":   2,
		"EVENT_TYPE_RESOURCE_DELETE":   11,
		"EVENT_TYPE_JOB_CREATE":        3,
		"EVENT_TYPE_JOB_UPDATE":        4,
		"EVENT_TYPE_JOB_DELETE":        5,
		"EVENT_TYPE_JOB_WAIT_UPSTREAM": 6,
		"EVENT_TYPE_JOB_IN_PROGRESS":   7,
		"EVENT_TYPE_JOB_SUCCESS":       8,
		"EVENT_TYPE_JOB_FAILURE":       9,
		"EVENT_TYPE_JOB_STATE_CHANGE":  10,
	}
)

func (x OptimusChangeEvent_EventType) Enum() *OptimusChangeEvent_EventType {
	p := new(OptimusChangeEvent_EventType)
	*p = x
	return p
}

func (x OptimusChangeEvent_EventType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (OptimusChangeEvent_EventType) Descriptor() protoreflect.EnumDescriptor {
	return file_gotocompany_optimus_integration_v1beta1_event_proto_enumTypes[1].Descriptor()
}

func (OptimusChangeEvent_EventType) Type() protoreflect.EnumType {
	return &file_gotocompany_optimus_integration_v1beta1_event_proto_enumTypes[1]
}

func (x OptimusChangeEvent_EventType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use OptimusChangeEvent_EventType.Descriptor instead.
func (OptimusChangeEvent_EventType) EnumDescriptor() ([]byte, []int) {
	return file_gotocompany_optimus_integration_v1beta1_event_proto_rawDescGZIP(), []int{4, 0}
}

type ResourceChangePayload struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	DatastoreName string                         `protobuf:"bytes,1,opt,name=datastore_name,json=datastoreName,proto3" json:"datastore_name,omitempty"`
	Resource      *v1beta1.ResourceSpecification `protobuf:"bytes,2,opt,name=resource,proto3" json:"resource,omitempty"`
	ChangeImpact  ChangeImpact                   `protobuf:"varint,3,opt,name=change_impact,json=changeImpact,proto3,enum=gotocompany.optimus.integration.v1beta1.ChangeImpact" json:"change_impact,omitempty"`
}

func (x *ResourceChangePayload) Reset() {
	*x = ResourceChangePayload{}
	if protoimpl.UnsafeEnabled {
		mi := &file_gotocompany_optimus_integration_v1beta1_event_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ResourceChangePayload) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ResourceChangePayload) ProtoMessage() {}

func (x *ResourceChangePayload) ProtoReflect() protoreflect.Message {
	mi := &file_gotocompany_optimus_integration_v1beta1_event_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ResourceChangePayload.ProtoReflect.Descriptor instead.
func (*ResourceChangePayload) Descriptor() ([]byte, []int) {
	return file_gotocompany_optimus_integration_v1beta1_event_proto_rawDescGZIP(), []int{0}
}

func (x *ResourceChangePayload) GetDatastoreName() string {
	if x != nil {
		return x.DatastoreName
	}
	return ""
}

func (x *ResourceChangePayload) GetResource() *v1beta1.ResourceSpecification {
	if x != nil {
		return x.Resource
	}
	return nil
}

func (x *ResourceChangePayload) GetChangeImpact() ChangeImpact {
	if x != nil {
		return x.ChangeImpact
	}
	return ChangeImpact_CHANGE_IMPACT_TYPE_UNSPECIFIED
}

type JobChangePayload struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	JobName      string                    `protobuf:"bytes,1,opt,name=job_name,json=jobName,proto3" json:"job_name,omitempty"`
	JobSpec      *v1beta1.JobSpecification `protobuf:"bytes,2,opt,name=job_spec,json=jobSpec,proto3" json:"job_spec,omitempty"`
	ChangeImpact ChangeImpact              `protobuf:"varint,3,opt,name=change_impact,json=changeImpact,proto3,enum=gotocompany.optimus.integration.v1beta1.ChangeImpact" json:"change_impact,omitempty"`
}

func (x *JobChangePayload) Reset() {
	*x = JobChangePayload{}
	if protoimpl.UnsafeEnabled {
		mi := &file_gotocompany_optimus_integration_v1beta1_event_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *JobChangePayload) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*JobChangePayload) ProtoMessage() {}

func (x *JobChangePayload) ProtoReflect() protoreflect.Message {
	mi := &file_gotocompany_optimus_integration_v1beta1_event_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use JobChangePayload.ProtoReflect.Descriptor instead.
func (*JobChangePayload) Descriptor() ([]byte, []int) {
	return file_gotocompany_optimus_integration_v1beta1_event_proto_rawDescGZIP(), []int{1}
}

func (x *JobChangePayload) GetJobName() string {
	if x != nil {
		return x.JobName
	}
	return ""
}

func (x *JobChangePayload) GetJobSpec() *v1beta1.JobSpecification {
	if x != nil {
		return x.JobSpec
	}
	return nil
}

func (x *JobChangePayload) GetChangeImpact() ChangeImpact {
	if x != nil {
		return x.ChangeImpact
	}
	return ChangeImpact_CHANGE_IMPACT_TYPE_UNSPECIFIED
}

type JobRunPayload struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	JobName     string                 `protobuf:"bytes,1,opt,name=job_name,json=jobName,proto3" json:"job_name,omitempty"`
	ScheduledAt *timestamppb.Timestamp `protobuf:"bytes,2,opt,name=scheduled_at,json=scheduledAt,proto3" json:"scheduled_at,omitempty"`
	JobRunId    string                 `protobuf:"bytes,3,opt,name=job_run_id,json=jobRunId,proto3" json:"job_run_id,omitempty"`
	StartTime   *timestamppb.Timestamp `protobuf:"bytes,4,opt,name=start_time,json=startTime,proto3" json:"start_time,omitempty"`
}

func (x *JobRunPayload) Reset() {
	*x = JobRunPayload{}
	if protoimpl.UnsafeEnabled {
		mi := &file_gotocompany_optimus_integration_v1beta1_event_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *JobRunPayload) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*JobRunPayload) ProtoMessage() {}

func (x *JobRunPayload) ProtoReflect() protoreflect.Message {
	mi := &file_gotocompany_optimus_integration_v1beta1_event_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use JobRunPayload.ProtoReflect.Descriptor instead.
func (*JobRunPayload) Descriptor() ([]byte, []int) {
	return file_gotocompany_optimus_integration_v1beta1_event_proto_rawDescGZIP(), []int{2}
}

func (x *JobRunPayload) GetJobName() string {
	if x != nil {
		return x.JobName
	}
	return ""
}

func (x *JobRunPayload) GetScheduledAt() *timestamppb.Timestamp {
	if x != nil {
		return x.ScheduledAt
	}
	return nil
}

func (x *JobRunPayload) GetJobRunId() string {
	if x != nil {
		return x.JobRunId
	}
	return ""
}

func (x *JobRunPayload) GetStartTime() *timestamppb.Timestamp {
	if x != nil {
		return x.StartTime
	}
	return nil
}

type JobStateChangePayload struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	JobName string           `protobuf:"bytes,1,opt,name=job_name,json=jobName,proto3" json:"job_name,omitempty"`
	State   v1beta1.JobState `protobuf:"varint,2,opt,name=state,proto3,enum=gotocompany.optimus.core.v1beta1.JobState" json:"state,omitempty"`
}

func (x *JobStateChangePayload) Reset() {
	*x = JobStateChangePayload{}
	if protoimpl.UnsafeEnabled {
		mi := &file_gotocompany_optimus_integration_v1beta1_event_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *JobStateChangePayload) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*JobStateChangePayload) ProtoMessage() {}

func (x *JobStateChangePayload) ProtoReflect() protoreflect.Message {
	mi := &file_gotocompany_optimus_integration_v1beta1_event_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use JobStateChangePayload.ProtoReflect.Descriptor instead.
func (*JobStateChangePayload) Descriptor() ([]byte, []int) {
	return file_gotocompany_optimus_integration_v1beta1_event_proto_rawDescGZIP(), []int{3}
}

func (x *JobStateChangePayload) GetJobName() string {
	if x != nil {
		return x.JobName
	}
	return ""
}

func (x *JobStateChangePayload) GetState() v1beta1.JobState {
	if x != nil {
		return x.State
	}
	return v1beta1.JobState(0)
}

type OptimusChangeEvent struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	EventId       string                       `protobuf:"bytes,1,opt,name=event_id,json=eventId,proto3" json:"event_id,omitempty"`
	OccurredAt    *timestamppb.Timestamp       `protobuf:"bytes,2,opt,name=occurred_at,json=occurredAt,proto3" json:"occurred_at,omitempty"`
	ProjectName   string                       `protobuf:"bytes,3,opt,name=project_name,json=projectName,proto3" json:"project_name,omitempty"`
	NamespaceName string                       `protobuf:"bytes,4,opt,name=namespace_name,json=namespaceName,proto3" json:"namespace_name,omitempty"`
	EventType     OptimusChangeEvent_EventType `protobuf:"varint,5,opt,name=event_type,json=eventType,proto3,enum=gotocompany.optimus.integration.v1beta1.OptimusChangeEvent_EventType" json:"event_type,omitempty"`
	// Types that are assignable to Payload:
	//
	//	*OptimusChangeEvent_JobChange
	//	*OptimusChangeEvent_ResourceChange
	//	*OptimusChangeEvent_JobRun
	//	*OptimusChangeEvent_JobStateChange
	Payload isOptimusChangeEvent_Payload `protobuf_oneof:"payload"`
	JobName string                       `protobuf:"bytes,10,opt,name=job_name,json=jobName,proto3" json:"job_name,omitempty"`
}

func (x *OptimusChangeEvent) Reset() {
	*x = OptimusChangeEvent{}
	if protoimpl.UnsafeEnabled {
		mi := &file_gotocompany_optimus_integration_v1beta1_event_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *OptimusChangeEvent) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*OptimusChangeEvent) ProtoMessage() {}

func (x *OptimusChangeEvent) ProtoReflect() protoreflect.Message {
	mi := &file_gotocompany_optimus_integration_v1beta1_event_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use OptimusChangeEvent.ProtoReflect.Descriptor instead.
func (*OptimusChangeEvent) Descriptor() ([]byte, []int) {
	return file_gotocompany_optimus_integration_v1beta1_event_proto_rawDescGZIP(), []int{4}
}

func (x *OptimusChangeEvent) GetEventId() string {
	if x != nil {
		return x.EventId
	}
	return ""
}

func (x *OptimusChangeEvent) GetOccurredAt() *timestamppb.Timestamp {
	if x != nil {
		return x.OccurredAt
	}
	return nil
}

func (x *OptimusChangeEvent) GetProjectName() string {
	if x != nil {
		return x.ProjectName
	}
	return ""
}

func (x *OptimusChangeEvent) GetNamespaceName() string {
	if x != nil {
		return x.NamespaceName
	}
	return ""
}

func (x *OptimusChangeEvent) GetEventType() OptimusChangeEvent_EventType {
	if x != nil {
		return x.EventType
	}
	return OptimusChangeEvent_EVENT_TYPE_TYPE_UNSPECIFIED
}

func (m *OptimusChangeEvent) GetPayload() isOptimusChangeEvent_Payload {
	if m != nil {
		return m.Payload
	}
	return nil
}

func (x *OptimusChangeEvent) GetJobChange() *JobChangePayload {
	if x, ok := x.GetPayload().(*OptimusChangeEvent_JobChange); ok {
		return x.JobChange
	}
	return nil
}

func (x *OptimusChangeEvent) GetResourceChange() *ResourceChangePayload {
	if x, ok := x.GetPayload().(*OptimusChangeEvent_ResourceChange); ok {
		return x.ResourceChange
	}
	return nil
}

func (x *OptimusChangeEvent) GetJobRun() *JobRunPayload {
	if x, ok := x.GetPayload().(*OptimusChangeEvent_JobRun); ok {
		return x.JobRun
	}
	return nil
}

func (x *OptimusChangeEvent) GetJobStateChange() *JobStateChangePayload {
	if x, ok := x.GetPayload().(*OptimusChangeEvent_JobStateChange); ok {
		return x.JobStateChange
	}
	return nil
}

func (x *OptimusChangeEvent) GetJobName() string {
	if x != nil {
		return x.JobName
	}
	return ""
}

type isOptimusChangeEvent_Payload interface {
	isOptimusChangeEvent_Payload()
}

type OptimusChangeEvent_JobChange struct {
	JobChange *JobChangePayload `protobuf:"bytes,6,opt,name=job_change,json=jobChange,proto3,oneof"`
}

type OptimusChangeEvent_ResourceChange struct {
	ResourceChange *ResourceChangePayload `protobuf:"bytes,7,opt,name=resource_change,json=resourceChange,proto3,oneof"`
}

type OptimusChangeEvent_JobRun struct {
	JobRun *JobRunPayload `protobuf:"bytes,8,opt,name=job_run,json=jobRun,proto3,oneof"`
}

type OptimusChangeEvent_JobStateChange struct {
	JobStateChange *JobStateChangePayload `protobuf:"bytes,9,opt,name=job_state_change,json=jobStateChange,proto3,oneof"`
}

func (*OptimusChangeEvent_JobChange) isOptimusChangeEvent_Payload() {}

func (*OptimusChangeEvent_ResourceChange) isOptimusChangeEvent_Payload() {}

func (*OptimusChangeEvent_JobRun) isOptimusChangeEvent_Payload() {}

func (*OptimusChangeEvent_JobStateChange) isOptimusChangeEvent_Payload() {}

var File_gotocompany_optimus_integration_v1beta1_event_proto protoreflect.FileDescriptor

var file_gotocompany_optimus_integration_v1beta1_event_proto_rawDesc = []byte{
	0x0a, 0x33, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2f, 0x6f, 0x70,
	0x74, 0x69, 0x6d, 0x75, 0x73, 0x2f, 0x69, 0x6e, 0x74, 0x65, 0x67, 0x72, 0x61, 0x74, 0x69, 0x6f,
	0x6e, 0x2f, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2f, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x27, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61,
	0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x69, 0x6e, 0x74, 0x65, 0x67,
	0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x1a, 0x1f,
	0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f,
	0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a,
	0x2f, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2f, 0x6f, 0x70, 0x74,
	0x69, 0x6d, 0x75, 0x73, 0x2f, 0x63, 0x6f, 0x72, 0x65, 0x2f, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61,
	0x31, 0x2f, 0x72, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x1a, 0x2f, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2f, 0x6f, 0x70,
	0x74, 0x69, 0x6d, 0x75, 0x73, 0x2f, 0x63, 0x6f, 0x72, 0x65, 0x2f, 0x76, 0x31, 0x62, 0x65, 0x74,
	0x61, 0x31, 0x2f, 0x6a, 0x6f, 0x62, 0x5f, 0x73, 0x70, 0x65, 0x63, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x22, 0xef, 0x01, 0x0a, 0x15, 0x52, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x43, 0x68,
	0x61, 0x6e, 0x67, 0x65, 0x50, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x12, 0x25, 0x0a, 0x0e, 0x64,
	0x61, 0x74, 0x61, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x0d, 0x64, 0x61, 0x74, 0x61, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x4e, 0x61,
	0x6d, 0x65, 0x12, 0x53, 0x0a, 0x08, 0x72, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x37, 0x2e, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61,
	0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e,
	0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x52, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65,
	0x53, 0x70, 0x65, 0x63, 0x69, 0x66, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x08, 0x72,
	0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x12, 0x5a, 0x0a, 0x0d, 0x63, 0x68, 0x61, 0x6e, 0x67,
	0x65, 0x5f, 0x69, 0x6d, 0x70, 0x61, 0x63, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x35,
	0x2e, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74,
	0x69, 0x6d, 0x75, 0x73, 0x2e, 0x69, 0x6e, 0x74, 0x65, 0x67, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e,
	0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x49,
	0x6d, 0x70, 0x61, 0x63, 0x74, 0x52, 0x0c, 0x63, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x49, 0x6d, 0x70,
	0x61, 0x63, 0x74, 0x22, 0xd8, 0x01, 0x0a, 0x10, 0x4a, 0x6f, 0x62, 0x43, 0x68, 0x61, 0x6e, 0x67,
	0x65, 0x50, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x12, 0x19, 0x0a, 0x08, 0x6a, 0x6f, 0x62, 0x5f,
	0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x6a, 0x6f, 0x62, 0x4e,
	0x61, 0x6d, 0x65, 0x12, 0x4d, 0x0a, 0x08, 0x6a, 0x6f, 0x62, 0x5f, 0x73, 0x70, 0x65, 0x63, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x32, 0x2e, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70,
	0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65,
	0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x4a, 0x6f, 0x62, 0x53, 0x70, 0x65, 0x63,
	0x69, 0x66, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x07, 0x6a, 0x6f, 0x62, 0x53, 0x70,
	0x65, 0x63, 0x12, 0x5a, 0x0a, 0x0d, 0x63, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x5f, 0x69, 0x6d, 0x70,
	0x61, 0x63, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x35, 0x2e, 0x67, 0x6f, 0x74, 0x6f,
	0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e,
	0x69, 0x6e, 0x74, 0x65, 0x67, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x62, 0x65,
	0x74, 0x61, 0x31, 0x2e, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x49, 0x6d, 0x70, 0x61, 0x63, 0x74,
	0x52, 0x0c, 0x63, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x49, 0x6d, 0x70, 0x61, 0x63, 0x74, 0x22, 0xc2,
	0x01, 0x0a, 0x0d, 0x4a, 0x6f, 0x62, 0x52, 0x75, 0x6e, 0x50, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64,
	0x12, 0x19, 0x0a, 0x08, 0x6a, 0x6f, 0x62, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x07, 0x6a, 0x6f, 0x62, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x3d, 0x0a, 0x0c, 0x73,
	0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x64, 0x5f, 0x61, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x0b, 0x73,
	0x63, 0x68, 0x65, 0x64, 0x75, 0x6c, 0x65, 0x64, 0x41, 0x74, 0x12, 0x1c, 0x0a, 0x0a, 0x6a, 0x6f,
	0x62, 0x5f, 0x72, 0x75, 0x6e, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08,
	0x6a, 0x6f, 0x62, 0x52, 0x75, 0x6e, 0x49, 0x64, 0x12, 0x39, 0x0a, 0x0a, 0x73, 0x74, 0x61, 0x72,
	0x74, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67,
	0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54,
	0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x09, 0x73, 0x74, 0x61, 0x72, 0x74, 0x54,
	0x69, 0x6d, 0x65, 0x22, 0x74, 0x0a, 0x15, 0x4a, 0x6f, 0x62, 0x53, 0x74, 0x61, 0x74, 0x65, 0x43,
	0x68, 0x61, 0x6e, 0x67, 0x65, 0x50, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x12, 0x19, 0x0a, 0x08,
	0x6a, 0x6f, 0x62, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07,
	0x6a, 0x6f, 0x62, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x40, 0x0a, 0x05, 0x73, 0x74, 0x61, 0x74, 0x65,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x2a, 0x2e, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d,
	0x70, 0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72,
	0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x4a, 0x6f, 0x62, 0x53, 0x74, 0x61,
	0x74, 0x65, 0x52, 0x05, 0x73, 0x74, 0x61, 0x74, 0x65, 0x22, 0xc3, 0x08, 0x0a, 0x12, 0x4f, 0x70,
	0x74, 0x69, 0x6d, 0x75, 0x73, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x45, 0x76, 0x65, 0x6e, 0x74,
	0x12, 0x19, 0x0a, 0x08, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x07, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x49, 0x64, 0x12, 0x3b, 0x0a, 0x0b, 0x6f,
	0x63, 0x63, 0x75, 0x72, 0x72, 0x65, 0x64, 0x5f, 0x61, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62,
	0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x0a, 0x6f, 0x63,
	0x63, 0x75, 0x72, 0x72, 0x65, 0x64, 0x41, 0x74, 0x12, 0x21, 0x0a, 0x0c, 0x70, 0x72, 0x6f, 0x6a,
	0x65, 0x63, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b,
	0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x25, 0x0a, 0x0e, 0x6e,
	0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x04, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x0d, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x4e, 0x61,
	0x6d, 0x65, 0x12, 0x64, 0x0a, 0x0a, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x5f, 0x74, 0x79, 0x70, 0x65,
	0x18, 0x05, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x45, 0x2e, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d,
	0x70, 0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x69, 0x6e, 0x74,
	0x65, 0x67, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31,
	0x2e, 0x4f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x45, 0x76,
	0x65, 0x6e, 0x74, 0x2e, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x54, 0x79, 0x70, 0x65, 0x52, 0x09, 0x65,
	0x76, 0x65, 0x6e, 0x74, 0x54, 0x79, 0x70, 0x65, 0x12, 0x5a, 0x0a, 0x0a, 0x6a, 0x6f, 0x62, 0x5f,
	0x63, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x18, 0x06, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x39, 0x2e, 0x67,
	0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d,
	0x75, 0x73, 0x2e, 0x69, 0x6e, 0x74, 0x65, 0x67, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x76,
	0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x4a, 0x6f, 0x62, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65,
	0x50, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x48, 0x00, 0x52, 0x09, 0x6a, 0x6f, 0x62, 0x43, 0x68,
	0x61, 0x6e, 0x67, 0x65, 0x12, 0x69, 0x0a, 0x0f, 0x72, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65,
	0x5f, 0x63, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x18, 0x07, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x3e, 0x2e,
	0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69,
	0x6d, 0x75, 0x73, 0x2e, 0x69, 0x6e, 0x74, 0x65, 0x67, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e,
	0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x52, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65,
	0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x50, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x48, 0x00, 0x52,
	0x0e, 0x72, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x12,
	0x51, 0x0a, 0x07, 0x6a, 0x6f, 0x62, 0x5f, 0x72, 0x75, 0x6e, 0x18, 0x08, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x36, 0x2e, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x6f,
	0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x69, 0x6e, 0x74, 0x65, 0x67, 0x72, 0x61, 0x74, 0x69,
	0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x4a, 0x6f, 0x62, 0x52, 0x75,
	0x6e, 0x50, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x48, 0x00, 0x52, 0x06, 0x6a, 0x6f, 0x62, 0x52,
	0x75, 0x6e, 0x12, 0x6a, 0x0a, 0x10, 0x6a, 0x6f, 0x62, 0x5f, 0x73, 0x74, 0x61, 0x74, 0x65, 0x5f,
	0x63, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x18, 0x09, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x3e, 0x2e, 0x67,
	0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d,
	0x75, 0x73, 0x2e, 0x69, 0x6e, 0x74, 0x65, 0x67, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x76,
	0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x4a, 0x6f, 0x62, 0x53, 0x74, 0x61, 0x74, 0x65, 0x43,
	0x68, 0x61, 0x6e, 0x67, 0x65, 0x50, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x48, 0x00, 0x52, 0x0e,
	0x6a, 0x6f, 0x62, 0x53, 0x74, 0x61, 0x74, 0x65, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x12, 0x19,
	0x0a, 0x08, 0x6a, 0x6f, 0x62, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x07, 0x6a, 0x6f, 0x62, 0x4e, 0x61, 0x6d, 0x65, 0x22, 0xf8, 0x02, 0x0a, 0x09, 0x45, 0x76,
	0x65, 0x6e, 0x74, 0x54, 0x79, 0x70, 0x65, 0x12, 0x1f, 0x0a, 0x1b, 0x45, 0x56, 0x45, 0x4e, 0x54,
	0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x55, 0x4e, 0x53, 0x50, 0x45,
	0x43, 0x49, 0x46, 0x49, 0x45, 0x44, 0x10, 0x00, 0x12, 0x1e, 0x0a, 0x1a, 0x45, 0x56, 0x45, 0x4e,
	0x54, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x52, 0x45, 0x53, 0x4f, 0x55, 0x52, 0x43, 0x45, 0x5f,
	0x43, 0x52, 0x45, 0x41, 0x54, 0x45, 0x10, 0x01, 0x12, 0x1e, 0x0a, 0x1a, 0x45, 0x56, 0x45, 0x4e,
	0x54, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x52, 0x45, 0x53, 0x4f, 0x55, 0x52, 0x43, 0x45, 0x5f,
	0x55, 0x50, 0x44, 0x41, 0x54, 0x45, 0x10, 0x02, 0x12, 0x1e, 0x0a, 0x1a, 0x45, 0x56, 0x45, 0x4e,
	0x54, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x52, 0x45, 0x53, 0x4f, 0x55, 0x52, 0x43, 0x45, 0x5f,
	0x44, 0x45, 0x4c, 0x45, 0x54, 0x45, 0x10, 0x0b, 0x12, 0x19, 0x0a, 0x15, 0x45, 0x56, 0x45, 0x4e,
	0x54, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x4a, 0x4f, 0x42, 0x5f, 0x43, 0x52, 0x45, 0x41, 0x54,
	0x45, 0x10, 0x03, 0x12, 0x19, 0x0a, 0x15, 0x45, 0x56, 0x45, 0x4e, 0x54, 0x5f, 0x54, 0x59, 0x50,
	0x45, 0x5f, 0x4a, 0x4f, 0x42, 0x5f, 0x55, 0x50, 0x44, 0x41, 0x54, 0x45, 0x10, 0x04, 0x12, 0x19,
	0x0a, 0x15, 0x45, 0x56, 0x45, 0x4e, 0x54, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x4a, 0x4f, 0x42,
	0x5f, 0x44, 0x45, 0x4c, 0x45, 0x54, 0x45, 0x10, 0x05, 0x12, 0x20, 0x0a, 0x1c, 0x45, 0x56, 0x45,
	0x4e, 0x54, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x4a, 0x4f, 0x42, 0x5f, 0x57, 0x41, 0x49, 0x54,
	0x5f, 0x55, 0x50, 0x53, 0x54, 0x52, 0x45, 0x41, 0x4d, 0x10, 0x06, 0x12, 0x1e, 0x0a, 0x1a, 0x45,
	0x56, 0x45, 0x4e, 0x54, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x4a, 0x4f, 0x42, 0x5f, 0x49, 0x4e,
	0x5f, 0x50, 0x52, 0x4f, 0x47, 0x52, 0x45, 0x53, 0x53, 0x10, 0x07, 0x12, 0x1a, 0x0a, 0x16, 0x45,
	0x56, 0x45, 0x4e, 0x54, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x4a, 0x4f, 0x42, 0x5f, 0x53, 0x55,
	0x43, 0x43, 0x45, 0x53, 0x53, 0x10, 0x08, 0x12, 0x1a, 0x0a, 0x16, 0x45, 0x56, 0x45, 0x4e, 0x54,
	0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x4a, 0x4f, 0x42, 0x5f, 0x46, 0x41, 0x49, 0x4c, 0x55, 0x52,
	0x45, 0x10, 0x09, 0x12, 0x1f, 0x0a, 0x1b, 0x45, 0x56, 0x45, 0x4e, 0x54, 0x5f, 0x54, 0x59, 0x50,
	0x45, 0x5f, 0x4a, 0x4f, 0x42, 0x5f, 0x53, 0x54, 0x41, 0x54, 0x45, 0x5f, 0x43, 0x48, 0x41, 0x4e,
	0x47, 0x45, 0x10, 0x0a, 0x42, 0x09, 0x0a, 0x07, 0x70, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x2a,
	0x75, 0x0a, 0x0c, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x49, 0x6d, 0x70, 0x61, 0x63, 0x74, 0x12,
	0x22, 0x0a, 0x1e, 0x43, 0x48, 0x41, 0x4e, 0x47, 0x45, 0x5f, 0x49, 0x4d, 0x50, 0x41, 0x43, 0x54,
	0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x55, 0x4e, 0x53, 0x50, 0x45, 0x43, 0x49, 0x46, 0x49, 0x45,
	0x44, 0x10, 0x00, 0x12, 0x1f, 0x0a, 0x1b, 0x43, 0x48, 0x41, 0x4e, 0x47, 0x45, 0x5f, 0x49, 0x4d,
	0x50, 0x41, 0x43, 0x54, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x49, 0x4e, 0x54, 0x45, 0x52, 0x4e,
	0x41, 0x4c, 0x10, 0x01, 0x12, 0x20, 0x0a, 0x1c, 0x43, 0x48, 0x41, 0x4e, 0x47, 0x45, 0x5f, 0x49,
	0x4d, 0x50, 0x41, 0x43, 0x54, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x42, 0x45, 0x48, 0x41, 0x56,
	0x49, 0x4f, 0x55, 0x52, 0x10, 0x02, 0x42, 0x49, 0x0a, 0x1e, 0x63, 0x6f, 0x6d, 0x2e, 0x67, 0x6f,
	0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x6e,
	0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x42, 0x05, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x50,
	0x01, 0x5a, 0x1e, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x67, 0x6f,
	0x74, 0x6f, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x6e, 0x2f, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75,
	0x73, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_gotocompany_optimus_integration_v1beta1_event_proto_rawDescOnce sync.Once
	file_gotocompany_optimus_integration_v1beta1_event_proto_rawDescData = file_gotocompany_optimus_integration_v1beta1_event_proto_rawDesc
)

func file_gotocompany_optimus_integration_v1beta1_event_proto_rawDescGZIP() []byte {
	file_gotocompany_optimus_integration_v1beta1_event_proto_rawDescOnce.Do(func() {
		file_gotocompany_optimus_integration_v1beta1_event_proto_rawDescData = protoimpl.X.CompressGZIP(file_gotocompany_optimus_integration_v1beta1_event_proto_rawDescData)
	})
	return file_gotocompany_optimus_integration_v1beta1_event_proto_rawDescData
}

var file_gotocompany_optimus_integration_v1beta1_event_proto_enumTypes = make([]protoimpl.EnumInfo, 2)
var file_gotocompany_optimus_integration_v1beta1_event_proto_msgTypes = make([]protoimpl.MessageInfo, 5)
var file_gotocompany_optimus_integration_v1beta1_event_proto_goTypes = []interface{}{
	(ChangeImpact)(0),                     // 0: gotocompany.optimus.integration.v1beta1.ChangeImpact
	(OptimusChangeEvent_EventType)(0),     // 1: gotocompany.optimus.integration.v1beta1.OptimusChangeEvent.EventType
	(*ResourceChangePayload)(nil),         // 2: gotocompany.optimus.integration.v1beta1.ResourceChangePayload
	(*JobChangePayload)(nil),              // 3: gotocompany.optimus.integration.v1beta1.JobChangePayload
	(*JobRunPayload)(nil),                 // 4: gotocompany.optimus.integration.v1beta1.JobRunPayload
	(*JobStateChangePayload)(nil),         // 5: gotocompany.optimus.integration.v1beta1.JobStateChangePayload
	(*OptimusChangeEvent)(nil),            // 6: gotocompany.optimus.integration.v1beta1.OptimusChangeEvent
	(*v1beta1.ResourceSpecification)(nil), // 7: gotocompany.optimus.core.v1beta1.ResourceSpecification
	(*v1beta1.JobSpecification)(nil),      // 8: gotocompany.optimus.core.v1beta1.JobSpecification
	(*timestamppb.Timestamp)(nil),         // 9: google.protobuf.Timestamp
	(v1beta1.JobState)(0),                 // 10: gotocompany.optimus.core.v1beta1.JobState
}
var file_gotocompany_optimus_integration_v1beta1_event_proto_depIdxs = []int32{
	7,  // 0: gotocompany.optimus.integration.v1beta1.ResourceChangePayload.resource:type_name -> gotocompany.optimus.core.v1beta1.ResourceSpecification
	0,  // 1: gotocompany.optimus.integration.v1beta1.ResourceChangePayload.change_impact:type_name -> gotocompany.optimus.integration.v1beta1.ChangeImpact
	8,  // 2: gotocompany.optimus.integration.v1beta1.JobChangePayload.job_spec:type_name -> gotocompany.optimus.core.v1beta1.JobSpecification
	0,  // 3: gotocompany.optimus.integration.v1beta1.JobChangePayload.change_impact:type_name -> gotocompany.optimus.integration.v1beta1.ChangeImpact
	9,  // 4: gotocompany.optimus.integration.v1beta1.JobRunPayload.scheduled_at:type_name -> google.protobuf.Timestamp
	9,  // 5: gotocompany.optimus.integration.v1beta1.JobRunPayload.start_time:type_name -> google.protobuf.Timestamp
	10, // 6: gotocompany.optimus.integration.v1beta1.JobStateChangePayload.state:type_name -> gotocompany.optimus.core.v1beta1.JobState
	9,  // 7: gotocompany.optimus.integration.v1beta1.OptimusChangeEvent.occurred_at:type_name -> google.protobuf.Timestamp
	1,  // 8: gotocompany.optimus.integration.v1beta1.OptimusChangeEvent.event_type:type_name -> gotocompany.optimus.integration.v1beta1.OptimusChangeEvent.EventType
	3,  // 9: gotocompany.optimus.integration.v1beta1.OptimusChangeEvent.job_change:type_name -> gotocompany.optimus.integration.v1beta1.JobChangePayload
	2,  // 10: gotocompany.optimus.integration.v1beta1.OptimusChangeEvent.resource_change:type_name -> gotocompany.optimus.integration.v1beta1.ResourceChangePayload
	4,  // 11: gotocompany.optimus.integration.v1beta1.OptimusChangeEvent.job_run:type_name -> gotocompany.optimus.integration.v1beta1.JobRunPayload
	5,  // 12: gotocompany.optimus.integration.v1beta1.OptimusChangeEvent.job_state_change:type_name -> gotocompany.optimus.integration.v1beta1.JobStateChangePayload
	13, // [13:13] is the sub-list for method output_type
	13, // [13:13] is the sub-list for method input_type
	13, // [13:13] is the sub-list for extension type_name
	13, // [13:13] is the sub-list for extension extendee
	0,  // [0:13] is the sub-list for field type_name
}

func init() { file_gotocompany_optimus_integration_v1beta1_event_proto_init() }
func file_gotocompany_optimus_integration_v1beta1_event_proto_init() {
	if File_gotocompany_optimus_integration_v1beta1_event_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_gotocompany_optimus_integration_v1beta1_event_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ResourceChangePayload); i {
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
		file_gotocompany_optimus_integration_v1beta1_event_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*JobChangePayload); i {
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
		file_gotocompany_optimus_integration_v1beta1_event_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*JobRunPayload); i {
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
		file_gotocompany_optimus_integration_v1beta1_event_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*JobStateChangePayload); i {
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
		file_gotocompany_optimus_integration_v1beta1_event_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*OptimusChangeEvent); i {
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
	file_gotocompany_optimus_integration_v1beta1_event_proto_msgTypes[4].OneofWrappers = []interface{}{
		(*OptimusChangeEvent_JobChange)(nil),
		(*OptimusChangeEvent_ResourceChange)(nil),
		(*OptimusChangeEvent_JobRun)(nil),
		(*OptimusChangeEvent_JobStateChange)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_gotocompany_optimus_integration_v1beta1_event_proto_rawDesc,
			NumEnums:      2,
			NumMessages:   5,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_gotocompany_optimus_integration_v1beta1_event_proto_goTypes,
		DependencyIndexes: file_gotocompany_optimus_integration_v1beta1_event_proto_depIdxs,
		EnumInfos:         file_gotocompany_optimus_integration_v1beta1_event_proto_enumTypes,
		MessageInfos:      file_gotocompany_optimus_integration_v1beta1_event_proto_msgTypes,
	}.Build()
	File_gotocompany_optimus_integration_v1beta1_event_proto = out.File
	file_gotocompany_optimus_integration_v1beta1_event_proto_rawDesc = nil
	file_gotocompany_optimus_integration_v1beta1_event_proto_goTypes = nil
	file_gotocompany_optimus_integration_v1beta1_event_proto_depIdxs = nil
}
