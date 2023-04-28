// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        (unknown)
// source: gotocompany/optimus/core/v1beta1/backup.proto

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

type IgnoredResource struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name   string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Reason string `protobuf:"bytes,2,opt,name=reason,proto3" json:"reason,omitempty"`
}

func (x *IgnoredResource) Reset() {
	*x = IgnoredResource{}
	if protoimpl.UnsafeEnabled {
		mi := &file_gotocompany_optimus_core_v1beta1_backup_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *IgnoredResource) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*IgnoredResource) ProtoMessage() {}

func (x *IgnoredResource) ProtoReflect() protoreflect.Message {
	mi := &file_gotocompany_optimus_core_v1beta1_backup_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use IgnoredResource.ProtoReflect.Descriptor instead.
func (*IgnoredResource) Descriptor() ([]byte, []int) {
	return file_gotocompany_optimus_core_v1beta1_backup_proto_rawDescGZIP(), []int{0}
}

func (x *IgnoredResource) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *IgnoredResource) GetReason() string {
	if x != nil {
		return x.Reason
	}
	return ""
}

type CreateBackupRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ProjectName   string            `protobuf:"bytes,1,opt,name=project_name,json=projectName,proto3" json:"project_name,omitempty"`
	DatastoreName string            `protobuf:"bytes,2,opt,name=datastore_name,json=datastoreName,proto3" json:"datastore_name,omitempty"`
	NamespaceName string            `protobuf:"bytes,4,opt,name=namespace_name,json=namespaceName,proto3" json:"namespace_name,omitempty"`
	Description   string            `protobuf:"bytes,5,opt,name=description,proto3" json:"description,omitempty"`
	Config        map[string]string `protobuf:"bytes,7,rep,name=config,proto3" json:"config,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	ResourceNames []string          `protobuf:"bytes,9,rep,name=resource_names,json=resourceNames,proto3" json:"resource_names,omitempty"`
}

func (x *CreateBackupRequest) Reset() {
	*x = CreateBackupRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_gotocompany_optimus_core_v1beta1_backup_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CreateBackupRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CreateBackupRequest) ProtoMessage() {}

func (x *CreateBackupRequest) ProtoReflect() protoreflect.Message {
	mi := &file_gotocompany_optimus_core_v1beta1_backup_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CreateBackupRequest.ProtoReflect.Descriptor instead.
func (*CreateBackupRequest) Descriptor() ([]byte, []int) {
	return file_gotocompany_optimus_core_v1beta1_backup_proto_rawDescGZIP(), []int{1}
}

func (x *CreateBackupRequest) GetProjectName() string {
	if x != nil {
		return x.ProjectName
	}
	return ""
}

func (x *CreateBackupRequest) GetDatastoreName() string {
	if x != nil {
		return x.DatastoreName
	}
	return ""
}

func (x *CreateBackupRequest) GetNamespaceName() string {
	if x != nil {
		return x.NamespaceName
	}
	return ""
}

func (x *CreateBackupRequest) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *CreateBackupRequest) GetConfig() map[string]string {
	if x != nil {
		return x.Config
	}
	return nil
}

func (x *CreateBackupRequest) GetResourceNames() []string {
	if x != nil {
		return x.ResourceNames
	}
	return nil
}

type CreateBackupResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ResourceNames    []string           `protobuf:"bytes,1,rep,name=resource_names,json=resourceNames,proto3" json:"resource_names,omitempty"`
	IgnoredResources []*IgnoredResource `protobuf:"bytes,3,rep,name=ignored_resources,json=ignoredResources,proto3" json:"ignored_resources,omitempty"`
	BackupId         string             `protobuf:"bytes,4,opt,name=backup_id,json=backupId,proto3" json:"backup_id,omitempty"`
}

func (x *CreateBackupResponse) Reset() {
	*x = CreateBackupResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_gotocompany_optimus_core_v1beta1_backup_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CreateBackupResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CreateBackupResponse) ProtoMessage() {}

func (x *CreateBackupResponse) ProtoReflect() protoreflect.Message {
	mi := &file_gotocompany_optimus_core_v1beta1_backup_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CreateBackupResponse.ProtoReflect.Descriptor instead.
func (*CreateBackupResponse) Descriptor() ([]byte, []int) {
	return file_gotocompany_optimus_core_v1beta1_backup_proto_rawDescGZIP(), []int{2}
}

func (x *CreateBackupResponse) GetResourceNames() []string {
	if x != nil {
		return x.ResourceNames
	}
	return nil
}

func (x *CreateBackupResponse) GetIgnoredResources() []*IgnoredResource {
	if x != nil {
		return x.IgnoredResources
	}
	return nil
}

func (x *CreateBackupResponse) GetBackupId() string {
	if x != nil {
		return x.BackupId
	}
	return ""
}

type ListBackupsRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ProjectName   string `protobuf:"bytes,1,opt,name=project_name,json=projectName,proto3" json:"project_name,omitempty"`
	DatastoreName string `protobuf:"bytes,2,opt,name=datastore_name,json=datastoreName,proto3" json:"datastore_name,omitempty"`
	NamespaceName string `protobuf:"bytes,3,opt,name=namespace_name,json=namespaceName,proto3" json:"namespace_name,omitempty"`
}

func (x *ListBackupsRequest) Reset() {
	*x = ListBackupsRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_gotocompany_optimus_core_v1beta1_backup_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListBackupsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListBackupsRequest) ProtoMessage() {}

func (x *ListBackupsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_gotocompany_optimus_core_v1beta1_backup_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListBackupsRequest.ProtoReflect.Descriptor instead.
func (*ListBackupsRequest) Descriptor() ([]byte, []int) {
	return file_gotocompany_optimus_core_v1beta1_backup_proto_rawDescGZIP(), []int{3}
}

func (x *ListBackupsRequest) GetProjectName() string {
	if x != nil {
		return x.ProjectName
	}
	return ""
}

func (x *ListBackupsRequest) GetDatastoreName() string {
	if x != nil {
		return x.DatastoreName
	}
	return ""
}

func (x *ListBackupsRequest) GetNamespaceName() string {
	if x != nil {
		return x.NamespaceName
	}
	return ""
}

type ListBackupsResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Backups []*BackupSpec `protobuf:"bytes,1,rep,name=backups,proto3" json:"backups,omitempty"`
}

func (x *ListBackupsResponse) Reset() {
	*x = ListBackupsResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_gotocompany_optimus_core_v1beta1_backup_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListBackupsResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListBackupsResponse) ProtoMessage() {}

func (x *ListBackupsResponse) ProtoReflect() protoreflect.Message {
	mi := &file_gotocompany_optimus_core_v1beta1_backup_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListBackupsResponse.ProtoReflect.Descriptor instead.
func (*ListBackupsResponse) Descriptor() ([]byte, []int) {
	return file_gotocompany_optimus_core_v1beta1_backup_proto_rawDescGZIP(), []int{4}
}

func (x *ListBackupsResponse) GetBackups() []*BackupSpec {
	if x != nil {
		return x.Backups
	}
	return nil
}

type BackupSpec struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id            string                 `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	CreatedAt     *timestamppb.Timestamp `protobuf:"bytes,3,opt,name=created_at,json=createdAt,proto3" json:"created_at,omitempty"`
	Description   string                 `protobuf:"bytes,4,opt,name=description,proto3" json:"description,omitempty"`
	Config        map[string]string      `protobuf:"bytes,5,rep,name=config,proto3" json:"config,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	ResourceNames []string               `protobuf:"bytes,6,rep,name=resource_names,json=resourceNames,proto3" json:"resource_names,omitempty"`
}

func (x *BackupSpec) Reset() {
	*x = BackupSpec{}
	if protoimpl.UnsafeEnabled {
		mi := &file_gotocompany_optimus_core_v1beta1_backup_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BackupSpec) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BackupSpec) ProtoMessage() {}

func (x *BackupSpec) ProtoReflect() protoreflect.Message {
	mi := &file_gotocompany_optimus_core_v1beta1_backup_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BackupSpec.ProtoReflect.Descriptor instead.
func (*BackupSpec) Descriptor() ([]byte, []int) {
	return file_gotocompany_optimus_core_v1beta1_backup_proto_rawDescGZIP(), []int{5}
}

func (x *BackupSpec) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *BackupSpec) GetCreatedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.CreatedAt
	}
	return nil
}

func (x *BackupSpec) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *BackupSpec) GetConfig() map[string]string {
	if x != nil {
		return x.Config
	}
	return nil
}

func (x *BackupSpec) GetResourceNames() []string {
	if x != nil {
		return x.ResourceNames
	}
	return nil
}

type GetBackupRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ProjectName   string `protobuf:"bytes,1,opt,name=project_name,json=projectName,proto3" json:"project_name,omitempty"`
	DatastoreName string `protobuf:"bytes,2,opt,name=datastore_name,json=datastoreName,proto3" json:"datastore_name,omitempty"`
	NamespaceName string `protobuf:"bytes,3,opt,name=namespace_name,json=namespaceName,proto3" json:"namespace_name,omitempty"`
	Id            string `protobuf:"bytes,4,opt,name=id,proto3" json:"id,omitempty"`
}

func (x *GetBackupRequest) Reset() {
	*x = GetBackupRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_gotocompany_optimus_core_v1beta1_backup_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetBackupRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetBackupRequest) ProtoMessage() {}

func (x *GetBackupRequest) ProtoReflect() protoreflect.Message {
	mi := &file_gotocompany_optimus_core_v1beta1_backup_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetBackupRequest.ProtoReflect.Descriptor instead.
func (*GetBackupRequest) Descriptor() ([]byte, []int) {
	return file_gotocompany_optimus_core_v1beta1_backup_proto_rawDescGZIP(), []int{6}
}

func (x *GetBackupRequest) GetProjectName() string {
	if x != nil {
		return x.ProjectName
	}
	return ""
}

func (x *GetBackupRequest) GetDatastoreName() string {
	if x != nil {
		return x.DatastoreName
	}
	return ""
}

func (x *GetBackupRequest) GetNamespaceName() string {
	if x != nil {
		return x.NamespaceName
	}
	return ""
}

func (x *GetBackupRequest) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

type GetBackupResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Spec *BackupSpec `protobuf:"bytes,1,opt,name=spec,proto3" json:"spec,omitempty"`
}

func (x *GetBackupResponse) Reset() {
	*x = GetBackupResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_gotocompany_optimus_core_v1beta1_backup_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetBackupResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetBackupResponse) ProtoMessage() {}

func (x *GetBackupResponse) ProtoReflect() protoreflect.Message {
	mi := &file_gotocompany_optimus_core_v1beta1_backup_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetBackupResponse.ProtoReflect.Descriptor instead.
func (*GetBackupResponse) Descriptor() ([]byte, []int) {
	return file_gotocompany_optimus_core_v1beta1_backup_proto_rawDescGZIP(), []int{7}
}

func (x *GetBackupResponse) GetSpec() *BackupSpec {
	if x != nil {
		return x.Spec
	}
	return nil
}

var File_gotocompany_optimus_core_v1beta1_backup_proto protoreflect.FileDescriptor

var file_gotocompany_optimus_core_v1beta1_backup_proto_rawDesc = []byte{
	0x0a, 0x2d, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2f, 0x6f, 0x70,
	0x74, 0x69, 0x6d, 0x75, 0x73, 0x2f, 0x63, 0x6f, 0x72, 0x65, 0x2f, 0x76, 0x31, 0x62, 0x65, 0x74,
	0x61, 0x31, 0x2f, 0x62, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12,
	0x20, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74,
	0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61,
	0x31, 0x1a, 0x1c, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x61, 0x6e,
	0x6e, 0x6f, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a,
	0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66,
	0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x1a, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x2d, 0x67, 0x65, 0x6e, 0x2d, 0x6f, 0x70, 0x65,
	0x6e, 0x61, 0x70, 0x69, 0x76, 0x32, 0x2f, 0x6f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2f, 0x61,
	0x6e, 0x6e, 0x6f, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x22, 0x3d, 0x0a, 0x0f, 0x49, 0x67, 0x6e, 0x6f, 0x72, 0x65, 0x64, 0x52, 0x65, 0x73, 0x6f, 0x75,
	0x72, 0x63, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x72, 0x65, 0x61, 0x73, 0x6f,
	0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x72, 0x65, 0x61, 0x73, 0x6f, 0x6e, 0x22,
	0xf7, 0x02, 0x0a, 0x13, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x21, 0x0a, 0x0c, 0x70, 0x72, 0x6f, 0x6a, 0x65,
	0x63, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x70,
	0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x25, 0x0a, 0x0e, 0x64, 0x61,
	0x74, 0x61, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x0d, 0x64, 0x61, 0x74, 0x61, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x4e, 0x61, 0x6d,
	0x65, 0x12, 0x25, 0x0a, 0x0e, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x5f, 0x6e,
	0x61, 0x6d, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x6e, 0x61, 0x6d, 0x65, 0x73,
	0x70, 0x61, 0x63, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x20, 0x0a, 0x0b, 0x64, 0x65, 0x73, 0x63,
	0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x64,
	0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x59, 0x0a, 0x06, 0x63, 0x6f,
	0x6e, 0x66, 0x69, 0x67, 0x18, 0x07, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x41, 0x2e, 0x67, 0x6f, 0x74,
	0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73,
	0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x43, 0x72,
	0x65, 0x61, 0x74, 0x65, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x2e, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x06, 0x63,
	0x6f, 0x6e, 0x66, 0x69, 0x67, 0x12, 0x25, 0x0a, 0x0e, 0x72, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63,
	0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x18, 0x09, 0x20, 0x03, 0x28, 0x09, 0x52, 0x0d, 0x72,
	0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x73, 0x1a, 0x39, 0x0a, 0x0b,
	0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b,
	0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a,
	0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61,
	0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x4a, 0x04, 0x08, 0x03, 0x10, 0x04, 0x4a, 0x04, 0x08,
	0x06, 0x10, 0x07, 0x4a, 0x04, 0x08, 0x08, 0x10, 0x09, 0x22, 0xc0, 0x01, 0x0a, 0x14, 0x43, 0x72,
	0x65, 0x61, 0x74, 0x65, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x12, 0x25, 0x0a, 0x0e, 0x72, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x5f, 0x6e,
	0x61, 0x6d, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x09, 0x52, 0x0d, 0x72, 0x65, 0x73, 0x6f,
	0x75, 0x72, 0x63, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x73, 0x12, 0x5e, 0x0a, 0x11, 0x69, 0x67, 0x6e,
	0x6f, 0x72, 0x65, 0x64, 0x5f, 0x72, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x73, 0x18, 0x03,
	0x20, 0x03, 0x28, 0x0b, 0x32, 0x31, 0x2e, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61,
	0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e,
	0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x49, 0x67, 0x6e, 0x6f, 0x72, 0x65, 0x64, 0x52,
	0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x52, 0x10, 0x69, 0x67, 0x6e, 0x6f, 0x72, 0x65, 0x64,
	0x52, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x73, 0x12, 0x1b, 0x0a, 0x09, 0x62, 0x61, 0x63,
	0x6b, 0x75, 0x70, 0x5f, 0x69, 0x64, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x62, 0x61,
	0x63, 0x6b, 0x75, 0x70, 0x49, 0x64, 0x4a, 0x04, 0x08, 0x02, 0x10, 0x03, 0x22, 0x85, 0x01, 0x0a,
	0x12, 0x4c, 0x69, 0x73, 0x74, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x73, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x12, 0x21, 0x0a, 0x0c, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x5f, 0x6e,
	0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x70, 0x72, 0x6f, 0x6a, 0x65,
	0x63, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x25, 0x0a, 0x0e, 0x64, 0x61, 0x74, 0x61, 0x73, 0x74,
	0x6f, 0x72, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d,
	0x64, 0x61, 0x74, 0x61, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x25, 0x0a,
	0x0e, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65,
	0x4e, 0x61, 0x6d, 0x65, 0x22, 0x5d, 0x0a, 0x13, 0x4c, 0x69, 0x73, 0x74, 0x42, 0x61, 0x63, 0x6b,
	0x75, 0x70, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x46, 0x0a, 0x07, 0x62,
	0x61, 0x63, 0x6b, 0x75, 0x70, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x2c, 0x2e, 0x67,
	0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d,
	0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e,
	0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x53, 0x70, 0x65, 0x63, 0x52, 0x07, 0x62, 0x61, 0x63, 0x6b,
	0x75, 0x70, 0x73, 0x22, 0xb3, 0x02, 0x0a, 0x0a, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x53, 0x70,
	0x65, 0x63, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02,
	0x69, 0x64, 0x12, 0x39, 0x0a, 0x0a, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x64, 0x5f, 0x61, 0x74,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61,
	0x6d, 0x70, 0x52, 0x09, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x64, 0x41, 0x74, 0x12, 0x20, 0x0a,
	0x0b, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x04, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x0b, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x12,
	0x50, 0x0a, 0x06, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x18, 0x05, 0x20, 0x03, 0x28, 0x0b, 0x32,
	0x38, 0x2e, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70,
	0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74,
	0x61, 0x31, 0x2e, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x53, 0x70, 0x65, 0x63, 0x2e, 0x43, 0x6f,
	0x6e, 0x66, 0x69, 0x67, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x06, 0x63, 0x6f, 0x6e, 0x66, 0x69,
	0x67, 0x12, 0x25, 0x0a, 0x0e, 0x72, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x5f, 0x6e, 0x61,
	0x6d, 0x65, 0x73, 0x18, 0x06, 0x20, 0x03, 0x28, 0x09, 0x52, 0x0d, 0x72, 0x65, 0x73, 0x6f, 0x75,
	0x72, 0x63, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x73, 0x1a, 0x39, 0x0a, 0x0b, 0x43, 0x6f, 0x6e, 0x66,
	0x69, 0x67, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c,
	0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a,
	0x02, 0x38, 0x01, 0x4a, 0x04, 0x08, 0x02, 0x10, 0x03, 0x22, 0x93, 0x01, 0x0a, 0x10, 0x47, 0x65,
	0x74, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x21,
	0x0a, 0x0c, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x4e, 0x61, 0x6d,
	0x65, 0x12, 0x25, 0x0a, 0x0e, 0x64, 0x61, 0x74, 0x61, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x5f, 0x6e,
	0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x64, 0x61, 0x74, 0x61, 0x73,
	0x74, 0x6f, 0x72, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x25, 0x0a, 0x0e, 0x6e, 0x61, 0x6d, 0x65,
	0x73, 0x70, 0x61, 0x63, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x0d, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12,
	0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x22,
	0x5b, 0x0a, 0x11, 0x47, 0x65, 0x74, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x12, 0x40, 0x0a, 0x04, 0x73, 0x70, 0x65, 0x63, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x2c, 0x2e, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79,
	0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31,
	0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x53, 0x70, 0x65, 0x63,
	0x52, 0x04, 0x73, 0x70, 0x65, 0x63, 0x4a, 0x04, 0x08, 0x02, 0x10, 0x03, 0x32, 0xb8, 0x05, 0x0a,
	0x0d, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0xe6,
	0x01, 0x0a, 0x0c, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x12,
	0x35, 0x2e, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70,
	0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74,
	0x61, 0x31, 0x2e, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x36, 0x2e, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d,
	0x70, 0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72,
	0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65,
	0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x67,
	0x82, 0xd3, 0xe4, 0x93, 0x02, 0x61, 0x22, 0x5c, 0x2f, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31,
	0x2f, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x2f, 0x7b, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63,
	0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x7d, 0x2f, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63,
	0x65, 0x2f, 0x7b, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x5f, 0x6e, 0x61, 0x6d,
	0x65, 0x7d, 0x2f, 0x64, 0x61, 0x74, 0x61, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x2f, 0x7b, 0x64, 0x61,
	0x74, 0x61, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x7d, 0x2f, 0x62, 0x61,
	0x63, 0x6b, 0x75, 0x70, 0x3a, 0x01, 0x2a, 0x12, 0xe0, 0x01, 0x0a, 0x0b, 0x4c, 0x69, 0x73, 0x74,
	0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x73, 0x12, 0x34, 0x2e, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f,
	0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f,
	0x72, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x4c, 0x69, 0x73, 0x74, 0x42,
	0x61, 0x63, 0x6b, 0x75, 0x70, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x35, 0x2e,
	0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69,
	0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31,
	0x2e, 0x4c, 0x69, 0x73, 0x74, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x73, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x22, 0x64, 0x82, 0xd3, 0xe4, 0x93, 0x02, 0x5e, 0x12, 0x5c, 0x2f, 0x76,
	0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2f, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x2f, 0x7b,
	0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x7d, 0x2f, 0x6e, 0x61,
	0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x2f, 0x7b, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61,
	0x63, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x7d, 0x2f, 0x64, 0x61, 0x74, 0x61, 0x73, 0x74, 0x6f,
	0x72, 0x65, 0x2f, 0x7b, 0x64, 0x61, 0x74, 0x61, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x5f, 0x6e, 0x61,
	0x6d, 0x65, 0x7d, 0x2f, 0x62, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x12, 0xda, 0x01, 0x0a, 0x09, 0x47,
	0x65, 0x74, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x12, 0x32, 0x2e, 0x67, 0x6f, 0x74, 0x6f, 0x63,
	0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63,
	0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x47, 0x65, 0x74, 0x42,
	0x61, 0x63, 0x6b, 0x75, 0x70, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x33, 0x2e, 0x67,
	0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d,
	0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e,
	0x47, 0x65, 0x74, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x22, 0x64, 0x82, 0xd3, 0xe4, 0x93, 0x02, 0x5e, 0x12, 0x5c, 0x2f, 0x76, 0x31, 0x2f, 0x70,
	0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x2f, 0x7b, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x5f,
	0x6e, 0x61, 0x6d, 0x65, 0x7d, 0x2f, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x2f,
	0x7b, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x7d,
	0x2f, 0x64, 0x61, 0x74, 0x61, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x2f, 0x7b, 0x64, 0x61, 0x74, 0x61,
	0x73, 0x74, 0x6f, 0x72, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x7d, 0x2f, 0x62, 0x61, 0x63, 0x6b,
	0x75, 0x70, 0x2f, 0x7b, 0x69, 0x64, 0x7d, 0x42, 0x95, 0x01, 0x0a, 0x1e, 0x63, 0x6f, 0x6d, 0x2e,
	0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x6e, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x42, 0x14, 0x42, 0x61, 0x63, 0x6b,
	0x75, 0x70, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x4d, 0x61, 0x6e, 0x61, 0x67, 0x65, 0x72,
	0x50, 0x01, 0x5a, 0x1e, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x67,
	0x6f, 0x74, 0x6f, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x6e, 0x2f, 0x6f, 0x70, 0x74, 0x69, 0x6d,
	0x75, 0x73, 0x92, 0x41, 0x3a, 0x12, 0x05, 0x32, 0x03, 0x30, 0x2e, 0x31, 0x1a, 0x0e, 0x31, 0x32,
	0x37, 0x2e, 0x30, 0x2e, 0x30, 0x2e, 0x31, 0x3a, 0x39, 0x31, 0x30, 0x30, 0x22, 0x04, 0x2f, 0x61,
	0x70, 0x69, 0x2a, 0x01, 0x01, 0x72, 0x18, 0x0a, 0x16, 0x4f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73,
	0x20, 0x42, 0x61, 0x63, 0x6b, 0x75, 0x70, 0x20, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x62,
	0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_gotocompany_optimus_core_v1beta1_backup_proto_rawDescOnce sync.Once
	file_gotocompany_optimus_core_v1beta1_backup_proto_rawDescData = file_gotocompany_optimus_core_v1beta1_backup_proto_rawDesc
)

func file_gotocompany_optimus_core_v1beta1_backup_proto_rawDescGZIP() []byte {
	file_gotocompany_optimus_core_v1beta1_backup_proto_rawDescOnce.Do(func() {
		file_gotocompany_optimus_core_v1beta1_backup_proto_rawDescData = protoimpl.X.CompressGZIP(file_gotocompany_optimus_core_v1beta1_backup_proto_rawDescData)
	})
	return file_gotocompany_optimus_core_v1beta1_backup_proto_rawDescData
}

var file_gotocompany_optimus_core_v1beta1_backup_proto_msgTypes = make([]protoimpl.MessageInfo, 10)
var file_gotocompany_optimus_core_v1beta1_backup_proto_goTypes = []interface{}{
	(*IgnoredResource)(nil),       // 0: gotocompany.optimus.core.v1beta1.IgnoredResource
	(*CreateBackupRequest)(nil),   // 1: gotocompany.optimus.core.v1beta1.CreateBackupRequest
	(*CreateBackupResponse)(nil),  // 2: gotocompany.optimus.core.v1beta1.CreateBackupResponse
	(*ListBackupsRequest)(nil),    // 3: gotocompany.optimus.core.v1beta1.ListBackupsRequest
	(*ListBackupsResponse)(nil),   // 4: gotocompany.optimus.core.v1beta1.ListBackupsResponse
	(*BackupSpec)(nil),            // 5: gotocompany.optimus.core.v1beta1.BackupSpec
	(*GetBackupRequest)(nil),      // 6: gotocompany.optimus.core.v1beta1.GetBackupRequest
	(*GetBackupResponse)(nil),     // 7: gotocompany.optimus.core.v1beta1.GetBackupResponse
	nil,                           // 8: gotocompany.optimus.core.v1beta1.CreateBackupRequest.ConfigEntry
	nil,                           // 9: gotocompany.optimus.core.v1beta1.BackupSpec.ConfigEntry
	(*timestamppb.Timestamp)(nil), // 10: google.protobuf.Timestamp
}
var file_gotocompany_optimus_core_v1beta1_backup_proto_depIdxs = []int32{
	8,  // 0: gotocompany.optimus.core.v1beta1.CreateBackupRequest.config:type_name -> gotocompany.optimus.core.v1beta1.CreateBackupRequest.ConfigEntry
	0,  // 1: gotocompany.optimus.core.v1beta1.CreateBackupResponse.ignored_resources:type_name -> gotocompany.optimus.core.v1beta1.IgnoredResource
	5,  // 2: gotocompany.optimus.core.v1beta1.ListBackupsResponse.backups:type_name -> gotocompany.optimus.core.v1beta1.BackupSpec
	10, // 3: gotocompany.optimus.core.v1beta1.BackupSpec.created_at:type_name -> google.protobuf.Timestamp
	9,  // 4: gotocompany.optimus.core.v1beta1.BackupSpec.config:type_name -> gotocompany.optimus.core.v1beta1.BackupSpec.ConfigEntry
	5,  // 5: gotocompany.optimus.core.v1beta1.GetBackupResponse.spec:type_name -> gotocompany.optimus.core.v1beta1.BackupSpec
	1,  // 6: gotocompany.optimus.core.v1beta1.BackupService.CreateBackup:input_type -> gotocompany.optimus.core.v1beta1.CreateBackupRequest
	3,  // 7: gotocompany.optimus.core.v1beta1.BackupService.ListBackups:input_type -> gotocompany.optimus.core.v1beta1.ListBackupsRequest
	6,  // 8: gotocompany.optimus.core.v1beta1.BackupService.GetBackup:input_type -> gotocompany.optimus.core.v1beta1.GetBackupRequest
	2,  // 9: gotocompany.optimus.core.v1beta1.BackupService.CreateBackup:output_type -> gotocompany.optimus.core.v1beta1.CreateBackupResponse
	4,  // 10: gotocompany.optimus.core.v1beta1.BackupService.ListBackups:output_type -> gotocompany.optimus.core.v1beta1.ListBackupsResponse
	7,  // 11: gotocompany.optimus.core.v1beta1.BackupService.GetBackup:output_type -> gotocompany.optimus.core.v1beta1.GetBackupResponse
	9,  // [9:12] is the sub-list for method output_type
	6,  // [6:9] is the sub-list for method input_type
	6,  // [6:6] is the sub-list for extension type_name
	6,  // [6:6] is the sub-list for extension extendee
	0,  // [0:6] is the sub-list for field type_name
}

func init() { file_gotocompany_optimus_core_v1beta1_backup_proto_init() }
func file_gotocompany_optimus_core_v1beta1_backup_proto_init() {
	if File_gotocompany_optimus_core_v1beta1_backup_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_gotocompany_optimus_core_v1beta1_backup_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*IgnoredResource); i {
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
		file_gotocompany_optimus_core_v1beta1_backup_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CreateBackupRequest); i {
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
		file_gotocompany_optimus_core_v1beta1_backup_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CreateBackupResponse); i {
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
		file_gotocompany_optimus_core_v1beta1_backup_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ListBackupsRequest); i {
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
		file_gotocompany_optimus_core_v1beta1_backup_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ListBackupsResponse); i {
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
		file_gotocompany_optimus_core_v1beta1_backup_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BackupSpec); i {
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
		file_gotocompany_optimus_core_v1beta1_backup_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetBackupRequest); i {
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
		file_gotocompany_optimus_core_v1beta1_backup_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetBackupResponse); i {
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
			RawDescriptor: file_gotocompany_optimus_core_v1beta1_backup_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   10,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_gotocompany_optimus_core_v1beta1_backup_proto_goTypes,
		DependencyIndexes: file_gotocompany_optimus_core_v1beta1_backup_proto_depIdxs,
		MessageInfos:      file_gotocompany_optimus_core_v1beta1_backup_proto_msgTypes,
	}.Build()
	File_gotocompany_optimus_core_v1beta1_backup_proto = out.File
	file_gotocompany_optimus_core_v1beta1_backup_proto_rawDesc = nil
	file_gotocompany_optimus_core_v1beta1_backup_proto_goTypes = nil
	file_gotocompany_optimus_core_v1beta1_backup_proto_depIdxs = nil
}
