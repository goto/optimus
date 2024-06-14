// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.0
// 	protoc        (unknown)
// source: gotocompany/optimus/core/v1beta1/namespace.proto

package optimus

import (
	_ "github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2/options"
	_ "google.golang.org/genproto/googleapis/api/annotations"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type RegisterProjectNamespaceRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ProjectName string                  `protobuf:"bytes,1,opt,name=project_name,json=projectName,proto3" json:"project_name,omitempty"`
	Namespace   *NamespaceSpecification `protobuf:"bytes,2,opt,name=namespace,proto3" json:"namespace,omitempty"`
}

func (x *RegisterProjectNamespaceRequest) Reset() {
	*x = RegisterProjectNamespaceRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_gotocompany_optimus_core_v1beta1_namespace_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RegisterProjectNamespaceRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RegisterProjectNamespaceRequest) ProtoMessage() {}

func (x *RegisterProjectNamespaceRequest) ProtoReflect() protoreflect.Message {
	mi := &file_gotocompany_optimus_core_v1beta1_namespace_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RegisterProjectNamespaceRequest.ProtoReflect.Descriptor instead.
func (*RegisterProjectNamespaceRequest) Descriptor() ([]byte, []int) {
	return file_gotocompany_optimus_core_v1beta1_namespace_proto_rawDescGZIP(), []int{0}
}

func (x *RegisterProjectNamespaceRequest) GetProjectName() string {
	if x != nil {
		return x.ProjectName
	}
	return ""
}

func (x *RegisterProjectNamespaceRequest) GetNamespace() *NamespaceSpecification {
	if x != nil {
		return x.Namespace
	}
	return nil
}

type RegisterProjectNamespaceResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Success bool   `protobuf:"varint,1,opt,name=success,proto3" json:"success,omitempty"`
	Message string `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
}

func (x *RegisterProjectNamespaceResponse) Reset() {
	*x = RegisterProjectNamespaceResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_gotocompany_optimus_core_v1beta1_namespace_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RegisterProjectNamespaceResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RegisterProjectNamespaceResponse) ProtoMessage() {}

func (x *RegisterProjectNamespaceResponse) ProtoReflect() protoreflect.Message {
	mi := &file_gotocompany_optimus_core_v1beta1_namespace_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RegisterProjectNamespaceResponse.ProtoReflect.Descriptor instead.
func (*RegisterProjectNamespaceResponse) Descriptor() ([]byte, []int) {
	return file_gotocompany_optimus_core_v1beta1_namespace_proto_rawDescGZIP(), []int{1}
}

func (x *RegisterProjectNamespaceResponse) GetSuccess() bool {
	if x != nil {
		return x.Success
	}
	return false
}

func (x *RegisterProjectNamespaceResponse) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

type ListProjectNamespacesRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ProjectName string `protobuf:"bytes,1,opt,name=project_name,json=projectName,proto3" json:"project_name,omitempty"`
}

func (x *ListProjectNamespacesRequest) Reset() {
	*x = ListProjectNamespacesRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_gotocompany_optimus_core_v1beta1_namespace_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListProjectNamespacesRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListProjectNamespacesRequest) ProtoMessage() {}

func (x *ListProjectNamespacesRequest) ProtoReflect() protoreflect.Message {
	mi := &file_gotocompany_optimus_core_v1beta1_namespace_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListProjectNamespacesRequest.ProtoReflect.Descriptor instead.
func (*ListProjectNamespacesRequest) Descriptor() ([]byte, []int) {
	return file_gotocompany_optimus_core_v1beta1_namespace_proto_rawDescGZIP(), []int{2}
}

func (x *ListProjectNamespacesRequest) GetProjectName() string {
	if x != nil {
		return x.ProjectName
	}
	return ""
}

type ListProjectNamespacesResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Namespaces []*NamespaceSpecification `protobuf:"bytes,1,rep,name=namespaces,proto3" json:"namespaces,omitempty"`
}

func (x *ListProjectNamespacesResponse) Reset() {
	*x = ListProjectNamespacesResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_gotocompany_optimus_core_v1beta1_namespace_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListProjectNamespacesResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListProjectNamespacesResponse) ProtoMessage() {}

func (x *ListProjectNamespacesResponse) ProtoReflect() protoreflect.Message {
	mi := &file_gotocompany_optimus_core_v1beta1_namespace_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListProjectNamespacesResponse.ProtoReflect.Descriptor instead.
func (*ListProjectNamespacesResponse) Descriptor() ([]byte, []int) {
	return file_gotocompany_optimus_core_v1beta1_namespace_proto_rawDescGZIP(), []int{3}
}

func (x *ListProjectNamespacesResponse) GetNamespaces() []*NamespaceSpecification {
	if x != nil {
		return x.Namespaces
	}
	return nil
}

type GetNamespaceRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ProjectName   string `protobuf:"bytes,1,opt,name=project_name,json=projectName,proto3" json:"project_name,omitempty"`
	NamespaceName string `protobuf:"bytes,2,opt,name=namespace_name,json=namespaceName,proto3" json:"namespace_name,omitempty"`
}

func (x *GetNamespaceRequest) Reset() {
	*x = GetNamespaceRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_gotocompany_optimus_core_v1beta1_namespace_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetNamespaceRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetNamespaceRequest) ProtoMessage() {}

func (x *GetNamespaceRequest) ProtoReflect() protoreflect.Message {
	mi := &file_gotocompany_optimus_core_v1beta1_namespace_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetNamespaceRequest.ProtoReflect.Descriptor instead.
func (*GetNamespaceRequest) Descriptor() ([]byte, []int) {
	return file_gotocompany_optimus_core_v1beta1_namespace_proto_rawDescGZIP(), []int{4}
}

func (x *GetNamespaceRequest) GetProjectName() string {
	if x != nil {
		return x.ProjectName
	}
	return ""
}

func (x *GetNamespaceRequest) GetNamespaceName() string {
	if x != nil {
		return x.NamespaceName
	}
	return ""
}

type GetNamespaceResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Namespace *NamespaceSpecification `protobuf:"bytes,1,opt,name=namespace,proto3" json:"namespace,omitempty"`
}

func (x *GetNamespaceResponse) Reset() {
	*x = GetNamespaceResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_gotocompany_optimus_core_v1beta1_namespace_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetNamespaceResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetNamespaceResponse) ProtoMessage() {}

func (x *GetNamespaceResponse) ProtoReflect() protoreflect.Message {
	mi := &file_gotocompany_optimus_core_v1beta1_namespace_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetNamespaceResponse.ProtoReflect.Descriptor instead.
func (*GetNamespaceResponse) Descriptor() ([]byte, []int) {
	return file_gotocompany_optimus_core_v1beta1_namespace_proto_rawDescGZIP(), []int{5}
}

func (x *GetNamespaceResponse) GetNamespace() *NamespaceSpecification {
	if x != nil {
		return x.Namespace
	}
	return nil
}

type NamespaceSpecification struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name   string            `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Config map[string]string `protobuf:"bytes,2,rep,name=config,proto3" json:"config,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *NamespaceSpecification) Reset() {
	*x = NamespaceSpecification{}
	if protoimpl.UnsafeEnabled {
		mi := &file_gotocompany_optimus_core_v1beta1_namespace_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *NamespaceSpecification) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NamespaceSpecification) ProtoMessage() {}

func (x *NamespaceSpecification) ProtoReflect() protoreflect.Message {
	mi := &file_gotocompany_optimus_core_v1beta1_namespace_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NamespaceSpecification.ProtoReflect.Descriptor instead.
func (*NamespaceSpecification) Descriptor() ([]byte, []int) {
	return file_gotocompany_optimus_core_v1beta1_namespace_proto_rawDescGZIP(), []int{6}
}

func (x *NamespaceSpecification) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *NamespaceSpecification) GetConfig() map[string]string {
	if x != nil {
		return x.Config
	}
	return nil
}

var File_gotocompany_optimus_core_v1beta1_namespace_proto protoreflect.FileDescriptor

var file_gotocompany_optimus_core_v1beta1_namespace_proto_rawDesc = []byte{
	0x0a, 0x30, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2f, 0x6f, 0x70,
	0x74, 0x69, 0x6d, 0x75, 0x73, 0x2f, 0x63, 0x6f, 0x72, 0x65, 0x2f, 0x76, 0x31, 0x62, 0x65, 0x74,
	0x61, 0x31, 0x2f, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x12, 0x20, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e,
	0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x62,
	0x65, 0x74, 0x61, 0x31, 0x1a, 0x1c, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x61, 0x70, 0x69,
	0x2f, 0x61, 0x6e, 0x6e, 0x6f, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x1a, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x2d, 0x67, 0x65, 0x6e, 0x2d, 0x6f,
	0x70, 0x65, 0x6e, 0x61, 0x70, 0x69, 0x76, 0x32, 0x2f, 0x6f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73,
	0x2f, 0x61, 0x6e, 0x6e, 0x6f, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x22, 0x9c, 0x01, 0x0a, 0x1f, 0x52, 0x65, 0x67, 0x69, 0x73, 0x74, 0x65, 0x72, 0x50,
	0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x21, 0x0a, 0x0c, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63,
	0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x70, 0x72,
	0x6f, 0x6a, 0x65, 0x63, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x56, 0x0a, 0x09, 0x6e, 0x61, 0x6d,
	0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x38, 0x2e, 0x67,
	0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d,
	0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e,
	0x4e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x53, 0x70, 0x65, 0x63, 0x69, 0x66, 0x69,
	0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x09, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63,
	0x65, 0x22, 0x56, 0x0a, 0x20, 0x52, 0x65, 0x67, 0x69, 0x73, 0x74, 0x65, 0x72, 0x50, 0x72, 0x6f,
	0x6a, 0x65, 0x63, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x73, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x08, 0x52, 0x07, 0x73, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, 0x12,
	0x18, 0x0a, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x22, 0x41, 0x0a, 0x1c, 0x4c, 0x69, 0x73,
	0x74, 0x50, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63,
	0x65, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x21, 0x0a, 0x0c, 0x70, 0x72, 0x6f,
	0x6a, 0x65, 0x63, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x0b, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x22, 0x79, 0x0a, 0x1d,
	0x4c, 0x69, 0x73, 0x74, 0x50, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x73,
	0x70, 0x61, 0x63, 0x65, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x58, 0x0a,
	0x0a, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28,
	0x0b, 0x32, 0x38, 0x2e, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e,
	0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x62,
	0x65, 0x74, 0x61, 0x31, 0x2e, 0x4e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x53, 0x70,
	0x65, 0x63, 0x69, 0x66, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x0a, 0x6e, 0x61, 0x6d,
	0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x73, 0x22, 0x5f, 0x0a, 0x13, 0x47, 0x65, 0x74, 0x4e, 0x61,
	0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x21,
	0x0a, 0x0c, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x4e, 0x61, 0x6d,
	0x65, 0x12, 0x25, 0x0a, 0x0e, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x5f, 0x6e,
	0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x6e, 0x61, 0x6d, 0x65, 0x73,
	0x70, 0x61, 0x63, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x22, 0x6e, 0x0a, 0x14, 0x47, 0x65, 0x74, 0x4e,
	0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x12, 0x56, 0x0a, 0x09, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x38, 0x2e, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e,
	0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76,
	0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x4e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65,
	0x53, 0x70, 0x65, 0x63, 0x69, 0x66, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x09, 0x6e,
	0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x22, 0xc5, 0x01, 0x0a, 0x16, 0x4e, 0x61, 0x6d,
	0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x53, 0x70, 0x65, 0x63, 0x69, 0x66, 0x69, 0x63, 0x61, 0x74,
	0x69, 0x6f, 0x6e, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x5c, 0x0a, 0x06, 0x63, 0x6f, 0x6e, 0x66, 0x69,
	0x67, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x44, 0x2e, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f,
	0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f,
	0x72, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x4e, 0x61, 0x6d, 0x65, 0x73,
	0x70, 0x61, 0x63, 0x65, 0x53, 0x70, 0x65, 0x63, 0x69, 0x66, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f,
	0x6e, 0x2e, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x06, 0x63,
	0x6f, 0x6e, 0x66, 0x69, 0x67, 0x1a, 0x39, 0x0a, 0x0b, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x45,
	0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01,
	0x32, 0xfe, 0x04, 0x0a, 0x10, 0x4e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x53, 0x65,
	0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0xd7, 0x01, 0x0a, 0x18, 0x52, 0x65, 0x67, 0x69, 0x73, 0x74,
	0x65, 0x72, 0x50, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61,
	0x63, 0x65, 0x12, 0x41, 0x2e, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79,
	0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31,
	0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x52, 0x65, 0x67, 0x69, 0x73, 0x74, 0x65, 0x72, 0x50, 0x72,
	0x6f, 0x6a, 0x65, 0x63, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x42, 0x2e, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70,
	0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65,
	0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x52, 0x65, 0x67, 0x69, 0x73, 0x74, 0x65,
	0x72, 0x50, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63,
	0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x34, 0x82, 0xd3, 0xe4, 0x93, 0x02,
	0x2e, 0x3a, 0x01, 0x2a, 0x22, 0x29, 0x2f, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2f, 0x70,
	0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x2f, 0x7b, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x5f,
	0x6e, 0x61, 0x6d, 0x65, 0x7d, 0x2f, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x12,
	0xcb, 0x01, 0x0a, 0x15, 0x4c, 0x69, 0x73, 0x74, 0x50, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x4e,
	0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x73, 0x12, 0x3e, 0x2e, 0x67, 0x6f, 0x74, 0x6f,
	0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e,
	0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x4c, 0x69, 0x73,
	0x74, 0x50, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63,
	0x65, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x3f, 0x2e, 0x67, 0x6f, 0x74, 0x6f,
	0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e,
	0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x4c, 0x69, 0x73,
	0x74, 0x50, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63,
	0x65, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x31, 0x82, 0xd3, 0xe4, 0x93,
	0x02, 0x2b, 0x12, 0x29, 0x2f, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2f, 0x70, 0x72, 0x6f,
	0x6a, 0x65, 0x63, 0x74, 0x2f, 0x7b, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x5f, 0x6e, 0x61,
	0x6d, 0x65, 0x7d, 0x2f, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x12, 0xc1, 0x01,
	0x0a, 0x0c, 0x47, 0x65, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x12, 0x35,
	0x2e, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74,
	0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61,
	0x31, 0x2e, 0x47, 0x65, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x36, 0x2e, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6d, 0x70,
	0x61, 0x6e, 0x79, 0x2e, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x2e, 0x63, 0x6f, 0x72, 0x65,
	0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x47, 0x65, 0x74, 0x4e, 0x61, 0x6d, 0x65,
	0x73, 0x70, 0x61, 0x63, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x42, 0x82,
	0xd3, 0xe4, 0x93, 0x02, 0x3c, 0x12, 0x3a, 0x2f, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2f,
	0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x2f, 0x7b, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74,
	0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x7d, 0x2f, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65,
	0x2f, 0x7b, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x5f, 0x6e, 0x61, 0x6d, 0x65,
	0x7d, 0x42, 0x9b, 0x01, 0x0a, 0x1e, 0x63, 0x6f, 0x6d, 0x2e, 0x67, 0x6f, 0x74, 0x6f, 0x63, 0x6f,
	0x6d, 0x70, 0x61, 0x6e, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x6e, 0x2e, 0x6f, 0x70, 0x74,
	0x69, 0x6d, 0x75, 0x73, 0x42, 0x17, 0x4e, 0x61, 0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x53,
	0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x4d, 0x61, 0x6e, 0x61, 0x67, 0x65, 0x72, 0x50, 0x01, 0x5a,
	0x1e, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x67, 0x6f, 0x74, 0x6f,
	0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x6e, 0x2f, 0x6f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x92,
	0x41, 0x3d, 0x12, 0x05, 0x32, 0x03, 0x30, 0x2e, 0x31, 0x1a, 0x0e, 0x31, 0x32, 0x37, 0x2e, 0x30,
	0x2e, 0x30, 0x2e, 0x31, 0x3a, 0x39, 0x31, 0x30, 0x30, 0x22, 0x04, 0x2f, 0x61, 0x70, 0x69, 0x2a,
	0x01, 0x01, 0x72, 0x1b, 0x0a, 0x19, 0x4f, 0x70, 0x74, 0x69, 0x6d, 0x75, 0x73, 0x20, 0x4e, 0x61,
	0x6d, 0x65, 0x73, 0x70, 0x61, 0x63, 0x65, 0x20, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x62,
	0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_gotocompany_optimus_core_v1beta1_namespace_proto_rawDescOnce sync.Once
	file_gotocompany_optimus_core_v1beta1_namespace_proto_rawDescData = file_gotocompany_optimus_core_v1beta1_namespace_proto_rawDesc
)

func file_gotocompany_optimus_core_v1beta1_namespace_proto_rawDescGZIP() []byte {
	file_gotocompany_optimus_core_v1beta1_namespace_proto_rawDescOnce.Do(func() {
		file_gotocompany_optimus_core_v1beta1_namespace_proto_rawDescData = protoimpl.X.CompressGZIP(file_gotocompany_optimus_core_v1beta1_namespace_proto_rawDescData)
	})
	return file_gotocompany_optimus_core_v1beta1_namespace_proto_rawDescData
}

var file_gotocompany_optimus_core_v1beta1_namespace_proto_msgTypes = make([]protoimpl.MessageInfo, 8)
var file_gotocompany_optimus_core_v1beta1_namespace_proto_goTypes = []interface{}{
	(*RegisterProjectNamespaceRequest)(nil),  // 0: gotocompany.optimus.core.v1beta1.RegisterProjectNamespaceRequest
	(*RegisterProjectNamespaceResponse)(nil), // 1: gotocompany.optimus.core.v1beta1.RegisterProjectNamespaceResponse
	(*ListProjectNamespacesRequest)(nil),     // 2: gotocompany.optimus.core.v1beta1.ListProjectNamespacesRequest
	(*ListProjectNamespacesResponse)(nil),    // 3: gotocompany.optimus.core.v1beta1.ListProjectNamespacesResponse
	(*GetNamespaceRequest)(nil),              // 4: gotocompany.optimus.core.v1beta1.GetNamespaceRequest
	(*GetNamespaceResponse)(nil),             // 5: gotocompany.optimus.core.v1beta1.GetNamespaceResponse
	(*NamespaceSpecification)(nil),           // 6: gotocompany.optimus.core.v1beta1.NamespaceSpecification
	nil,                                      // 7: gotocompany.optimus.core.v1beta1.NamespaceSpecification.ConfigEntry
}
var file_gotocompany_optimus_core_v1beta1_namespace_proto_depIdxs = []int32{
	6, // 0: gotocompany.optimus.core.v1beta1.RegisterProjectNamespaceRequest.namespace:type_name -> gotocompany.optimus.core.v1beta1.NamespaceSpecification
	6, // 1: gotocompany.optimus.core.v1beta1.ListProjectNamespacesResponse.namespaces:type_name -> gotocompany.optimus.core.v1beta1.NamespaceSpecification
	6, // 2: gotocompany.optimus.core.v1beta1.GetNamespaceResponse.namespace:type_name -> gotocompany.optimus.core.v1beta1.NamespaceSpecification
	7, // 3: gotocompany.optimus.core.v1beta1.NamespaceSpecification.config:type_name -> gotocompany.optimus.core.v1beta1.NamespaceSpecification.ConfigEntry
	0, // 4: gotocompany.optimus.core.v1beta1.NamespaceService.RegisterProjectNamespace:input_type -> gotocompany.optimus.core.v1beta1.RegisterProjectNamespaceRequest
	2, // 5: gotocompany.optimus.core.v1beta1.NamespaceService.ListProjectNamespaces:input_type -> gotocompany.optimus.core.v1beta1.ListProjectNamespacesRequest
	4, // 6: gotocompany.optimus.core.v1beta1.NamespaceService.GetNamespace:input_type -> gotocompany.optimus.core.v1beta1.GetNamespaceRequest
	1, // 7: gotocompany.optimus.core.v1beta1.NamespaceService.RegisterProjectNamespace:output_type -> gotocompany.optimus.core.v1beta1.RegisterProjectNamespaceResponse
	3, // 8: gotocompany.optimus.core.v1beta1.NamespaceService.ListProjectNamespaces:output_type -> gotocompany.optimus.core.v1beta1.ListProjectNamespacesResponse
	5, // 9: gotocompany.optimus.core.v1beta1.NamespaceService.GetNamespace:output_type -> gotocompany.optimus.core.v1beta1.GetNamespaceResponse
	7, // [7:10] is the sub-list for method output_type
	4, // [4:7] is the sub-list for method input_type
	4, // [4:4] is the sub-list for extension type_name
	4, // [4:4] is the sub-list for extension extendee
	0, // [0:4] is the sub-list for field type_name
}

func init() { file_gotocompany_optimus_core_v1beta1_namespace_proto_init() }
func file_gotocompany_optimus_core_v1beta1_namespace_proto_init() {
	if File_gotocompany_optimus_core_v1beta1_namespace_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_gotocompany_optimus_core_v1beta1_namespace_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RegisterProjectNamespaceRequest); i {
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
		file_gotocompany_optimus_core_v1beta1_namespace_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RegisterProjectNamespaceResponse); i {
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
		file_gotocompany_optimus_core_v1beta1_namespace_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ListProjectNamespacesRequest); i {
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
		file_gotocompany_optimus_core_v1beta1_namespace_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ListProjectNamespacesResponse); i {
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
		file_gotocompany_optimus_core_v1beta1_namespace_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetNamespaceRequest); i {
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
		file_gotocompany_optimus_core_v1beta1_namespace_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetNamespaceResponse); i {
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
		file_gotocompany_optimus_core_v1beta1_namespace_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*NamespaceSpecification); i {
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
			RawDescriptor: file_gotocompany_optimus_core_v1beta1_namespace_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   8,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_gotocompany_optimus_core_v1beta1_namespace_proto_goTypes,
		DependencyIndexes: file_gotocompany_optimus_core_v1beta1_namespace_proto_depIdxs,
		MessageInfos:      file_gotocompany_optimus_core_v1beta1_namespace_proto_msgTypes,
	}.Build()
	File_gotocompany_optimus_core_v1beta1_namespace_proto = out.File
	file_gotocompany_optimus_core_v1beta1_namespace_proto_rawDesc = nil
	file_gotocompany_optimus_core_v1beta1_namespace_proto_goTypes = nil
	file_gotocompany_optimus_core_v1beta1_namespace_proto_depIdxs = nil
}
