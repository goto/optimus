package resource

import (
	"strings"
	"time"

	"github.com/goto/optimus/core/tenant"
)

type Status string

type SourceModifiedTimeStatus struct {
	FullName         string
	LastModifiedTime time.Time
	Err              error
}

type SourceModifiedRevisionStatus struct {
	FullName string
	Revision int
	Err      error
}

const (
	StatusUnknown           Status = "unknown"
	StatusValidationFailure Status = "validation_failure"
	StatusValidationSuccess Status = "validation_success"
	StatusToCreate          Status = "to_create"
	StatusToUpdate          Status = "to_update"
	StatusToDelete          Status = "to_delete"
	StatusDeleted           Status = "deleted"
	StatusSkipped           Status = "skipped"
	StatusCreateFailure     Status = "create_failure"
	StatusUpdateFailure     Status = "update_failure"
	StatusDeleteFailure     Status = "delete_failure"
	StatusExistInStore      Status = "exist_in_store"
	StatusSuccess           Status = "success"

	StatusFailure Status = "failed"
)

func (s Status) String() string {
	return string(s)
}

func FromStringToStatus(status string) Status {
	switch strings.ToLower(status) {
	case StatusValidationFailure.String():
		return StatusValidationFailure
	case StatusValidationSuccess.String():
		return StatusValidationSuccess
	case StatusToCreate.String():
		return StatusToCreate
	case StatusToUpdate.String():
		return StatusToUpdate
	case StatusSkipped.String():
		return StatusSkipped
	case StatusCreateFailure.String():
		return StatusCreateFailure
	case StatusUpdateFailure.String():
		return StatusUpdateFailure
	case StatusExistInStore.String():
		return StatusExistInStore
	case StatusSuccess.String():
		return StatusSuccess
	case StatusDeleted.String():
		return StatusDeleted
	default:
		return StatusUnknown
	}
}

func StatusForToCreate(status Status) bool {
	return status == StatusCreateFailure || status == StatusToCreate || status == StatusDeleted
}

func StatusForToUpdate(status Status) bool {
	return status == StatusSuccess ||
		status == StatusToUpdate ||
		status == StatusExistInStore ||
		status == StatusUpdateFailure
}

func StatusForToDelete(status Status) bool {
	return status == StatusSuccess || status == StatusExistInStore || status == StatusSkipped
}

func StatusIsSuccess(status Status) bool {
	return status == StatusSuccess
}

type SyncResponse struct {
	ResourceNames    []string
	IgnoredResources []IgnoredResource
}

type (
	DeleteRequest struct {
		Tenant    tenant.Tenant
		Datastore Store
		FullName  string
		Force     bool
	}

	DeleteResponse struct {
		DownstreamJobs []string
		Resource       *Resource
	}
)
