package gdrive

import (
	"context"
	"fmt"
	"io"
	"time"

	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
)

const (
	GoogleDriveTimeFormat = "2006-01-02T15:04:05.000Z"
)

type GDrive struct {
	srv                            *drive.Service
	maxFileSizeSupportedBytes      int64
	driveFileCleanupSizeLimitBytes int64
}

func NewGDrives(ctx context.Context, creds string, maxFileSizeSupportedMB, driveFileCleanupSizeLimitMB int) (*GDrive, error) {
	driveSrv, err := drive.NewService(ctx, option.WithCredentialsJSON([]byte(creds)))
	if err != nil {
		return nil, fmt.Errorf("not able to create drive service err: %w", err)
	}

	return &GDrive{
		srv:                            driveSrv,
		maxFileSizeSupportedBytes:      int64(maxFileSizeSupportedMB) * 1000000,
		driveFileCleanupSizeLimitBytes: int64(driveFileCleanupSizeLimitMB) * 1000000,
	}, nil
}

// IsWithinDownloadLimit checks whether the given file's size is within the permissible download limit.
// Returns true if no limit is set or if the file size is below the configured maximum.
func (gd *GDrive) IsWithinDownloadLimit(file *drive.File) bool {
	if gd.maxFileSizeSupportedBytes == 0 {
		// No file size limit is configured; allow download
		return true
	}
	if file.Size <= gd.maxFileSizeSupportedBytes {
		return true
	}
	return false
}

// IsWithinParseLimit checks whether the given file's size is within the permissible parse limit.
// Returns true if no limit is set or if the file size is below the configured maximum.
func (gd *GDrive) IsWithinParseLimit(file *drive.File) bool {
	if gd.driveFileCleanupSizeLimitBytes == 0 {
		// No parse size limit is configured; allow parsing
		return true
	}
	if file.Size <= gd.driveFileCleanupSizeLimitBytes {
		return true
	}
	return false
}

// DownloadFile downloads a file from Google Drive
func (gd *GDrive) DownloadFile(fileID string) ([]byte, error) {
	resp, err := gd.srv.Files.Get(fileID).SupportsAllDrives(true).Download()
	if err != nil {
		return nil, fmt.Errorf("failed to download file: %w", err)
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (gd *GDrive) FolderListShow(folderID string) (*drive.FileList, error) {
	return gd.srv.Files.List().
		IncludeItemsFromAllDrives(true).
		SupportsAllDrives(true).
		Q(fmt.Sprintf("'%s' in parents", folderID)).
		Fields("files(fileExtension, id, mimeType, modifiedTime, name, properties, size)").
		Do()
}

func (gd *GDrive) getLatestFileModifyTimestamp(folderID string) (*time.Time, error) {
	fileList, err := gd.FolderListShow(folderID)
	if err != nil {
		return nil, err
	}
	latestTime := time.Time{}
	for _, file := range fileList.Files {
		modifiedTime, err := time.Parse("2006-01-02T15:04:05.000Z", file.ModifiedTime)
		if err != nil {
			continue
		}
		if modifiedTime.After(latestTime) {
			latestTime = modifiedTime
		}
	}
	return &latestTime, nil
}

func (gd *GDrive) fetchLastModifiedViaRevisionAPI(fileID string) *time.Time {
	var revisionList *drive.RevisionList

	revisionList, err := gd.srv.Revisions.List(fileID).Fields("revisions(modifiedTime,mimeType, id )").Do()
	if err != nil {
		return nil
	}

	if revisionList == nil || len(revisionList.Revisions) < 1 {
		return nil
	}

	timeString := revisionList.Revisions[len(revisionList.Revisions)-1].ModifiedTime
	modifiedTime, err := time.Parse(GoogleDriveTimeFormat, timeString)
	if err != nil {
		return nil
	}

	return &modifiedTime
}

func (gd *GDrive) GetLastModified(url string) (*time.Time, error) {
	fileID, err := ExtractFileID(url)
	if err != nil {
		return nil, err
	}

	file, err := gd.srv.Files.Get(fileID).SupportsAllDrives(true).
		Fields("modifiedTime", "mimeType", "version").Do()
	if err != nil {
		return nil, fmt.Errorf("unable to get file metadata: %w", err)
	}
	if isFolder(file) {
		return gd.getLatestFileModifyTimestamp(fileID)
	}
	modifiedTime, err := time.Parse(GoogleDriveTimeFormat, file.ModifiedTime)
	if err != nil {
		return nil, err
	}

	if isGoogleSheet(file) {
		// for Google Sheet Type sources, Try fetching via revision ID, if revision ID is available
		modifiedTimeByRevisionsAPI := gd.fetchLastModifiedViaRevisionAPI(fileID)
		if modifiedTimeByRevisionsAPI != nil && modifiedTimeByRevisionsAPI.After(modifiedTime) {
			modifiedTime = *modifiedTimeByRevisionsAPI
		}
	}

	return &modifiedTime, nil
}

// ListDriveEntity determines if a Google Drive link is a file or a directory
func (gd *GDrive) ListDriveEntity(url string) (string, *drive.File, error) {
	fileID, err := ExtractFileID(url)
	if err != nil {
		return "", nil, err
	}
	file, err := gd.srv.Files.Get(fileID).SupportsAllDrives(true).
		Fields("fileExtension", "id", "mimeType", "modifiedTime", "name", "properties", "size").Do()
	if err != nil {
		return "", nil, fmt.Errorf("unable to get file metadata: %w", err)
	}

	if isFolder(file) {
		return "folder", file, nil
	}
	return "file", file, nil
}
