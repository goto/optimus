package gdrive

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
)

type GDrive struct {
	srv *drive.Service
}

func NewGDrives(ctx context.Context, creds string) (*GDrive, error) {
	driveSrv, err := drive.NewService(ctx, option.WithCredentialsJSON([]byte(creds)))
	if err != nil {
		return nil, fmt.Errorf("not able to create drive service err: %w", err)
	}

	return &GDrive{srv: driveSrv}, nil
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

func (gd *GDrive) GetFilesMeta(folderID string) (*drive.FileList, error) {
	return gd.srv.Files.List().Q(fmt.Sprintf("'%s' in parents", folderID)).SupportsAllDrives(true).
		Fields("files(fileExtension, id, mimeType, modifiedTime, name, properties, size)").
		Do()
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

	if file.MimeType == "application/vnd.google-apps.folder" {
		return "folder", file, nil
	}
	return "file", file, nil
}
