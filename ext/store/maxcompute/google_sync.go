package maxcompute

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"google.golang.org/api/drive/v3"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/ext/sheets/gdrive"
	"github.com/goto/optimus/ext/sheets/gsheet"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/pool"
)

func (s *SyncerService) getDriveClient(ctx context.Context, tnnt tenant.Tenant) (*gdrive.GDrive, error) {
	secret, err := s.secretProvider.GetSecret(ctx, tnnt, GsheetCredsKey)
	if err != nil {
		return nil, err
	}

	driveSrv, err := gdrive.NewGDrives(ctx, secret.Value(), s.maxFileSizeSupported, s.driveFileCleanupSizeLimit)
	if err != nil {
		return nil, fmt.Errorf("not able to create drive service err: %w", err)
	}

	return driveSrv, nil
}

func getGSheetContent(et *ExternalTable, sheets *gsheet.GSheets) (string, error) {
	headers, err := et.Source.GetHeaderCount()
	if err != nil {
		return "", err
	}

	et.Source.GetFormattedDate = et.Source.GetFormattedDate || !et.Schema.ContainsDateTimeColumns()

	uri := et.Source.SourceURIs[0]
	columnCount := len(et.Schema)
	return sheets.GetAsCSV(uri, et.Source.Range, et.Source.GetFormattedDate, et.Source.GetFormattedData, columnCount, func(rowIndex, colIndex int, data any) (string, error) {
		if rowIndex < headers {
			s, _ := ParseString(data) // ignore header parsing error, as headers will be ignored in data
			return s, nil
		}
		value, err := formatSheetData(colIndex, data, et.Schema)
		if err != nil {
			if d, ok := data.(string); ok {
				if strings.HasPrefix(d, "#REF!") || strings.HasPrefix(d, "#N/A") {
					err = nil
					value = ""
				} else {
					if _, ok := validInfinityValues[d]; ok { // check infinity
						err = nil
						value = d
					}
				}
			}
		}
		err = errors.WrapIfErr(EntityFormatter, fmt.Sprintf("for column Index:%d", colIndex), err)
		return value, err
	})
}

func processGoogleSheet(ctx context.Context, sheetSrv *gsheet.GSheets, ossClient *oss.Client, et *ExternalTable, commonLocation string) error {
	if len(et.Source.SourceURIs) == 0 {
		return errors.InvalidArgument(EntityExternalTable, "source URI is empty for Google Sheet")
	}

	content, err := getGSheetContent(et, sheetSrv)
	if err != nil {
		return err
	}

	bucketName, objectPath, err := getBucketNameAndPath(commonLocation, et.Source.Location, et.FullName())
	if err != nil {
		return err
	}
	objectKey := objectPath + "file.csv"
	err = deleteFolderFromBucket(ctx, ossClient, bucketName, objectPath)
	if err != nil {
		return err
	}
	return writeToBucket(ctx, ossClient, bucketName, objectKey, content)
}

func (s *SyncerService) GetGoogleSourceLastModified(ctx context.Context, tnnt tenant.Tenant, resources []*resource.Resource) ([]resource.SourceModifiedTimeStatus, error) {
	ets, err := ConvertSpecsTo[ExternalTable](resources)
	if err != nil {
		return nil, err
	}
	return s.getGoogleSourceLastModified(ctx, tnnt, ets)
}

func (s *SyncerService) getGoogleSourceLastModified(ctx context.Context, tnnt tenant.Tenant, ets []*ExternalTable) ([]resource.SourceModifiedTimeStatus, error) {
	var response []resource.SourceModifiedTimeStatus

	driveClient, err := s.getDriveClient(ctx, tnnt)
	if err != nil {
		return nil, errors.InternalError(EntityExternalTable, "unable to get google drive Client", err)
	}

	var jobs []func() pool.JobResult[resource.SourceModifiedTimeStatus]
	for _, et := range ets {
		et := et
		if et.Source.SourceType != GoogleSheet && et.Source.SourceType != GoogleDrive {
			response = append(response, resource.SourceModifiedTimeStatus{
				FullName: et.FullName(),
				Err:      errors.InvalidArgument(EntityExternalTable, "source is not GoogleSheet or GoogleDrive"),
			})
			continue
		}

		jobs = append(jobs, func() pool.JobResult[resource.SourceModifiedTimeStatus] {
			lastModified, err := driveClient.GetLastModified(et.Source.SourceURIs[0])
			if err != nil {
				return pool.JobResult[resource.SourceModifiedTimeStatus]{
					Output: resource.SourceModifiedTimeStatus{
						FullName: et.FullName(),
						Err:      errors.InvalidArgument(EntityExternalTable, err.Error()),
					},
					Err: errors.InvalidArgument(EntityExternalTable, err.Error()),
				}
			}
			return pool.JobResult[resource.SourceModifiedTimeStatus]{
				Output: resource.SourceModifiedTimeStatus{
					FullName:         et.FullName(),
					LastModifiedTime: *lastModified,
				},
			}
		})
	}
	resultsChan := pool.RunWithWorkers(10, jobs)
	for result := range resultsChan {
		response = append(response, result.Output)
	}
	return response, nil
}

func SyncDriveFileToOSS(ctx context.Context, driveClient *gdrive.GDrive, driveFile *drive.File, ossClient *oss.Client, bucketName, objectName string, et *ExternalTable) error {
	if !strings.EqualFold(driveFile.FileExtension, et.Source.ContentType) {
		return nil
	}
	if !driveClient.IsWithinDownloadLimit(driveFile) {
		return errors.InvalidArgument(EntityExternalTable, fmt.Sprintf("file size:[%d] mb, greated than configured limit", driveFile.Size/1000000))
	}
	content, err := driveClient.DownloadFile(driveFile.Id)
	if err != nil {
		return err
	}
	var contentToSync string
	if et.Source.CleanGDriveCSV && strings.EqualFold(et.Source.ContentType, CSV) {
		if !driveClient.IsWithinParseLimit(driveFile) {
			return errors.InvalidArgument(EntityExternalTable, fmt.Sprintf("external table source option 'clean_gdrive_csv' is enabled, but the file size (%d MB) exceeds the server's configured driveFileCleanupSizeLimit", driveFile.Size/1000000))
		}
		contentToSync, err = cleanCsvContent(string(content), et)
		if err != nil {
			return err
		}
	} else {
		contentToSync = string(content)
	}
	return writeToBucket(ctx, ossClient, bucketName, objectName, contentToSync)
}

func SyncDriveFolderToOSS(ctx context.Context, driveClient *gdrive.GDrive, ossClient *oss.Client, folderID, bucketName, destination string, et *ExternalTable) error {
	driveFiles, err := driveClient.FolderListShow(folderID)
	if err != nil {
		return err
	}
	for _, driveFile := range driveFiles.Files {
		objectName := filepath.Join(destination, driveFile.Name)
		if strings.EqualFold(driveFile.MimeType, gdrive.TypeFolder) {
			err = SyncDriveFolderToOSS(ctx, driveClient, ossClient, driveFile.Id, bucketName, objectName, et)
			if err != nil {
				return err
			}
		}
		err = SyncDriveFileToOSS(ctx, driveClient, driveFile, ossClient, bucketName, objectName, et)
		if err != nil {
			return err
		}
	}
	return nil
}

func syncFromDrive(ctx context.Context, driveSrv *gdrive.GDrive, ossClient *oss.Client, et *ExternalTable, bucketName, objectPath string) error {
	fileType, driveFile, err := driveSrv.ListDriveEntity(et.Source.SourceURIs[0])
	if err != nil {
		return err
	}
	if fileType == "folder" {
		return SyncDriveFolderToOSS(ctx, driveSrv, ossClient, driveFile.Id, bucketName, objectPath, et)
	}
	objectName := filepath.Join(objectPath, driveFile.Name)
	return SyncDriveFileToOSS(ctx, driveSrv, driveFile, ossClient, bucketName, objectName, et)
}

func processGoogleDrive(ctx context.Context, driveSrv *gdrive.GDrive, ossClient *oss.Client, et *ExternalTable, commonLocation string) error {
	if len(et.Source.SourceURIs) == 0 {
		return errors.InvalidArgument(EntityExternalTable, "source URI is empty for Google Drive")
	}

	bucketName, objectPath, err := getBucketNameAndPath(commonLocation, et.Source.Location, et.FullName())
	if err != nil {
		return err
	}

	err = deleteFolderFromBucket(ctx, ossClient, bucketName, objectPath)
	if err != nil {
		return err
	}

	return syncFromDrive(ctx, driveSrv, ossClient, et, bucketName, objectPath)
}

func processGoogleTypeSources(ctx context.Context, externalTableClients ExtTableClients, ossClient *oss.Client, et *ExternalTable, commonLocation string) error {
	switch et.Source.SourceType {
	case GoogleSheet:
		return processGoogleSheet(ctx, externalTableClients.GSheet, ossClient, et, commonLocation)
	case GoogleDrive:
		return processGoogleDrive(ctx, externalTableClients.GDrive, ossClient, et, commonLocation)
	}
	return nil
}
