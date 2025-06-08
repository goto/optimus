package maxcompute

import (
	"context"
	"fmt"
	"strings"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/ext/sheets/lark"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/pool"
)

func (s *SyncerService) getLarkClient(ctx context.Context, tnnt tenant.Tenant) (*lark.Client, error) {
	secret, err := s.secretProvider.GetSecret(ctx, tnnt, LarkCredentialsKey)
	if err != nil {
		return nil, err
	}
	client, err := lark.NewLarkClient(ctx, secret.Value())
	if err != nil {
		return nil, fmt.Errorf("not able to create Lark Client err: %w", err)
	}
	return client, nil
}

func (s *SyncerService) GetLarkRevisionIDs(ctx context.Context, tnnt tenant.Tenant, resources []*resource.Resource) ([]resource.SourceModifiedRevisionStatus, error) {
	var response []resource.SourceModifiedRevisionStatus
	lc, err := s.getLarkClient(ctx, tnnt)
	if err != nil {
		return nil, err
	}

	var jobs []func() pool.JobResult[resource.SourceModifiedRevisionStatus]
	for _, res := range resources {
		r := res
		et, err := ConvertSpecTo[ExternalTable](r)
		if err != nil {
			response = append(response, resource.SourceModifiedRevisionStatus{
				FullName: r.FullName(),
				Err:      err,
			})
			continue
		}
		if et.Source.SourceType != LarkSheet {
			response = append(response, resource.SourceModifiedRevisionStatus{
				FullName: r.FullName(),
				Err:      errors.InvalidArgument(EntityExternalTable, "source is not LarkSheet"),
			})
			continue
		}

		jobs = append(jobs, func() pool.JobResult[resource.SourceModifiedRevisionStatus] {
			revisionID, err := lc.GetRevisionID(et.Source.SourceURIs[0])
			if err != nil {
				return pool.JobResult[resource.SourceModifiedRevisionStatus]{
					Output: resource.SourceModifiedRevisionStatus{
						FullName: r.FullName(),
						Err:      errors.InvalidArgument(EntityExternalTable, err.Error()),
					},
					Err: errors.InvalidArgument(EntityExternalTable, err.Error()),
				}
			}
			return pool.JobResult[resource.SourceModifiedRevisionStatus]{
				Output: resource.SourceModifiedRevisionStatus{
					FullName: r.FullName(),
					Revision: revisionID,
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

func getLSheetContent(et *ExternalTable, sheets *lark.Client) (int, string, error) {
	headers, err := et.Source.GetHeaderCount()
	if err != nil {
		return 0, "", err
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
				if strings.HasPrefix(d, "#REF!") || strings.HasPrefix(d, "#N/A") || strings.HasPrefix(d, "NaN") {
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

func processLarkSheet(ctx context.Context, lark *lark.Client, ossClient *oss.Client, et *ExternalTable, commonLocation string) (int, error) {
	if len(et.Source.SourceURIs) == 0 {
		return 0, errors.InvalidArgument(EntityExternalTable, "source URI is empty for Lark Sheet")
	}

	revisionNumber, content, err := getLSheetContent(et, lark)
	if err != nil {
		return revisionNumber, err
	}

	bucketName, objectPath, err := getBucketNameAndPath(commonLocation, et.Source.Location, et.FullName())
	if err != nil {
		return revisionNumber, err
	}
	objectKey := objectPath + "file.csv"
	err = deleteFolderFromBucket(ctx, ossClient, bucketName, objectPath)
	if err != nil {
		return revisionNumber, err
	}
	return revisionNumber, writeToBucket(ctx, ossClient, bucketName, objectKey, content)
}
