package larkdrive

import (
	"context"
	"fmt"
	"regexp"

	"github.com/goto/optimus/ext/sheets/lark"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/salt/log"

	larksdk "github.com/larksuite/oapi-sdk-go/v3"
	larkdrive "github.com/larksuite/oapi-sdk-go/v3/service/drive/v1"
)

const EntityLarkDrive = "larkdrive"

var driveIDRegexp = regexp.MustCompile(`larksuite\.com\/drive\/folder\/([a-zA-Z0-9]+)`)

type SheetCallback func(file *Sheet) error

type Client struct {
	client      *larksdk.Client
	sheetClient *lark.Client
}

func (*Client) GetFolderToken(url string) (string, error) {
	var folderToken string

	if !driveIDRegexp.MatchString(url) {
		return folderToken, errors.NotFound(EntityLarkDrive, "invalid Lark Drive URL format")
	}

	subMatched := driveIDRegexp.FindStringSubmatch(url)
	if len(subMatched) > 1 {
		folderToken = subMatched[1]
		return folderToken, nil
	}

	return folderToken, errors.NotFound(EntityLarkDrive, "failed to extract folder token from URL")
}

func (c *Client) IterateSheet(ctx context.Context, folderToken string, callback SheetCallback) error {
	builder := larkdrive.NewListFileReqBuilder().
		PageSize(100).
		FolderToken(folderToken).
		OrderBy("EditedTime").
		Direction("DESC")

	for {
		req := builder.Build()

		resp, err := c.client.Drive.File.List(ctx, req)
		if err != nil {
			return errors.AddErrContext(err, EntityLarkDrive, "failed to list files from lark folder")
		}

		if !resp.Success() {
			return errors.NewError(ErrCodeToErrorType(resp.CodeError), EntityLarkDrive, fmt.Sprintf("failed to list files with error: %s", resp.ErrorResp()))
		}

		for _, file := range resp.Data.Files {
			sheet, err := NewSheet(c.sheetClient, file)
			if err != nil {
				continue
			}

			if err := callback(sheet); err != nil {
				return err
			}
		}

		hasMoreData := resp.Data.HasMore != nil && *resp.Data.HasMore
		hasNextPageToken := resp.Data.NextPageToken == nil
		hasNextPage := hasMoreData && hasNextPageToken
		if hasNextPage {
			break
		}

		builder.PageToken(*resp.Data.NextPageToken)
	}

	return nil
}

func (c *Client) GetRevisionID(ctx context.Context, url string) (int, error) {
	folderToken, err := c.GetFolderToken(url)
	if err != nil {
		return 0, err
	}

	builder := larkdrive.NewListFileReqBuilder().
		PageSize(100).
		FolderToken(folderToken).
		OrderBy("EditedTime").
		Direction("DESC")
	revisionNumbers := make([]int, 0)

	for {
		req := builder.Build()

		resp, err := c.client.Drive.File.List(ctx, req)
		if err != nil {
			return 0, errors.AddErrContext(err, EntityLarkDrive, "failed to list files from lark folder")
		}

		if !resp.Success() {
			return 0, errors.NewError(ErrCodeToErrorType(resp.CodeError), EntityLarkDrive, fmt.Sprintf("failed to list files with error: %s", resp.ErrorResp()))
		}

		for _, file := range resp.Data.Files {
			sheet, err := NewSheet(c.sheetClient, file)
			if err != nil {
				continue
			}

			revisionId, err := sheet.GetRevisionID(ctx, *sheet.Url)
			if err != nil {
				return 0, errors.AddErrContext(err, EntityLarkDrive, fmt.Sprintf("failed to get revision ID for file: %s", *file.Name))
			}
			revisionNumbers = append(revisionNumbers, revisionId)
		}

		hasMoreData := resp.Data.HasMore != nil && *resp.Data.HasMore
		hasNextPageToken := resp.Data.NextPageToken == nil
		hasNextPage := hasMoreData && hasNextPageToken
		if hasNextPage {
			break
		}

		builder.PageToken(*resp.Data.NextPageToken)
	}

	revisionNumber := GenerateRevNumForDrive(revisionNumbers)
	return revisionNumber, nil
}

func NewClient(secret string, logger log.Logger) (*Client, error) {
	cred, err := NewCredentialFromSecret(secret)
	if err != nil {
		return nil, err
	}

	options := []larksdk.ClientOptionFunc{
		larksdk.WithLogger(NewLogger(logger)),
	}
	client := larksdk.NewClient(cred.AppID, cred.AppSecret, options...)

	return &Client{
		client: client,
	}, nil
}
