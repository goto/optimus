package larkdrive

import (
	"bytes"
	"encoding/binary"
	"hash/crc64"
	"sort"

	larksheet "github.com/goto/optimus/ext/sheets/lark"
	"github.com/goto/optimus/internal/errors"

	larkdrive "github.com/larksuite/oapi-sdk-go/v3/service/drive/v1"
)

// LarkFileType enum can be referred to https://open.larksuite.com/document/server-docs/docs/drive-v1/folder/list on data.files[].type

const (
	LarkFileTypeSheet = "sheet"
)

// Sheet represents from Lark Drive File with type "sheet" and have client to interact with Lark Sheets API.
type Sheet struct {
	*larksheet.Client
	*larkdrive.File
}

func NewSheet(client *larksheet.Client, file *larkdrive.File) (*Sheet, error) {
	if file == nil {
		return nil, errors.InvalidArgument(EntityLarkDrive, "file cannot be nil")
	}
	if file.Token == nil || *file.Token == "" {
		return nil, errors.InvalidArgument(EntityLarkDrive, "file token cannot be empty")
	}
	if file.Name == nil || *file.Name == "" {
		return nil, errors.InvalidArgument(EntityLarkDrive, "file name cannot be empty")
	}

	if !IsLarkSheet(file) {
		return nil, errors.InvalidArgument(EntityLarkDrive, "file is not a Lark Sheet")
	}

	return &Sheet{
		Client: client,
		File:   file,
	}, nil
}

func IsLarkSheet(file *larkdrive.File) bool {
	if file.Type == nil {
		return false
	}
	return *file.Type == LarkFileTypeSheet
}

// GenerateRevNumForDrive generates a revision number for a Lark Drive file based on its revisions.
// It sorts the revisions, converts them to a byte slice, and computes a CRC32 checksum.
// This is used to create a unique identifier for the file based on its revision history.
// If revisions ordered need to be preserved, we can copy the revisions slice before sorting it.
func GenerateRevNumForDrive(revisions []int) int {
	if len(revisions) == 0 {
		return 0
	}

	sort.Ints(revisions)

	table := crc64.MakeTable(crc64.ECMA)
	var buf bytes.Buffer
	for _, v := range revisions {
		binary.Write(&buf, binary.BigEndian, uint32(v))
	}

	revNumber := int(crc64.Checksum(buf.Bytes(), table))
	buf.Reset()

	return revNumber
}
