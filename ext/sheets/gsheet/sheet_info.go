package gsheet

import (
	"errors"
	"regexp"
	"strconv"
)

var (
	sheetIDRegex = regexp.MustCompile(`spreadsheets/d/([^/]*)`)
	gidRegex     = regexp.MustCompile(`gid=([0-9]*)`)
)

type SheetsInfo struct {
	SheetID string
	GID     int64
}

func FromURL(u1 string) (*SheetsInfo, error) {
	res := sheetIDRegex.FindStringSubmatch(u1)
	if len(res) < 2 || res[1] == "" {
		return nil, errors.New("not able to get spreadsheetID")
	}

	gid := int64(-1)
	res2 := gidRegex.FindStringSubmatch(u1)
	if len(res2) > 1 && res2[1] != "" {
		gid1, err := strconv.ParseInt(res2[1], 10, 64)
		if err == nil {
			gid = gid1
		}
	}

	return &SheetsInfo{
		SheetID: res[1],
		GID:     gid,
	}, nil
}
