package gsheet

import (
	"errors"
	"regexp"
)

var (
	sheetIDRegex = regexp.MustCompile(`spreadsheets/d/([^/]*)`)
	gidRegex     = regexp.MustCompile(`gid=([0-9]*)`)
)

type SheetsInfo struct {
	SheetID string
	Gid     string
}

func FromURL(u1 string) (*SheetsInfo, error) {
	res := sheetIDRegex.FindStringSubmatch(u1)
	if len(res) < 2 || res[1] == "" {
		return nil, errors.New("not able to get spreadsheetID")
	}

	gid := ""
	res2 := gidRegex.FindStringSubmatch(u1)
	if len(res2) > 1 && res2[1] != "" {
		gid = res2[1]
	}

	return &SheetsInfo{
		SheetID: res[1],
		Gid:     gid,
	}, nil
}
