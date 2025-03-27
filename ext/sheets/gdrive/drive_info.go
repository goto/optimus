package gdrive

import (
	"fmt"
	"regexp"

	"google.golang.org/api/drive/v3"
)

const TypeFolder = "application/vnd.google-apps.folder"

var driveIDRegex = regexp.MustCompile(`(?:drive|docs)\.google\.com\/(?:file\/d\/|drive\/folders\/|open\?id=|spreadsheets\/d\/)([a-zA-Z0-9_-]+)`)

func ExtractFileID(driveLink string) (string, error) {
	res := driveIDRegex.FindStringSubmatch(driveLink)
	if len(res) > 1 {
		return res[1], nil
	}
	return "", fmt.Errorf("invalid URL: %s", driveLink)
}

func isFolder(file *drive.File) bool {
	return file.MimeType == "application/vnd.google-apps.folder"
}
