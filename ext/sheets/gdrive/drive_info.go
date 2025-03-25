package gdrive

import (
	"fmt"
	"regexp"
)

const TypeFolder = "application/vnd.google-apps.folder"

var driveIDRegex = regexp.MustCompile(`(?:drive\.google\.com\/(?:file\/d\/|drive\/folders\/|open\?id=))([a-zA-Z0-9_-]+)`)

func ExtractFileID(driveLink string) (string, error) {
	res := driveIDRegex.FindStringSubmatch(driveLink)
	if len(res) > 1 {
		return res[1], nil
	}
	return "", fmt.Errorf("invalid URL: %s", driveLink)
}
