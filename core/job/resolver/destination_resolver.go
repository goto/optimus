package resolver

import (
	"context"
	"errors"
	"fmt"
)

// GenerateDestination uses config details to build target table
// this format should match with GenerateDependencies output
// Note: only for bq2bq job
func GenerateDestination(ctx context.Context, configs map[string]string) (string, error) {
	proj, ok1 := configs["PROJECT"]
	dataset, ok2 := configs["DATASET"]
	tab, ok3 := configs["TABLE"]
	if ok1 && ok2 && ok3 {
		return fmt.Sprintf("%s:%s.%s", proj, dataset, tab), nil
	}
	return "", errors.New("missing config key required to generate destination")
}
