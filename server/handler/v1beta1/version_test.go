package v1beta1_test

import (
	"context"
	"testing"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"

	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
	v1 "github.com/goto/optimus/server/handler/v1beta1"
)

func TestVersionHandler(t *testing.T) {
	logger := log.NewNoop()
	ctx := context.Background()

	t.Run("Version", func(t *testing.T) {
		t.Run("returns the version of server", func(t *testing.T) {
			Version := "1.0.1"

			versionHandler := v1.NewVersionHandler(logger, Version)
			versionRequest := pb.VersionRequest{Client: Version}

			resp, err := versionHandler.Version(ctx, &versionRequest)
			assert.NoError(t, err)
			assert.Equal(t, Version, resp.Server)
		})
	})
}
