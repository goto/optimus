package compiler

import (
	"context"

	"github.com/goto/optimus/core/scheduler"
)

type AirflowCompiler interface {
	Compile(ctx context.Context, jobWithDetails *scheduler.JobWithDetails) ([]byte, error)
}
