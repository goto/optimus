package filesystem

import "context"

type SchedulerFS interface {
	Write(ctx context.Context, path string, data []byte) error
	List(dirPath string) []string
	Delete(ctx context.Context, path string) error
}
