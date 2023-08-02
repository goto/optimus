package filesystem

import "context"

type memBlob struct{}

func newMemBlob() SchedulerFS {
	return &memBlob{}
}

func (fs *memBlob) Write(ctx context.Context, path string, data []byte) error {
	// TODO: implement here
	return nil
}

func (fs *memBlob) List(dirPath string) []string {
	// TODO: implement here
	return nil
}

func (fs *memBlob) Delete(ctx context.Context, path string) error {
	// TODO: implement here
	return nil
}
