package filesystem

import "context"

type fileBlob struct{}

func newFileBlob() SchedulerFS {
	return &fileBlob{}
}

func (fs *fileBlob) Write(ctx context.Context, path string, data []byte) error {
	// TODO: implement here
	return nil
}

func (fs *fileBlob) List(dirPath string) []string {
	// TODO: implement here
	return nil
}

func (fs *fileBlob) Delete(ctx context.Context, path string) error {
	// TODO: implement here
	return nil
}
