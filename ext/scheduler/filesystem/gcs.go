package filesystem

import "context"

type gcsBucket struct{}

func newGCSBucket() SchedulerFS {
	return &gcsBucket{}
}

func (fs *gcsBucket) Write(ctx context.Context, path string, data []byte) error {
	// TODO: implement here
	return nil
}

func (fs *gcsBucket) List(dirPath string) []string {
	// TODO: implement here
	return nil
}

func (fs *gcsBucket) Delete(ctx context.Context, path string) error {
	// TODO: implement here
	return nil
}
