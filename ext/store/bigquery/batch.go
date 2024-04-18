package bigquery

import (
	"context"

	"github.com/kushsharma/parallel"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
)

type Batch struct {
	Dataset        Dataset
	DatasetDetails *resource.Resource

	provider ClientProvider

	Tables         []*resource.Resource
	ExternalTables []*resource.Resource
	Views          []*resource.Resource
}

func (b *Batch) QueueJobs(ctx context.Context, account string, runner *parallel.Runner) error {
	client, err := b.provider.Get(ctx, account)
	if err != nil {
		return err
	}

	if err := b.validateDataset(ctx, client); err != nil {
		return err
	}

	if b.DatasetDetails != nil {
		dsHandle := client.DatasetHandleFrom(b.Dataset)
		if err := createOrUpdate(ctx, dsHandle, b.DatasetDetails); err != nil {
			return err
		}
	}

	for _, table := range b.Tables {
		runner.Add(func(res *resource.Resource) func() (interface{}, error) {
			return func() (interface{}, error) {
				ds, err := DataSetFor(res.Name())
				if err != nil {
					return res, err
				}
				resourceName, err := ResourceNameFor(res.Name(), res.Kind())
				if err != nil {
					return res, err
				}
				handle := client.TableHandleFrom(ds, resourceName)
				err = createOrUpdate(ctx, handle, res)
				return res, err
			}
		}(table))
	}

	for _, extTables := range b.ExternalTables {
		runner.Add(func(res *resource.Resource) func() (interface{}, error) {
			return func() (interface{}, error) {
				ds, err := DataSetFor(res.Name())
				if err != nil {
					return res, err
				}
				resourceName, err := ResourceNameFor(res.Name(), res.Kind())
				if err != nil {
					return res, err
				}
				handle := client.ExternalTableHandleFrom(ds, resourceName)
				err = createOrUpdate(ctx, handle, res)
				return res, err
			}
		}(extTables))
	}

	for _, view := range b.Views {
		runner.Add(func(res *resource.Resource) func() (interface{}, error) {
			return func() (interface{}, error) {
				ds, err := DataSetFor(res.Name())
				if err != nil {
					return res, err
				}
				resourceName, err := ResourceNameFor(res.Name(), res.Kind())
				if err != nil {
					return res, err
				}
				handle := client.ViewHandleFrom(ds, resourceName)
				err = createOrUpdate(ctx, handle, res)
				return res, err
			}
		}(view))
	}
	return nil
}

func createOrUpdate(ctx context.Context, handle ResourceHandle, res *resource.Resource) error {
	if res.Status() == resource.StatusToUpdate {
		return update(ctx, handle, res)
	} else if res.Status() == resource.StatusToCreate {
		return create(ctx, handle, res)
	}
	return nil
}

func create(ctx context.Context, handle ResourceHandle, res *resource.Resource) error {
	if handle.Exists(ctx) {
		return res.MarkSuccess()
	}

	err := handle.Create(ctx, res)
	if err != nil && !errors.IsErrorType(err, errors.ErrAlreadyExists) {
		res.MarkFailure()
		return err
	}

	return res.MarkSuccess()
}

func update(ctx context.Context, handle ResourceHandle, res *resource.Resource) error {
	if err := handle.Update(ctx, res); err != nil {
		res.MarkFailure()
		return err
	}
	return res.MarkSuccess()
}

func (b *Batch) validateDataset(ctx context.Context, client Client) error {
	if b.DatasetDetails != nil {
		return nil
	}

	dsHandle := client.DatasetHandleFrom(b.Dataset)
	if !dsHandle.Exists(ctx) {
		return errors.NotFound(EntityDataset, "dataset ["+b.Dataset.FullName()+"] is not found")
	}

	return nil
}

func BatchesFrom(resources []*resource.Resource, provider ClientProvider) (map[string]*Batch, error) {
	mapping := make(map[string]*Batch)

	me := errors.NewMultiError("error while creating batches")
	for _, res := range resources {
		dataset, err := DataSetFor(res.Name())
		if err != nil {
			me.Append(err)
			continue
		}

		batch, ok := mapping[dataset.FullName()]
		if !ok {
			batch = &Batch{
				Dataset:  dataset,
				provider: provider,
			}
		}

		switch res.Kind() {
		case KindDataset:
			batch.DatasetDetails = res
		case KindView:
			batch.Views = append(batch.Views, res)
		case KindExternalTable:
			batch.ExternalTables = append(batch.ExternalTables, res)
		case KindTable:
			batch.Tables = append(batch.Tables, res)
		default:
		}

		mapping[dataset.FullName()] = batch
	}
	return mapping, me.ToErr()
}
