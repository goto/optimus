package tenant_test

import (
	"context"
	"testing"

	"github.com/goto/optimus/core/tenant"
	postgres "github.com/goto/optimus/internal/store/postgres/tenant"
	"github.com/goto/optimus/tests/setup"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

func TestPostgresLocationRepository(t *testing.T) {
	ctx := context.Background()

	proj, _ := tenant.NewProject("t-optimus-1",
		map[string]string{
			tenant.ProjectSchedulerHost:  "host",
			tenant.ProjectStoragePathKey: "gs://location",
		})

	dbSetup := func() *pgxpool.Pool {
		dbPool := setup.TestPool()
		setup.TruncateTablesWith(dbPool)

		prjRepo := postgres.NewProjectRepository(dbPool)
		err := prjRepo.Save(ctx, proj)
		if err != nil {
			panic(err)
		}

		return dbPool
	}
	loc1 := tenant.NewLocation("location1", "project1", "dataset1")

	t.Run("Create", func(t *testing.T) {
		t.Run("successfully store locations for the project", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewLocationRepository(db)

			err := repo.Create(ctx, proj.Name(), loc1)
			assert.NoError(t, err)

			actualLocations, err := repo.Get(ctx, proj.Name())
			assert.NoError(t, err)
			assert.Len(t, actualLocations, 1)
			assert.EqualValues(t, loc1, actualLocations[0])
		})

		t.Run("return error if there are duplicate location names", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewLocationRepository(db)

			// Create the first location
			err := repo.Create(ctx, proj.Name(), loc1)
			assert.NoError(t, err)

			// Attempt to create the same location again
			err = repo.Create(ctx, proj.Name(), loc1)
			assert.Error(t, err)
		})
	})

	t.Run("Get", func(t *testing.T) {
		t.Run("successfully retrieve locations for the project", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewLocationRepository(db)

			// Create a location
			err := repo.Create(ctx, proj.Name(), loc1)
			assert.NoError(t, err)

			actualLocations, err := repo.Get(ctx, proj.Name())
			assert.NoError(t, err)
			assert.Len(t, actualLocations, 1)
			assert.EqualValues(t, loc1, actualLocations[0])
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Run("successfully update an existing location", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewLocationRepository(db)

			err := repo.Create(ctx, proj.Name(), loc1)
			assert.NoError(t, err)

			updatedLocation := tenant.NewLocation(loc1.Name(), "project2", "dataset2")
			err = repo.Update(ctx, proj.Name(), updatedLocation)
			assert.NoError(t, err)

			actualLocations, err := repo.Get(ctx, proj.Name())
			assert.NoError(t, err)
			assert.Len(t, actualLocations, 1)
			assert.EqualValues(t, updatedLocation, actualLocations[0])
		})

		t.Run("return error if location is not found", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewLocationRepository(db)

			// Attempt to update a non-existent location
			nonExistentLocation := tenant.NewLocation("nonExistentLocation", "project1", "dataset1")
			err := repo.Update(ctx, proj.Name(), nonExistentLocation)
			assert.Error(t, err)
		})
	})

	t.Run("Delete", func(t *testing.T) {
		t.Run("successfully delete an existing location", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewLocationRepository(db)

			err := repo.Create(ctx, proj.Name(), loc1)
			assert.NoError(t, err)

			err = repo.Delete(ctx, proj.Name(), loc1.Name())
			assert.NoError(t, err)

			actualLocations, err := repo.Get(ctx, proj.Name())
			assert.NoError(t, err)
			assert.Len(t, actualLocations, 0)
		})

		t.Run("return success if location is not found", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewLocationRepository(db)

			nonExistentLocationName := "nonExistentLocation"
			err := repo.Delete(ctx, proj.Name(), nonExistentLocationName)
			assert.NoError(t, err)
		})
	})

}
