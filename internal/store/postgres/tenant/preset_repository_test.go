//go:build !unit_test

package tenant_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/window"
	postgres "github.com/goto/optimus/internal/store/postgres/tenant"
	"github.com/goto/optimus/tests/setup"
)

func TestPostgresPresetRepository(t *testing.T) {
	ctx := context.Background()

	proj, _ := tenant.NewProject("t-optimus-1",
		map[string]string{
			"bucket":                     "gs://some_folder-2",
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

	conf1 := window.SimpleConfig{
		Size:       "1d",
		Delay:      "1h",
		Location:   "",
		TruncateTo: "",
	}
	preset1 := tenant.NewPresetWithConfig("yesterday_v1", "preset for testing v1", conf1)
	preset2 := tenant.NewPresetWithConfig("yesterday_v2", "preset for testing v2", conf1)

	t.Run("Create", func(t *testing.T) {
		t.Run("should store presets with the given project name", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewPresetRepository(db)

			actualError := repo.Create(ctx, proj.Name(), preset1)
			assert.Nil(t, actualError)

			actualPresets, err := repo.Read(ctx, proj.Name())
			assert.NoError(t, err)
			assert.Len(t, actualPresets, 1)
			assert.EqualValues(t, preset1, actualPresets[0])
		})

		t.Run("should not insert duplicated presets within a project name", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewPresetRepository(db)

			actualError := repo.Create(ctx, proj.Name(), preset1)
			assert.Nil(t, actualError)
			actualError = repo.Create(ctx, proj.Name(), preset1)
			assert.Error(t, actualError)

			actualPresets, err := repo.Read(ctx, proj.Name())
			assert.NoError(t, err)
			assert.Len(t, actualPresets, 1)
			assert.EqualValues(t, preset1, actualPresets[0])
		})
	})

	t.Run("Read", func(t *testing.T) {
		t.Run("should read the stored presets given project name", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewPresetRepository(db)

			actualError := repo.Create(ctx, proj.Name(), preset1)
			assert.Nil(t, actualError)
			actualError = repo.Create(ctx, proj.Name(), preset2)
			assert.Nil(t, actualError)

			actualPresets, err := repo.Read(ctx, proj.Name())
			assert.NoError(t, err)
			assert.Len(t, actualPresets, 2)
			assert.EqualValues(t, preset1, actualPresets[0])
			assert.EqualValues(t, preset2, actualPresets[1])
		})

		t.Run("should return empty if no presets are found for the given project name", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewPresetRepository(db)

			actualPresets, err := repo.Read(ctx, proj.Name())
			assert.NoError(t, err)
			assert.Empty(t, actualPresets)
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Run("should update the stored presets given project name and incoming presets", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewPresetRepository(db)

			actualError := repo.Create(ctx, proj.Name(), preset1)
			assert.Nil(t, actualError)

			updatedDescription := "updated description"
			incomingPreset := tenant.NewPresetWithConfig(preset1.Name(), updatedDescription, preset1.Config())

			actualError = repo.Update(ctx, proj.Name(), incomingPreset)
			assert.NoError(t, actualError)

			storedPresets, err := repo.Read(ctx, proj.Name())
			assert.NoError(t, err)
			assert.Len(t, storedPresets, 1)
			assert.EqualValues(t, storedPresets[0], incomingPreset)
		})

		t.Run("should return error if the targeted preset within the project name does not exist", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewPresetRepository(db)

			actualError := repo.Update(ctx, proj.Name(), preset1)
			assert.Error(t, actualError)
		})
	})

	t.Run("Delete", func(t *testing.T) {
		t.Run("should delete the targeted preset within project", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewPresetRepository(db)

			actualError := repo.Create(ctx, proj.Name(), preset1)
			assert.Nil(t, actualError)
			actualError = repo.Create(ctx, proj.Name(), preset2)
			assert.Nil(t, actualError)

			storedPresets, err := repo.Read(ctx, proj.Name())
			assert.NoError(t, err)
			assert.Len(t, storedPresets, 2)

			actualError = repo.Delete(ctx, proj.Name(), preset1.Name())
			assert.Nil(t, actualError)

			storedPresets, err = repo.Read(ctx, proj.Name())
			assert.NoError(t, err)
			assert.Len(t, storedPresets, 1)

			assert.EqualValues(t, preset2, storedPresets[0])
		})
	})
}
