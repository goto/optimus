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

func TestPostgresPresetRepository(t *testing.T) {
	ctx := context.Background()
	dbSetup := func() *pgxpool.Pool {
		dbPool := setup.TestPool()
		setup.TruncateTablesWith(dbPool)

		return dbPool
	}

	preset1, err := tenant.NewPreset("yesterday_v1", "preset for testing v1", "d", "-1h", "24h")
	assert.NoError(t, err)
	preset2, err := tenant.NewPreset("yesterday_v2", "preset for testing v2", "d", "-1h", "24h")
	assert.NoError(t, err)

	projectName, err := tenant.ProjectNameFrom("project_test")
	assert.NoError(t, err)

	t.Run("Create", func(t *testing.T) {
		t.Run("should store presets with the given project name", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewPresetRepository(db)

			actualError := repo.Create(ctx, projectName, preset1)
			assert.Nil(t, actualError)

			actualPresets, err := repo.Read(ctx, projectName)
			assert.NoError(t, err)
			assert.Len(t, actualPresets, 1)
			assert.EqualValues(t, preset1, actualPresets[0])
		})

		t.Run("should not insert duplicated presets within a project name", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewPresetRepository(db)

			actualError := repo.Create(ctx, projectName, preset1)
			assert.Nil(t, actualError)
			actualError = repo.Create(ctx, projectName, preset1)
			assert.Error(t, actualError)

			actualPresets, err := repo.Read(ctx, projectName)
			assert.NoError(t, err)
			assert.Len(t, actualPresets, 1)
			assert.EqualValues(t, preset1, actualPresets[0])
		})
	})

	t.Run("Read", func(t *testing.T) {
		t.Run("should read the stored presets given project name", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewPresetRepository(db)

			actualError := repo.Create(ctx, projectName, preset1)
			assert.Nil(t, actualError)
			actualError = repo.Create(ctx, projectName, preset2)
			assert.Nil(t, actualError)

			actualPresets, err := repo.Read(ctx, projectName)
			assert.NoError(t, err)
			assert.Len(t, actualPresets, 2)
			assert.EqualValues(t, preset1, actualPresets[0])
			assert.EqualValues(t, preset2, actualPresets[1])
		})

		t.Run("should return empty if no presets are found for the given project name", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewPresetRepository(db)

			actualPresets, err := repo.Read(ctx, projectName)
			assert.NoError(t, err)
			assert.Empty(t, actualPresets)
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Run("should update the stored presets given project name and incoming presets", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewPresetRepository(db)

			actualError := repo.Create(ctx, projectName, preset1)
			assert.Nil(t, actualError)

			updatedDescription := "updated description"
			incomingPreset, err := tenant.NewPreset(
				preset1.Name(), updatedDescription,
				preset1.Window().GetTruncateTo(), preset1.Window().GetOffset(), preset1.Window().GetSize(),
			)
			assert.NoError(t, err)

			actualError = repo.Update(ctx, projectName, incomingPreset)
			assert.NoError(t, actualError)

			storedPresets, err := repo.Read(ctx, projectName)
			assert.NoError(t, err)
			assert.Len(t, storedPresets, 1)
			assert.EqualValues(t, storedPresets[0], incomingPreset)
		})

		t.Run("should return error if the targeted preset within the project name does not exist", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewPresetRepository(db)

			actualError := repo.Update(ctx, projectName, preset1)
			assert.Error(t, actualError)
		})
	})

	t.Run("Delete", func(t *testing.T) {
		t.Run("should delete the targeted preset within project", func(t *testing.T) {
			db := dbSetup()
			repo := postgres.NewPresetRepository(db)

			actualError := repo.Create(ctx, projectName, preset1)
			assert.Nil(t, actualError)
			actualError = repo.Create(ctx, projectName, preset2)
			assert.Nil(t, actualError)

			storedPresets, err := repo.Read(ctx, projectName)
			assert.NoError(t, err)
			assert.Len(t, storedPresets, 2)

			actualError = repo.Delete(ctx, projectName, preset1.Name())
			assert.Nil(t, actualError)

			storedPresets, err = repo.Read(ctx, projectName)
			assert.NoError(t, err)
			assert.Len(t, storedPresets, 1)

			assert.EqualValues(t, preset1, storedPresets[0])
		})
	})
}
