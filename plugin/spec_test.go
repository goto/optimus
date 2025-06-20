package plugin_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/plugin"
)

func TestPluginSpec(t *testing.T) {
	t.Run("GetImage return the image for default version", func(t *testing.T) {
		spec := plugin.Spec{
			SpecVersion: 1,
			Name:        "BQ",
			Description: "BQ plugin",
			PluginVersion: map[string]plugin.VersionDetails{
				plugin.DefaultVersion: {
					Image: "example.io/namespace/bq2bq-executor",
					Tag:   "latest",
					Entrypoint: plugin.Entrypoint{
						Shell:  "/bin/bash",
						Script: "python3 /opt/bumblebee/main.py",
					},
				},
			},
		}

		image, err := spec.GetImage("")
		assert.NoError(t, err)
		assert.Equal(t, "example.io/namespace/bq2bq-executor:latest", image)
	})
	t.Run("GetImage return the image as per version", func(t *testing.T) {
		spec := plugin.Spec{
			SpecVersion: 1,
			Name:        "BQ",
			Description: "BQ plugin",
			PluginVersion: map[string]plugin.VersionDetails{
				plugin.DefaultVersion: {
					Image: "example.io/namespace/bq2bq-executor",
					Tag:   "latest",
				},
				"3.5": {
					Tag: "3.5.0",
				},
			},
		}

		image, err := spec.GetImage("3.5")
		assert.NoError(t, err)
		assert.Equal(t, "example.io/namespace/bq2bq-executor:3.5.0", image)
	})
	t.Run("GetEntrypoint return the default entry point", func(t *testing.T) {
		spec := plugin.Spec{
			SpecVersion: 1,
			Name:        "BQ",
			Description: "BQ plugin",
			PluginVersion: map[string]plugin.VersionDetails{
				plugin.DefaultVersion: {
					Entrypoint: plugin.Entrypoint{
						Shell:  "/bin/bash",
						Script: "python3 /opt/bumblebee/main.py",
					},
				},
			},
		}

		ep, err := spec.GetEntrypoint("")
		assert.NoError(t, err)
		assert.Equal(t, "/bin/bash", ep.Shell)
		assert.Equal(t, "python3 /opt/bumblebee/main.py", ep.Script)
	})
	t.Run("GetEntrypoint return the entrypoint for version", func(t *testing.T) {
		spec := plugin.Spec{
			SpecVersion: 1,
			Name:        "BQ",
			Description: "BQ plugin",
			PluginVersion: map[string]plugin.VersionDetails{
				plugin.DefaultVersion: {
					Entrypoint: plugin.Entrypoint{
						Shell:  "/bin/bash",
						Script: "python3 /opt/bumblebee/main.py",
					},
				},
				"3.5": {
					Entrypoint: plugin.Entrypoint{
						Shell: "/bin/zsh",
					},
				},
			},
		}

		ep, err := spec.GetEntrypoint("3.5")
		assert.NoError(t, err)
		assert.Equal(t, "/bin/zsh", ep.Shell)
		assert.Equal(t, "python3 /opt/bumblebee/main.py", ep.Script)
	})
	t.Run("GetEntrypoint return default shell when missing", func(t *testing.T) {
		spec := plugin.Spec{
			SpecVersion: 1,
			Name:        "BQ",
			Description: "BQ plugin",
			PluginVersion: map[string]plugin.VersionDetails{
				plugin.DefaultVersion: {
					Entrypoint: plugin.Entrypoint{
						Script: "python3 /opt/bumblebee/main.py",
					},
				},
			},
		}

		ep, err := spec.GetEntrypoint("")
		assert.NoError(t, err)
		assert.Equal(t, "/bin/sh", ep.Shell)
	})
	t.Run("Load", func(t *testing.T) {
		t.Run("returns error when no path is wrong", func(t *testing.T) {
			_, err := plugin.Load("./tests/sample_non_existing.yaml")
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "spec not found")
		})
		t.Run("returns error when no plugin exists", func(t *testing.T) {
			_, err := plugin.Load("./tests/sample_plugin_invalid.yaml")
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "yaml: unmarshal errors")
		})
		t.Run("returns plugin after loading", func(t *testing.T) {
			p1, err := plugin.Load("./tests/sample_plugin.yaml")
			assert.NoError(t, err)

			assert.Equal(t, p1.Name, "bq2bqtest")
		})
	})
	t.Run("Validate", func(t *testing.T) {
		t.Run("returns error on missing name", func(t *testing.T) {
			s1 := plugin.Spec{}
			err := s1.Validate()
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "plugin name is required")
		})
		t.Run("returns error on missing versions", func(t *testing.T) {
			s1 := plugin.Spec{
				Name:        "test",
				SpecVersion: 1,
			}
			err := s1.Validate()
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "plugin versions are required")
		})
		t.Run("returns error on missing default image", func(t *testing.T) {
			s1 := plugin.Spec{
				Name:        "test",
				SpecVersion: 1,
				PluginVersion: map[string]plugin.VersionDetails{
					plugin.DefaultVersion: {},
				},
			}
			err := s1.Validate()
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "default image is required")
		})
	})
}
