package jsonschema

import (
	"embed"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

//go:embed job.json resource.json optimus.json presets.json
var embedded embed.FS

func GetCompiler() (*jsonschema.Compiler, error) {
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)

	compiler.UseLoader(jsonschema.SchemeURLLoader{
		"embed": newJSONSchemaEmbedLoader(),
	})
	return compiler, nil
}

type EmbedLoader struct {
}

func newJSONSchemaEmbedLoader() *EmbedLoader {
	return &EmbedLoader{}
}

func (l EmbedLoader) Load(url string) (any, error) {
	fileName := strings.TrimPrefix(url, "embed://")
	jobSchema, err := embedded.ReadFile(fileName)
	if err != nil {
		return nil, err
	}
	return jsonschema.UnmarshalJSON(strings.NewReader(string(jobSchema)))
}
