package jsonschema

import (
	"embed"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

//go:embed job.json resource.json optimus.json presets.json
var embedded embed.FS

type EmbedLoader struct{}

func newJSONSchemaEmbedLoader() *EmbedLoader {
	return &EmbedLoader{}
}

func (EmbedLoader) Load(url string) (any, error) {
	fileName := strings.TrimPrefix(url, "embed://")
	jobSchema, err := embedded.ReadFile(fileName)
	if err != nil {
		return nil, err
	}
	return jsonschema.UnmarshalJSON(strings.NewReader(string(jobSchema)))
}

func GetCompiler() *jsonschema.Compiler {
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)

	compiler.UseLoader(jsonschema.SchemeURLLoader{
		"embed": newJSONSchemaEmbedLoader(),
	})
	return compiler
}
