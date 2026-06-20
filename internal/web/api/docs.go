package api

import (
	"context"
	"embed"

	apigen "github.com/21S1298001/Mahiron5/internal/web/api/gen"
	"sigs.k8s.io/yaml"
)

//go:embed api.yml
var f embed.FS

func GetApiDocumentation(ctx context.Context, h *Handler) (apigen.GetApiDocumentationRes, error) {
	res := &apigen.GetApiDocumentationOK{}

	schemaYaml, err := f.ReadFile("api.yml")
	if err != nil {
		return nil, err
	}

	schema, err := yaml.YAMLToJSON(schemaYaml)
	if err != nil {
		return nil, err
	}

	err = res.UnmarshalJSON(schema)
	if err != nil {
		return nil, err
	}

	return res, nil
}
