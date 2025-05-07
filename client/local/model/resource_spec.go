package model

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

type Deprecation struct {
	Reason           string `yaml:"reason"`
	Date             string `yaml:"date"`
	ReplacementTable string `yaml:"replacement_table"`
}

type ResourceSpec struct {
	Version     int                    `yaml:"version"`
	Name        string                 `yaml:"name"`
	Type        string                 `yaml:"type"`
	Labels      map[string]string      `yaml:"labels"`
	Spec        map[string]interface{} `yaml:"spec"`
	Path        string                 `yaml:"-"`
	Deprecation *Deprecation           `yaml:"deprecation"`
}

func (r ResourceSpec) ToProto() (*pb.ResourceSpecification, error) {
	specPb, err := structpb.NewStruct(r.Spec)
	if err != nil {
		return nil, fmt.Errorf("error constructing spec pb: %w", err)
	}

	var deprecation *pb.Deprecation
	if r.Deprecation != nil && len(r.Deprecation.Date) > 0 {
		deprecationDate, err := time.Parse(time.DateOnly, r.Deprecation.Date)
		if err != nil {
			return nil, fmt.Errorf("error parsing deprivation Date, for resource: %s, expected_format: [YYYY-MM-DD], got: [%s], err: %w", r.Name, r.Deprecation.Date, err)
		}
		deprecation = &pb.Deprecation{
			Reason:           r.Deprecation.Reason,
			Date:             timestamppb.New(deprecationDate),
			ReplacementTable: r.Deprecation.ReplacementTable,
		}
	}
	return &pb.ResourceSpecification{
		Version:     int32(r.Version),
		Name:        r.Name,
		Type:        r.Type,
		Labels:      r.Labels,
		Spec:        specPb,
		Assets:      nil, // TODO: check if we really need assets
		Deprecation: deprecation,
	}, nil
}
