package alerts

import (
	"encoding/json"

	"github.com/goto/optimus/ext/notify/alertmanager"
)

// AlertLog represents the structure received from the alert system
type AlertLog struct {
	Project  string          `json:"project"`
	Data     json.RawMessage `json:"data"`
	Template string          `json:"template"`
	Labels   json.RawMessage `json:"labels"`
	Endpoint string          `json:"endpoint"`
	Status   string          `json:"status"`
	Message  string          `json:"message"`
}

func toDBSpec(payload *alertmanager.AlertPayload) (*AlertLog, error) {
	dataJSON, err := json.Marshal(payload.Data)
	if err != nil {
		return nil, err
	}

	labelsJSON, err := json.Marshal(payload.Labels)
	if err != nil {
		return nil, err
	}

	return &AlertLog{
		Project:  payload.Project,
		Data:     dataJSON,
		Template: payload.Template,
		Labels:   labelsJSON,
		Endpoint: payload.Endpoint,
	}, nil
}
