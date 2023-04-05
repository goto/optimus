package event

import "github.com/goto/optimus/core/resource"

type ResourceCreated struct {
	Event

	resource resource.Resource
}
