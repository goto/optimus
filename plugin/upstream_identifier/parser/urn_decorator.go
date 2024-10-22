package parser

import (
	"fmt"
	"strings"
)

func BQURNDecorator(f func(string) []string) func(string) []string {
	return func(rawResource string) []string {
		resourceURNs := []string{}
		tables := f(rawResource)
		for _, table := range tables {
			tableSplitted := strings.Split(table, ".")
			if len(tableSplitted) != 3 {
				continue
			}
			resourceURN := fmt.Sprintf("bigquery://%s:%s.%s", tableSplitted[0], tableSplitted[1], tableSplitted[2])
			resourceURNs = append(resourceURNs, resourceURN)
		}
		return resourceURNs
	}
}

func MaxcomputeURNDecorator(f func(string) []string) func(string) []string {
	return func(rawResource string) []string {
		resourceURNs := []string{}
		tables := f(rawResource)
		for _, table := range tables {
			tableSplitted := strings.Split(table, ".")
			if len(tableSplitted) != 3 {
				continue
			}
			resourceURN := fmt.Sprintf("maxcompute://%s.%s.%s", tableSplitted[0], tableSplitted[1], tableSplitted[2])
			resourceURNs = append(resourceURNs, resourceURN)
		}
		return resourceURNs
	}
}
