package utils_test

import (
	"testing"

	"github.com/goto/optimus/internal/utils"
)

func TestContainsString(t *testing.T) {
	type args struct {
		s []string
		v string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "should found an item if present",
			args: args{
				s: []string{"a", "abc", "d"},
				v: "abc",
			},
			want: true,
		},
		{
			name: "should not found an item if not present",
			args: args{
				s: []string{"a", "abc", "d"},
				v: "abcd",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := utils.ContainsString(tt.args.s, tt.args.v); got != tt.want {
				t.Errorf("ContainsString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCompareStringSlices(t *testing.T) {
	type args struct {
		newItems      []string
		existingItems []string
	}
	tests := []struct {
		name         string
		args         args
		wantToAdd    []string
		wantToRemove []string
	}{
		{
			name: "should return items to add and remove correctly",
			args: args{
				newItems:      []string{"a", "b", "c"},
				existingItems: []string{"b", "c", "d"},
			},
			wantToAdd:    []string{"a"},
			wantToRemove: []string{"d"},
		},
		{
			name: "should return empty slices if no changes",
			args: args{
				newItems:      []string{"a", "b", "c"},
				existingItems: []string{"a", "b", "c"},
			},
			wantToAdd:    []string{},
			wantToRemove: []string{},
		},
		{
			name: "should return all new items if existing is empty",
			args: args{
				newItems:      []string{"a", "b", "c"},
				existingItems: []string{},
			},
			wantToAdd:    []string{"a", "b", "c"},
			wantToRemove: []string{},
		},
		{
			name: "should return all existing items to remove if new is empty",
			args: args{
				newItems:      []string{},
				existingItems: []string{"a", "b", "c"},
			},
			wantToAdd:    []string{},
			wantToRemove: []string{"a", "b", "c"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			toAdd, toRemove := utils.CompareStringSlices(tt.args.newItems, tt.args.existingItems)
			if !equalSlices(toAdd, tt.wantToAdd) {
				t.Errorf("CompareStringSlices() gotToAdd = %v, want %v", toAdd, tt.wantToAdd)
			}
			if !equalSlices(toRemove, tt.wantToRemove) {
				t.Errorf("CompareStringSlices() gotToRemove = %v, want %v", toRemove, tt.wantToRemove)
			}
		})
	}
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[string]int)
	for _, v := range a {
		counts[v]++
	}
	for _, v := range b {
		if counts[v] == 0 {
			return false
		}
		counts[v]--
	}
	return true
}
