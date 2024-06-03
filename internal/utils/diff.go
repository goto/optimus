package utils

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"reflect"
	"strconv"
	"strings"
)

const (
	strLenThreshold     = 250
	maxStringDiffLength = 3000
)

type Diff struct {
	Field           string
	diffTypeUnified bool
	Value1, Value2  interface{}
}

func (d *Diff) SetDiffTypeUnified() {
	d.diffTypeUnified = true
}

func (d *Diff) IsDiffTypeUnified() bool {
	return d.diffTypeUnified
}

type AttributePath string

func (a AttributePath) Add(propName string) AttributePath {
	if a == "" {
		return AttributePath(propName)
	}
	return AttributePath(string(a) + "." + propName)
}

func (a AttributePath) String() string {
	return string(a)
}

func getHash(s string) uint64 {
	h := fnv.New64()
	h.Write([]byte(s))
	return h.Sum64()
}

type elementTracker[V any] struct {
	index int
	item  V
}

type CmpOptions struct {
	IgnoreOrderList bool
}

func compareLargeStrings(prefix AttributePath, text1, text2 string) (stringDiff []Diff) {
	defer func() {
		if r := recover(); r != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Recovered in compareLargeStrings err:%v\n", r)
		}
	}()

	unifiedDiff := GetMyersDiff(strings.Split(text1, "\n"), strings.Split(text2, "\n"), 2)

	if len(unifiedDiff) > maxStringDiffLength {
		unifiedDiff = unifiedDiff[:maxStringDiffLength] + "\n ...\n Diff Truncated due to huge size..."
	}
	stringDiff = append(stringDiff, Diff{
		Field:           prefix.String(),
		diffTypeUnified: true,
		Value1:          "",
		Value2:          unifiedDiff,
	})
	return
}

func compareString(prefix AttributePath, text1, text2 string) []Diff {
	if !(text1 != text2) {
		return nil
	}
	if len(text1) > strLenThreshold || len(text2) > strLenThreshold {
		stringDiff := compareLargeStrings(prefix, text1, text2)
		if stringDiff != nil {
			return stringDiff
		}
		return []Diff{{
			Field:  prefix.String(),
			Value1: text1[10:],
			Value2: text2[10:],
		}}
	}
	return []Diff{{
		Field:  prefix.String(),
		Value1: text1,
		Value2: text2,
	}}
}

// compareList remove all unchanged items and return an associative array
func compareList[V any](prefix AttributePath, list1, list2 []V, opt *CmpOptions) []Diff { //nolint
	list1Dict := make(map[uint64]elementTracker[V])
	list2Dict := make(map[uint64]elementTracker[V])
	// make map of all items that are there in list1
	for i, item := range list1 {
		list1Dict[getHash(fmt.Sprintf("%v", item))] = elementTracker[V]{
			index: i,
			item:  item,
		}
	}
	// find items that are there in list 2 but not in list 1
	for i, item := range list2 {
		hash := getHash(fmt.Sprintf("%v", item))
		if v, ok := list1Dict[hash]; ok {
			if opt != nil && opt.IgnoreOrderList {
				// delete unmodified items that are even out of order
				delete(list1Dict, hash)
				continue
			}
			if v.index == i {
				delete(list1Dict, hash)
				continue
			}
		}
		list2Dict[getHash(fmt.Sprintf("%v", item))] = elementTracker[V]{
			index: i,
			item:  item,
		}
	}
	oldList := make(map[string]V)
	newList := make(map[string]V)
	for _, element := range list1Dict {
		oldList[strconv.Itoa(element.index)] = element.item
	}
	for _, element := range list2Dict {
		newList[strconv.Itoa(element.index)] = element.item
	}
	if len(oldList) == 0 && len(newList) == 0 {
		return nil
	}
	oldListStr, _ := json.Marshal(oldList)
	newListStr, _ := json.Marshal(newList)
	return []Diff{{
		Field:  prefix.Add("indicesModified").String(),
		Value1: string(oldListStr),
		Value2: string(newListStr),
	}}
}

func nestedMapDiff(prefix AttributePath, map1, map2 reflect.Value, opt *CmpOptions) []Diff { //nolint
	var diffs []Diff
	map1Keys := map1.MapKeys()
	map2Keys := map2.MapKeys()

	map1Set := make(map[interface{}]bool)
	map2Set := make(map[interface{}]bool)

	for _, k := range map1Keys {
		map1Set[k.Interface()] = true
	}

	for _, k := range map2Keys {
		map2Set[k.Interface()] = true
	}

	for _, k := range map1Keys {
		key := k.Interface()
		qualifiedField := prefix.Add(fmt.Sprintf("%v", key))
		v1 := map1.MapIndex(k).Interface()
		v2 := map2.MapIndex(k).Interface()
		if _, ok := map2Set[key]; ok {
			if reflect.TypeOf(v1) == reflect.TypeOf(v2) {
				switch {
				case reflect.ValueOf(v1).Kind() == reflect.Map:
					nestedMap1 := reflect.ValueOf(v1)
					nestedMap2 := reflect.ValueOf(v2)
					iterDiffs := nestedMapDiff(qualifiedField, nestedMap1, nestedMap2, opt)
					diffs = append(diffs, iterDiffs...)
					continue
				case reflect.ValueOf(v1).Kind() == reflect.Slice:
					diffs = append(diffs, compareList[any](qualifiedField, v1.([]any), v2.([]any), opt)...)
					continue
				case reflect.ValueOf(v1).Kind() == reflect.String:
					diffs = append(diffs, compareString(qualifiedField, v1.(string), v2.(string))...)
					continue
				case !reflect.DeepEqual(v1, v2):
					diffs = append(diffs, Diff{Field: qualifiedField.String(), Value1: v1, Value2: v2})
					continue
				}
			} else {
				diffs = append(diffs, Diff{Field: qualifiedField.String(), Value1: v1, Value2: v2})
			}
		} else {
			diffs = append(diffs, Diff{Field: qualifiedField.String(), Value1: v1, Value2: nil})
		}
	}

	for _, k := range map2Keys {
		key := k.Interface()
		qualifiedField := prefix.Add(fmt.Sprintf("%v", key))
		if _, ok := map1Set[key]; !ok {
			v2 := map2.MapIndex(k).Interface()
			diffs = append(diffs, Diff{Field: qualifiedField.String(), Value1: nil, Value2: v2})
		}
	}

	return diffs
}

func unMarshalRawJson(j json.RawMessage) (map[string]interface{}, error) {
	var val map[string]interface{}
	err := json.Unmarshal(j, &val)
	if err != nil {
		return nil, err
	}
	return val, nil
}

// GetDiffs compares two interfaces and returns a slice of Diff containing differences. Does not compare unexported attributes
func GetDiffs(i1, i2 interface{}, opt *CmpOptions) ([]Diff, error) {
	defer func() {
		if r := recover(); r != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Recovered in GetDiffs err:%v\n", r)
		}
	}()
	k, err := json.Marshal(i1)
	if err != nil {
		return nil, err
	}
	l, err := json.Marshal(i2)
	if err != nil {
		return nil, err
	}
	i1map, err := unMarshalRawJson(k)
	if err != nil {
		return nil, err
	}
	i2map, err := unMarshalRawJson(l)
	if err != nil {
		return nil, err
	}
	return nestedMapDiff("", reflect.ValueOf(i1map), reflect.ValueOf(i2map), opt), nil
}
