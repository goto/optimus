package utils

import "regexp"

func FindGroupsAll(re1, re2 *regexp.Regexp, input string) []string {
	var results []string

	collect := func(re *regexp.Regexp) {
		matches := re.FindAllStringSubmatch(input, -1)
		for _, match := range matches {
			if len(match) > 1 {
				results = append(results, match[1:]...) // collect all groups
			}
		}
	}

	collect(re1)
	collect(re2)

	return results
}

func GetDistinctStrings(input []string) []string {
	distinctMap := make(map[string]struct{})
	for _, v := range input {
		distinctMap[v] = struct{}{}
	}
	var res []string
	for v := range distinctMap {
		res = append(res, v)
	}
	return res
}

func RemoveFromStringArray(input, toRemove []string) []string {
	toRemoveMap := make(map[string]struct{})
	for _, v := range toRemove {
		toRemoveMap[v] = struct{}{}
	}

	var res []string
	for _, v := range input {
		if _, found := toRemoveMap[v]; !found {
			res = append(res, v)
		}
	}
	return res
}
