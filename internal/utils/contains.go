package utils

// ContainsString returns true if a string is present in string slice
func ContainsString(s []string, v string) bool {
	for _, vv := range s {
		if vv == v {
			return true
		}
	}
	return false
}

// CompareStringSlices compares two string slices, each guaranteed to be unique and
// returns the items added to newItems & removed from existingItems
func CompareStringSlices(newItems, existingItems []string) (toAdd, toRemove []string) {
	existingSet := make(map[string]struct{}, len(existingItems))
	for _, item := range existingItems {
		existingSet[item] = struct{}{}
	}

	for _, item := range newItems {
		if _, found := existingSet[item]; found {
			delete(existingSet, item)
		} else {
			toAdd = append(toAdd, item)
		}
	}

	for item := range existingSet {
		toRemove = append(toRemove, item)
	}

	return
}
