package utils

func MergeAnyMaps(maps ...map[string]interface{}) map[string]interface{} {
	imp := map[string]interface{}{}
	for _, mp := range maps {
		for k, v := range mp {
			imp[k] = v
		}
	}
	return imp
}

// MergeMaps can merge values from multiple maps into one
// It can also create clone of a map
func MergeMaps(maps ...map[string]string) map[string]string {
	smp := map[string]string{}
	for _, mp := range maps {
		for k, v := range mp {
			smp[k] = v
		}
	}
	return smp
}

func MapToList[V any](inputMap map[string]V) []V {
	var smp []V
	for _, value := range inputMap {
		smp = append(smp, value)
	}
	return smp
}

func ListToMap(inputList []string) map[string]struct{} {
	smp := map[string]struct{}{}
	for _, value := range inputList {
		smp[value] = struct{}{}
	}
	return smp
}

func AppendToMap(gmap map[string]interface{}, mp map[string]string) {
	for k, v := range mp {
		gmap[k] = v
	}
}

func Contains[K comparable, V any](mp map[K]V, keys ...K) bool {
	for _, key := range keys {
		_, ok := mp[key]
		if !ok {
			return false
		}
	}
	return true
}

func ConfigAs[T any](mapping map[string]any, key string) T {
	var zero T
	val, ok := mapping[key]
	if ok {
		s, ok := val.(T)
		if ok {
			return s
		}
	}
	return zero
}
