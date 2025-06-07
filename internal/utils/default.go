package utils

func GetFirstNonEmpty[T comparable](opts ...T) T {
	var zero T

	for _, opt := range opts {
		if opt == zero {
			continue
		}
		return opt
	}
	return opts[0]
}
