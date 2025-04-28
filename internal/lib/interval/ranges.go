package interval

import "time"

type Data[V any] struct {
	In   Interval
	Data V
}

type Range[V any] []Data[V]

func (r Range[V]) Values() []V {
	var vs []V
	for _, v := range r {
		vs = append(vs, v.Data)
	}
	return vs
}

// UpdateDataFrom can be simplified further, but as this is a transient fix
// we will roll out a proper update to this to match range more efficiently
func (r Range[V]) UpdateDataFrom(r2 Range[V]) Range[V] {
	r3 := Range[V]{}
	for _, d1 := range r {
		d2, ok := find(d1.In.start, r2)
		if !ok {
			r3 = append(r3, d1)
			continue
		}

		if d2.In.Equal(d1.In) {
			r3 = append(r3, d2)
		} else {
			r3 = append(r3, d1)
		}
	}
	return r3
}

func find[V any](start time.Time, vs []Data[V]) (Data[V], bool) {
	for _, v := range vs {
		if v.In.start.Equal(start) {
			return v, true
		}
	}
	return Data[V]{}, false
}
