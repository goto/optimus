package plan

type Queue[T any] []T

func NewQueue[T any]() Queue[T] { return make(Queue[T], 0) }

func (q *Queue[T]) Push(value T) {
	*q = append(*q, value)
}

func (q *Queue[T]) Pop() T {
	var front T
	if !q.Next() {
		return front
	}

	front = (*q)[0]
	*q = (*q)[1:]
	return front
}

func (q *Queue[T]) Next() bool {
	return len(*q) > 0
}
