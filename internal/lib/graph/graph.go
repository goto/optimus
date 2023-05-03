package graph

import (
	"fmt"
)

type Graph[v fmt.Stringer] interface {
	AddNode(src, dst v)
	Traverse(root v) []v
	GetCyclics(root v) [][]v
}

type graph[v fmt.Stringer] struct {
	nodes map[string]*node[v]
}

type node[v fmt.Stringer] struct {
	isRoot   bool
	element  v
	children []*node[v]
}

type stack[v fmt.Stringer] struct {
	ordered []v
	element map[string]int
}

func (s *stack[v]) isExist(elm v) bool {
	_, ok := s.element[elm.String()]
	return ok
}

func (s *stack[v]) pop() v {
	n := len(s.ordered)
	elm := s.ordered[n-1]
	s.ordered = s.ordered[:n-1]
	delete(s.element, elm.String())
	return elm
}

func (s *stack[v]) push(elm v) {
	s.ordered = append(s.ordered, elm)
	s.element[elm.String()] = len(s.ordered) - 1
}

func (s *stack[v]) getUntil(elm v) []v {
	if s.isExist(elm) {
		idx := s.element[elm.String()]
		result := make([]v, len(s.ordered)-1)
		copy(result, s.ordered[idx:len(s.ordered)])
		return result
	}
	return nil
}

func NewGraph[v fmt.Stringer]() Graph[v] {
	return &graph[v]{
		nodes: map[string]*node[v]{},
	}
}

func (g *graph[v]) AddNode(src, dst v) {
	srcNode := &node[v]{isRoot: true, element: src, children: []*node[v]{}}
	dstNode := &node[v]{isRoot: true, element: dst, children: []*node[v]{}}
	if n, ok := g.nodes[src.String()]; ok {
		srcNode = n
	}
	if n, ok := g.nodes[dst.String()]; ok {
		dstNode = n
	}
	srcNode.children = append(srcNode.children, dstNode)
	dstNode.isRoot = false

	g.nodes[src.String()] = srcNode
	g.nodes[dst.String()] = dstNode
}

func (g *graph[v]) Traverse(root v) []v {
	if _, ok := g.nodes[root.String()]; !ok {
		return nil
	}

	stk := &stack[v]{ordered: []v{}, element: map[string]int{}}
	g.traverse(root, stk)

	return stk.ordered
}

func (g *graph[v]) GetCyclics(root v) [][]v {
	if _, ok := g.nodes[root.String()]; !ok {
		return nil
	}

	stk := &stack[v]{ordered: []v{}, element: map[string]int{}}
	cyclics := [][]v{}
	g.traverseCyclic(root, &cyclics, stk)

	return cyclics
}

func (g *graph[v]) traverseCyclic(curr v, cyclics *[][]v, stk *stack[v]) {
	stk.push(curr)
	for _, child := range g.nodes[curr.String()].children {
		if !stk.isExist(child.element) {
			g.traverseCyclic(child.element, cyclics, stk)
		} else {
			*cyclics = append(*cyclics, stk.getUntil(child.element))
		}
	}
	stk.pop()
}

func (g *graph[v]) traverse(curr v, stk *stack[v]) {
	stk.push(curr)
	for _, child := range g.nodes[curr.String()].children {
		if !stk.isExist(child.element) {
			g.traverse(child.element, stk)
		}
	}
}
