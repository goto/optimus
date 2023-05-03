package graph_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/internal/lib/graph"
)

type str string

func String(s string) fmt.Stringer {
	return str(s)
}

func (s str) String() string {
	return string(s)
}

func TestGraph(t *testing.T) {
	t.Run("Traverse", func(t *testing.T) {
		// A->B-
		//  \    \
		//    ->C- ->D
		//  /
		// E

		// Traverse
		// A->B->D->C
		// E->C->D
		// B->D
		grp := graph.NewGraph[fmt.Stringer]()
		a := String("A")
		b := String("B")
		c := String("C")
		d := String("D")
		e := String("E")
		grp.AddNode(a, b)
		grp.AddNode(a, c)
		grp.AddNode(c, d)
		grp.AddNode(e, c)
		grp.AddNode(b, d)

		assert.Equal(t, []fmt.Stringer{a, b, d, c}, grp.Traverse(a))
		assert.Equal(t, []fmt.Stringer{e, c, d}, grp.Traverse(e))
		assert.Equal(t, []fmt.Stringer{b, d}, grp.Traverse(b))
	})
	t.Run("IsCyclic", func(t *testing.T) {
		grp := graph.NewGraph[fmt.Stringer]()
		a := String("A")
		b := String("B")
		c := String("C")
		d := String("D")
		e := String("E")
		f := String("F")
		g := String("G")
		h := String("H")
		grp.AddNode(a, b)
		grp.AddNode(b, c)
		grp.AddNode(c, d)
		grp.AddNode(c, h)
		grp.AddNode(c, f)
		grp.AddNode(d, e)
		grp.AddNode(e, b)
		grp.AddNode(f, g)
		grp.AddNode(f, h)
		grp.AddNode(g, e)

		assert.Equal(t, [][]fmt.Stringer{{b, c, d, e}, {b, c, f, g, e}}, grp.GetCyclics(a))
	})
}
