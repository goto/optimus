package graph_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/internal/lib/graph"
	tree2 "github.com/goto/optimus/internal/lib/tree"
)

type str string

func String(s string) fmt.Stringer {
	return str(s)
}

func (s str) String() string {
	return string(s)
}

type testNode struct {
	Name string
}

func (t testNode) GetName() string {
	return t.Name
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

func BenchmarkGraph(bn *testing.B) {
	bn.Run("MultiRootTree", func(bn *testing.B) {
		a := tree2.NewTreeNode(testNode{Name: "A"})
		b := tree2.NewTreeNode(testNode{Name: "B"})
		c := tree2.NewTreeNode(testNode{Name: "C"})
		d := tree2.NewTreeNode(testNode{Name: "D"})
		e := tree2.NewTreeNode(testNode{Name: "E"})
		f := tree2.NewTreeNode(testNode{Name: "F"})
		g := tree2.NewTreeNode(testNode{Name: "G"})
		h := tree2.NewTreeNode(testNode{Name: "H"})

		multiRootTree := tree2.NewMultiRootTree()
		multiRootTree.AddNode(a)
		multiRootTree.AddNode(b)
		multiRootTree.AddNode(c)
		multiRootTree.AddNode(d)
		multiRootTree.AddNode(e)
		multiRootTree.AddNode(f)
		multiRootTree.AddNode(g)
		multiRootTree.AddNode(h)

		b.AddDependent(a)
		c.AddDependent(b)
		d.AddDependent(c)
		e.AddDependent(d)
		b.AddDependent(e)
		h.AddDependent(c)
		f.AddDependent(c)
		g.AddDependent(f)
		e.AddDependent(g)

		for i := 0; i < bn.N; i++ {
			_, _ = multiRootTree.ValidateCyclic()
		}
	})
	bn.Run("Graph", func(bn *testing.B) {
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

		for i := 0; i < bn.N; i++ {
			_ = grp.GetCyclics(a)
		}
	})
}
