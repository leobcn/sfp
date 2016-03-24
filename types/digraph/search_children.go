package digraph

import ()

import (
	"github.com/timtadh/data-structures/set"
)

import (
	"github.com/timtadh/sfp/lattice"
	"github.com/timtadh/sfp/types/digraph/subgraph"
)

func searchChildren(n *SearchNode) (nodes []lattice.Node, err error) {
	dt := n.dt()
	if nodes, err := precheckChildren(n, dt.ChildCount, dt.Children); err != nil {
		return nil, err
	} else if nodes != nil {
		return nodes, nil
	}

	b := subgraph.BuildFrom(n.Pat)
	exts := set.NewSortedSet(10)
	colors := n.Pat.V.Colors()
	for color, vidxs := range colors {
		err := dt.ColorOutEdges.DoFind(int32(color), func (_ int32, e subgraph.Edge) error {
			toColor := e.Targ
			toVidxs := colors[toColor]
			for _, fromVidx := range vidxs {
				exts.Add(
					b.Mutation(func(b *subgraph.Builder) {
						nv := b.AddVertex(toColor)
						b.AddEdge(&b.V[fromVidx], nv, e.Color)
					}).Build())
				for _, toVidx := range toVidxs {
					// need to add HasEdge Check...!!
					exts.Add(
						b.Mutation(func(b *subgraph.Builder) {
							b.AddEdge(&b.V[fromVidx], &b.V[toVidx], e.Color)
						}).Build())
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		err = dt.ColorInEdges.DoFind(int32(color), func (_ int32, e subgraph.Edge) error {
			fromColor := e.Src
			fromVidxs := colors[fromColor]
			for _, toVidx := range vidxs {
				exts.Add(
					b.Mutation(func(b *subgraph.Builder) {
						nv := b.AddVertex(fromColor)
						b.AddEdge(nv, &b.V[toVidx], e.Color)
					}).Build())
				for _, fromVidx := range fromVidxs {
					// need to add HasEdge Check...!!
					exts.Add(
						b.Mutation(func(b *subgraph.Builder) {
							b.AddEdge(&b.V[fromVidx], &b.V[toVidx], e.Color)
						}).Build())
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	for x, next := exts.Items()(); next != nil; x, next = next() {
		ext := x.(*subgraph.SubGraph)
		supported, err := countMinImageSupportTill(dt, ext, dt.Support())
		if err != nil {
			return nil, err
		} else if supported {
			nodes = append(nodes, &SearchNode{Dt: dt, Pat: ext})
		}
	}
	return nodes, nil
}
