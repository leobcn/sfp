package digraph

import ()

import (
	// "github.com/timtadh/data-structures/errors"
	"github.com/timtadh/data-structures/set"
)

import (
	"github.com/timtadh/sfp/lattice"
	"github.com/timtadh/sfp/stores/bytes_bytes"
	"github.com/timtadh/sfp/stores/bytes_int"
	"github.com/timtadh/sfp/types/digraph/subgraph"
)

type Node interface {
	lattice.Node
	New(*subgraph.SubGraph, []*subgraph.Extension, []*subgraph.Embedding) Node
	Label() []byte
	Extensions() ([]*subgraph.Extension, error)
	Embeddings() ([]*subgraph.Embedding, error)
	SubGraph() *subgraph.SubGraph
	loadFrequentVertices() ([]lattice.Node, error)
	isRoot() bool
	edges() int
	dt() *Digraph
}

func precheckChildren(n Node, kidCount bytes_int.MultiMap, kids bytes_bytes.MultiMap) (has bool, nodes []lattice.Node, err error) {
	dt := n.dt()
	if n.isRoot() {
		nodes, err = n.loadFrequentVertices()
		if err != nil {
			return false, nil, err
		}
		return true, nodes, nil
	}
	if n.edges() >= dt.MaxEdges {
		return true, []lattice.Node{}, nil
	}
	if nodes, has, err := cachedAdj(n, dt, kidCount, kids); err != nil {
		return false, nil, err
	} else if has {
		// errors.Logf("DEBUG", "cached %v, %v", n, nodes)
		return true, nodes, nil
	}
	// errors.Logf("DEBUG", "not cached %v", n)
	return false, nil, nil
}

func canonChildren(n Node) (nodes []lattice.Node, err error) {
	dt := n.dt()
	if has, nodes, err := precheckChildren(n, dt.CanonKidCount, dt.CanonKids); err != nil {
		return nil, err
	} else if has {
		// errors.Logf("DEBUG", "got from precheck %v", n)
		return nodes, nil
	}
	patterns, err := extendNode(n)
	if err != nil {
		return nil, err
	}
	for i, next := patterns.Items()(); next != nil; i, next = next() {
		extPat := i.(*subgraph.SubGraph)
		if canonized, err := isCanonicalExtension(n.SubGraph(), extPat); err != nil {
			return nil, err
		} else if !canonized {
			continue
		}
		support, exts, embs, err := extsAndEmbs(dt, extPat)
		if err != nil {
			return nil, err
		}
		if support >= dt.Support() {
			nodes = append(nodes, n.New(extPat, exts, embs))
		}
	}
	// errors.Logf("DEBUG", "n %v canon-kids %v", n, len(nodes))
	return nodes, cacheAdj(dt, dt.CanonKidCount, dt.CanonKids, n.Label(), nodes)
}

func children(n Node) (nodes []lattice.Node, err error) {
	// errors.Logf("DEBUG", "")
	// errors.Logf("DEBUG", "")
	// errors.Logf("DEBUG", "")
	// errors.Logf("DEBUG", "")
	// errors.Logf("DEBUG", "n %v", n)
	dt := n.dt()
	if has, nodes, err := precheckChildren(n, dt.ChildCount, dt.Children); err != nil {
		return nil, err
	} else if has {
		// errors.Logf("DEBUG", "got from precheck %v", n)
		return nodes, nil
	}

	patterns, err := extendNode(n)
	if err != nil {
		return nil, err
	}
	for i, next := patterns.Items()(); next != nil; i, next = next() {
		pattern := i.(*subgraph.SubGraph)
		support, exts, embs, err := extsAndEmbs(dt, pattern)
		if err != nil {
			return nil, err
		}
		// errors.Logf("DEBUG", "pattern %v support %v exts %v", pattern, len(embs), len(exts))
		if support >= dt.Support() {
			nodes = append(nodes, n.New(pattern, exts, embs))
		}
	}

	// errors.Logf("DEBUG", "n %v kids %v", n, len(nodes))

	return nodes, cacheAdj(dt, dt.ChildCount, dt.Children, n.Label(), nodes)
}

func extendNode(n Node) (*set.SortedSet, error) {
	// errors.Logf("DEBUG", "n.SubGraph %v", n.SubGraph())
	sg := n.SubGraph()
	b := subgraph.Build(len(sg.V), len(sg.E)).From(sg)
	extPoints, err := n.Extensions()
	if err != nil {
		return nil, err
	}
	patterns := set.NewSortedSet(len(extPoints))
	for _, ep := range extPoints {
		// errors.Logf("DEBUG", "  ext point %v", ep)
		bc := b.Copy()
		bc.Extend(ep)
		ext := bc.Build()
		patterns.Add(ext)
		// errors.Logf("DEBUG", "    ext %v", ext)
	}

	return patterns, nil
}

/*
func children(n Node) (nodes []lattice.Node, err error) {
	dt := n.dt()
	if nodes, err := precheckChildren(n, dt.ChildCount, dt.Children); err != nil {
		return nil, err
	} else if nodes != nil {
		return nodes, nil
	}
	// errors.Logf("DEBUG", "Children of %v", n)
	exts := ext.NewCollector(dt.MaxVertices)
	add := validExtChecker(dt, func(sg *goiso.SubGraph, e *goiso.Edge) {
		dt.Extender.Extend(sg, e, exts.Ch())
	})
	embeddings, err := n.Embeddings()
	if err != nil {
		return nil, err
	}
	added := 0
	sup, err := dt.Supported(dt, embeddings)
	if err != nil {
		return nil, err
	}
	sizes := set.NewSortedSet(len(embeddings[0].V))
	for _, set := range support.VertexMapSets(embeddings) {
		sizes.Add(types.Int(set.Size()))
	}
	errors.Logf("EMBEDDINGS", "len(V) %v len(embeddings) %v supported %v unique-vertex-embeddings %v", len(embeddings[0].V), len(embeddings), len(sup), sizes)
	for _, sg := range embeddings {
		for i := range sg.V {
			u := &sg.V[i]
			for _, e := range dt.G.Kids[u.Id] {
				added += add(sg, e)
			}
			for _, e := range dt.G.Parents[u.Id] {
				added += add(sg, e)
			}
		}
	}
	return nodesFromEmbeddings(n, exts.Wait(added))
}
*/
