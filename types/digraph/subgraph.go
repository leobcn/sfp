package digraph

import (
	"fmt"
	"encoding/binary"
	"strings"
)

import (
	"github.com/timtadh/data-structures/errors"
	"github.com/timtadh/goiso"
)


type SubGraph struct {
	V []Vertex
	E []Edge
	Adj [][]int
}

type Vertex struct {
	Idx int
	Color int
}

type Edge struct {
	Src, Targ, Color int
}

func NewSubGraph(sg *goiso.SubGraph) *SubGraph {
	pat := &SubGraph{
		V: make([]Vertex, len(sg.V)),
		E: make([]Edge, len(sg.E)),
		Adj: make([][]int, len(sg.V)),
	}
	for i := range sg.V {
		pat.V[i].Idx = i
		pat.V[i].Color = sg.V[i].Color
		pat.Adj[i] = make([]int, 0, 5)
	}
	for i := range sg.E {
		pat.E[i].Src = sg.E[i].Src
		pat.E[i].Targ = sg.E[i].Targ
		pat.E[i].Color = sg.E[i].Color
		pat.Adj[pat.E[i].Src] = append(pat.Adj[pat.E[i].Src], i)
		pat.Adj[pat.E[i].Targ] = append(pat.Adj[pat.E[i].Targ], i)
	}
	return pat
}

func (sg *SubGraph) Embeddings(dt *Graph) ([]*goiso.SubGraph, error) {
	if len(sg.V) == 0 {
		return nil, nil
	}
	startIdx := sg.LeastCommonVertex(dt)
	chain := sg.EdgeChainFrom(startIdx)
	cur, err := sg.VertexEmbeddings(dt, startIdx)
	if err != nil {
		return nil, err
	}
	for _, e := range chain {
		next := make([]*goiso.SubGraph, 0, len(cur))
		for _, emb := range cur {
			for _, ext := range sg.ExtendEmbedding(dt, emb, e) {
				next = append(next, ext)
			}
		}
		cur = next
	}
	return cur, nil
}

func (sg *SubGraph) LeastCommonVertex(dt *Graph) int {
	minFreq := -1
	minIdx := -1
	for i := range sg.V {
		f := dt.G.ColorFrequency(sg.V[i].Color)
		if f < minFreq || minIdx == -1 {
			minFreq = f
			minIdx = i
		}
	}
	return minIdx
}

func (sg *SubGraph) VertexEmbeddings(dt *Graph, idx int) ([]*goiso.SubGraph, error) {
	embs := make([]*goiso.SubGraph, 0, dt.G.ColorFrequency(sg.V[idx].Color))
	err := dt.ColorMap.DoFind(int32(sg.V[idx].Color), func(color, dtIdx int32) error {
		sg, _ := dt.G.VertexSubGraph(int(dtIdx))
		embs = append(embs, sg)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return embs, nil
}

// this is really a depth first search from the given idx
func (sg *SubGraph) EdgeChainFrom(idx int) []*Edge {
	edges := make([]*Edge, 0, len(sg.E))
	added := make(map[int]bool, len(sg.E))
	seen := make(map[int]bool, len(sg.V))
	var visit func(int)
	visit = func(u int) {
		seen[u] = true
		for _, e := range sg.Adj[u] {
			if !added[e] {
				added[e] = true
				edges = append(edges, &sg.E[e])
			}
		}
		for _, e := range sg.Adj[u] {
			v := sg.E[e].Targ
			if !seen[v] {
				visit(v)
			}
		}
	}
	visit(idx)
	return edges
}

func (sg *SubGraph) ExtendEmbedding(dt *Graph, cur *goiso.SubGraph, e *Edge) []*goiso.SubGraph {
	exts := make([]*goiso.SubGraph, 0, 10)
	for _, src := range sg.findSrcs(cur, e) {
		for _, ke := range sg.findEdgesFromSrc(dt, cur, src, e) {
			exts = append(exts, cur.EdgeExtend(ke))
		}
	}
	return exts, nil
}

func (sg *SubGraph) findSrcs(cur *goiso.SubGraph, e *Edge) []int {
	color := sg.V[e.Src].Color
	srcs := make([]int, 0, 10)
	for i := range cur.V {
		if cur.V[i].Color == color {
			srcs = append(srcs, i)
		}
	}
	return srcs
}

func (sg *SubGraph) findEdgesFromSrc(dt *Graph, cur *goiso.SubGraph, src int, e *Edge) []*goiso.Edge {
	srcDtIdx := cur.V[src].Id
	tcolor := sg.V[e.Targ].Color
	ecolor := e.Color
	edges := make([]*goiso.Edge, 0, 10)
	for _, ke := range dt.G.Kids[srcDtIdx] {
		if ke.Color != ecolor {
			continue
		} else if dt.G.V[ke.Targ].Color != tcolor {
			continue
		}
		if !cur.HasEdge(ke) {
			edges = append(edges, ke)
		}
	}
	return edges
}

func (sg *SubGraph) Label() []byte {
	size := 8 + len(sg.V)*4 + len(sg.E)*12
	label := make([]byte, size)
	binary.BigEndian.PutUint32(label[0:4], uint32(len(sg.E)))
	binary.BigEndian.PutUint32(label[4:8], uint32(len(sg.V)))
	off := 8
	for i, v := range sg.V {
		s := off + i*4
		e := s + 4
		binary.BigEndian.PutUint32(label[s:e], uint32(v.Color))
	}
	off += len(sg.V)*4
	for i, edge := range sg.E {
		s := off + i*12
		e := s + 4
		binary.BigEndian.PutUint32(label[s:e], uint32(edge.Src))
		s += 4
		e += 4
		binary.BigEndian.PutUint32(label[s:e], uint32(edge.Targ))
		s += 4
		e += 4
		binary.BigEndian.PutUint32(label[s:e], uint32(edge.Color))
	}
	return label
}

func (sg *SubGraph) String() string {
	V := make([]string, 0, len(sg.V))
	E := make([]string, 0, len(sg.E))
	for _, v := range sg.V {
		V = append(V, fmt.Sprintf(
			"(%v:%v)",
			v.Idx,
			v.Color,
		))
	}
	for _, e := range sg.E {
		E = append(E, fmt.Sprintf(
			"[%v->%v:%v]",
			e.Src,
			e.Targ,
			e.Color,
		))
	}
	return fmt.Sprintf("{%v:%v}%v%v", len(sg.E), len(sg.V), strings.Join(V, ""), strings.Join(E, ""))
}



