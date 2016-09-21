package digraph

import ()

import ()

type IdColorColor struct {
	Id, EdgeColor, VertexColor int
}

type Colors struct {
	SrcColor, TargColor, EdgeColor int
}

type Indices struct {
	G               *Digraph
	ColorIndex      map[int][]int          // Colors -> []Idx in G.V
	SrcIndex        map[IdColorColor][]int // (SrcIdx, EdgeColor, TargColor) -> TargIdx (where Idx in G.V)
	TargIndex       map[IdColorColor][]int // (TargIdx, EdgeColor, SrcColor) -> SrcIdx (where Idx in G.V)
	EdgeIndex       map[Edge]*Edge
	EdgeCounts      map[Colors]int         // (src-color, targ-color, edge-color) -> count
	FreqEdges       []Colors               // frequent color triples
	EdgesFromColor  map[int][]Colors       // freq src-colors -> color triples
	EdgesToColor    map[int][]Colors       // freq targ-colors -> color triples
}

func NewIndices(b *Builder, minSupport int) *Indices {
	i := &Indices{
		ColorIndex:     make(map[int][]int, len(b.VertexColors)),
		SrcIndex:       make(map[IdColorColor][]int, len(b.E)),
		TargIndex:      make(map[IdColorColor][]int, len(b.E)),
		EdgeIndex:      make(map[Edge]*Edge, len(b.E)),
		EdgeCounts:     make(map[Colors]int, len(b.E)),
		FreqEdges:      make([]Colors, 0, len(b.E)),
		EdgesFromColor: make(map[int][]Colors, len(b.VertexColors)),
		EdgesToColor:   make(map[int][]Colors, len(b.VertexColors)),
	}
	i.G = b.Build(
		func(u *Vertex) {
			if i.ColorIndex[u.Color] == nil {
				i.ColorIndex[u.Color] = make([]int, 0, b.VertexColors[u.Color])
			}
			i.ColorIndex[u.Color] = append(i.ColorIndex[u.Color], u.Idx)
		},
		func(e *Edge) {
			edge := Edge{Src: e.Src, Targ: e.Targ, Color: e.Color}
			srcKey := IdColorColor{e.Src, e.Color, b.V[e.Targ].Color}
			targKey := IdColorColor{e.Targ, e.Color, b.V[e.Src].Color}
			colorKey := Colors{b.V[e.Src].Color, b.V[e.Targ].Color, e.Color}
			if i.SrcIndex[srcKey] == nil {
				i.SrcIndex[srcKey] = make([]int, 0, 10)
			}
			if i.TargIndex[targKey] == nil {
				i.TargIndex[targKey] = make([]int, 0, 10)
			}
			if i.EdgesFromColor[e.Color] == nil {
				i.EdgesFromColor[e.Color] = make([]Colors, 0, 10)
			}
			if i.EdgesToColor[e.Color] == nil {
				i.EdgesToColor[e.Color] = make([]Colors, 0, 10)
			}
			i.EdgeIndex[edge] = e
			i.SrcIndex[srcKey] = append(i.SrcIndex[srcKey], e.Targ)
			i.TargIndex[targKey] = append(i.TargIndex[targKey], e.Src)
			i.EdgeCounts[colorKey] += 1
			// only add to frequent edges exactly when this colorKey has
			// surpassed min_support.
			if i.EdgeCounts[colorKey] == minSupport {
				i.FreqEdges = append(i.FreqEdges, colorKey)
				i.EdgesFromColor[colorKey.SrcColor] = append(
					i.EdgesFromColor[colorKey.SrcColor],
					colorKey)
				i.EdgesToColor[colorKey.TargColor] = append(
					i.EdgesToColor[colorKey.TargColor],
					colorKey)
			}
		})
	return i
}

func (i *Indices) Colors(e *Edge) Colors {
	return Colors{
		SrcColor: i.G.V[e.Src].Color,
		TargColor: i.G.V[e.Targ].Color,
		EdgeColor: e.Color,
	}
}

// From an sg.V.id, get the degree of that vertex in the graph.
// so the id is really a Graph Idx
func (i *Indices) Degree(id int) int {
	return len(i.G.Adj[id])
	// return len(i.G.Kids[id]) + len(i.G.Parents[id])
}

func (i *Indices) InDegree(id int) int {
	panic("re-implement")
	// return len(i.G.Parents[id])
}

func (i *Indices) OutDegree(id int) int {
	panic("re-implement")
	// return len(i.G.Kids[id])
}

func (indices *Indices) HasEdge(srcId, targId, color int) bool {
	_, has := indices.EdgeIndex[Edge{Src: srcId, Targ: targId, Color: color}]
	return has
}

func (indices *Indices) TargsFromSrc(srcId, edgeColor, targColor int, exclude func(int) bool, do func(int)) {
	for _, targId := range indices.SrcIndex[IdColorColor{srcId, edgeColor, targColor}] {
		if exclude != nil && exclude(targId) {
			continue
		}
		do(targId)
	}
}

func (indices *Indices) SrcsToTarg(targId, edgeColor, srcColor int, exclude func(int) bool, do func(int)) {
	for _, srcId := range indices.TargIndex[IdColorColor{targId, edgeColor, srcColor}] {
		if exclude != nil && exclude(srcId) {
			continue
		}
		do(srcId)
	}
}