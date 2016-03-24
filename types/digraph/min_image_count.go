package digraph

import (
	"github.com/timtadh/goiso"
)

import (
	"github.com/timtadh/sfp/types/digraph/subgraph"
)


func countMinImageSupportTill(dt *Digraph, pat *subgraph.SubGraph, till int) (supported bool, err error) {
	embeddings := make([]*goiso.SubGraph, 0, 10)
	ei, err := pat.IterEmbeddings(dt.G, dt.ColorMap, dt.Extender, func(leastCommonVertex int, chain []*subgraph.Edge) func(emb *goiso.SubGraph) bool {
		if dt.G.ColorFrequency(pat.V[leastCommonVertex].Color) < dt.Support() {
			return func(emb *goiso.SubGraph) bool { return true }
		}
		return func(emb *goiso.SubGraph) bool {
			for _, found := range embeddings {
				for i := range emb.V {
					if found.V[leastCommonVertex].Id == emb.V[i].Id {
						return true
					}
				}
			}
			return false
		}
	})
	if err != nil {
		return false, err
	}
	for emb, ei := ei(); ei != nil; emb, ei = ei() {
		embeddings = append(embeddings, emb)
		if len(embeddings) >= till {
			return true, nil
		}
	}
	return false, nil
}

