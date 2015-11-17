package graph

import (
	"fmt"
)

import (
	"github.com/timtadh/data-structures/errors"
	"github.com/timtadh/goiso"
)

import (
	"github.com/timtadh/sfp/lattice"
)



type Node struct {
	name []byte
	sgs []*goiso.SubGraph
}


func (n *Node) Save(dt *Graph) error {
	return errors.Errorf("unimplemented")
}

func (n *Node) String() string {
	return fmt.Sprintf("<Node %v>", n.sgs[0].Label())
}

func (n *Node) StartingPoint() bool {
	return n.Size() == 1
}

func (n *Node) Size() int {
	return n.items.Size()
}

func (n *Node) Parents(support int, dtype lattice.DataType) ([]lattice.Node, error) {
	if n.items.Size() == 1 {
		return []lattice.Node{}, nil
	}
	dt := dtype.(*ItemSets)
	i := setToInt32s(n.items)
	if has, err := dt.ParentCount.Has(i); err != nil {
		return nil, err
	} else if has {
		return n.cached(dt, dt.Parents, i)
	}
	parents := make([]*set.SortedSet, 0, n.items.Size())
	for item, next := n.items.Items()(); next != nil; item, next = next() {
		parent := n.items.Copy()
		parent.Delete(item)
		parents = append(parents, parent)
	}
	nodes := make([]lattice.Node, 0, 10)
	for _, items := range parents {
		if node, err := TryLoadNode(setToInt32s(items), dt); err != nil {
			return nil, err
		} else if node != nil {
			nodes = append(nodes, node)
			continue
		}
		var txs types.Set
		for item, next := items.Items()(); next != nil; item, next = next() {
			mytxs := set.NewSortedSet(len(n.txs)+10)
			err := dt.InvertedIndex.DoFind(int32(item.(types.Int32)), func(item, tx int32) error {
				return mytxs.Add(types.Int32(tx))
			})
			if err != nil {
				return nil, err
			}
			if txs == nil {
				txs = mytxs
			} else {
				txs, err = txs.Intersect(mytxs)
				if err != nil {
					return nil, err
				}
			}
		}
		stxs := make([]int32, 0, txs.Size())
		for item, next := txs.Items()(); next != nil; item, next = next() {
			stxs = append(stxs, int32(item.(types.Int32)))
		}
		node := &Node{items, stxs}
		err := node.Save(dt)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	err := n.cache(dt.ParentCount, dt.Parents, i, nodes)
	if err != nil {
		return nil, err
	}
	return nodes, nil
}

func (n *Node) Children(support int, dtype lattice.DataType) ([]lattice.Node, error) {
	dt := dtype.(*ItemSets)
	i := setToInt32s(n.items)
	if has, err := dt.ChildCount.Has(i); err != nil {
		return nil, err
	} else if has {
		return n.cached(dt, dt.Children, i)
	}
	exts := make(map[int32][]int32)
	for _, tx := range n.txs {
		err := dt.Index.DoFind(tx,
			func(tx, item int32) error {
				if !n.items.Has(types.Int32(item)) {
					exts[item] = append(exts[item], tx)
				}
				return nil
			})
		if err != nil {
			return nil, err
		}
	}
	nodes := make([]lattice.Node, 0, 10)
	for item, txs := range exts {
		if len(txs) >= support && !n.items.Has(types.Int32(item)) {
			items := n.items.Copy()
			items.Add(types.Int32(item))
			node := &Node{
				items: items,
				txs: txs,
			}
			err := node.Save(dt)
			if err != nil {
				return nil, err
			}
			nodes = append(nodes, node)
		}
	}
	err := n.cache(dt.ChildCount, dt.Children, i, nodes)
	if err != nil {
		return nil, err
	}
	return nodes, nil
}

func (n *Node) AdjacentCount(support int, dtype lattice.DataType) (int, error) {
	pc, err := n.ParentCount(support, dtype)
	if err != nil {
		return 0, err
	}
	cc, err := n.ChildCount(support, dtype)
	if err != nil {
		return 0, err
	}
	return pc + cc, nil
}

func (n *Node) ParentCount(support int, dtype lattice.DataType) (int, error) {
	dt := dtype.(*ItemSets)
	i := setToInt32s(n.items)
	if has, err := dt.ParentCount.Has(i); err != nil {
		return 0, err
	} else if !has {
		nodes, err := n.Parents(support, dtype)
		if err != nil {
			return 0, err
		}
		return len(nodes), nil
	}
	var count int32
	err := dt.ParentCount.DoFind(i, func(_ []int32, c int32) error {
		count = c
		return nil
	})
	if err != nil {
		return 0, err
	}
	return int(count), nil
}

func (n *Node) ChildCount(support int, dtype lattice.DataType) (int, error) {
	dt := dtype.(*ItemSets)
	i := setToInt32s(n.items)
	if has, err := dt.ChildCount.Has(i); err != nil {
		return 0, err
	} else if !has {
		nodes, err := n.Children(support, dtype)
		if err != nil {
			return 0, err
		}
		return len(nodes), nil
	}
	var count int32
	err := dt.ChildCount.DoFind(i, func(_ []int32, c int32) error {
		count = c
		return nil
	})
	if err != nil {
		return 0, err
	}
	return int(count), nil
}

func (n *Node) Maximal(support int, dtype lattice.DataType) (bool, error) {
	count, err := n.ChildCount(support, dtype)
	if err != nil {
		return false, err
	}
	return count == 0, nil
}

func (n *Node) cache(counts ints_int.MultiMap, m ints_ints.MultiMap, key []int32, nodes []lattice.Node) error {
	for _, node := range nodes {
		err := m.Add(key, setToInt32s(node.(*Node).items))
		if err != nil {
			return err
		}
	}
	return counts.Add(key, int32(len(nodes)))
}

func (n *Node) cached(dt *ItemSets, m ints_ints.MultiMap, key []int32) (nodes []lattice.Node, _ error) {
	nodes = make([]lattice.Node, 0, 10)
	doerr := m.DoFind(key,
		func(_, value []int32) error {
			node, err := LoadNode(value, dt)
			if err != nil {
				return err
			}
			nodes = append(nodes, node)
			return nil
		})
	if doerr != nil {
		return nil, doerr
	}
	return nodes, nil
}

func (n *Node) Label() []byte {
	size := uint32(n.items.Size())
	bytes := make([]byte, 4*(size + 1))
	binary.BigEndian.PutUint32(bytes[0:4], size)
	s := 4
	e := s + 4
	for item, next := n.items.Items()(); next != nil; item, next = next() {
		binary.BigEndian.PutUint32(bytes[s:e], uint32(int32(item.(types.Int32))))
		s += 4
		e = s + 4
	}
	return bytes
}

func (n *Node) Embeddings() ([]lattice.Embedding, error) {
	embeddings := make([]lattice.Embedding, 0, len(n.txs))
	for _, tx := range n.txs {
		embeddings = append(embeddings, &Embedding{tx:tx})
	}
	return embeddings, nil
}

func (n *Node) Lattice(support int, dtype lattice.DataType) (*lattice.Lattice, error) {
	return nil, &lattice.NoLattice{}
}

func (e *Embedding) Components() ([]int, error) {
	return []int{int(e.tx)}, nil
}

