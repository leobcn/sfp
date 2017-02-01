package urw

import (
	"math/rand"
)

import (
	"github.com/timtadh/data-structures/errors"
)

import (
	"github.com/timtadh/sfp/config"
	"github.com/timtadh/sfp/lattice"
	"github.com/timtadh/sfp/miners"
)

type Miner struct {
	Config *config.Config
	Pos    lattice.DataType
	Rptr   miners.Reporter
	Samples int
	MinPosSupport, MaxNegSupport int
	Neg    lattice.DataType
}

func NewMiner(conf *config.Config, samples, pos, neg int, negLoader func(lattice.PrFormatter) (lattice.DataType, lattice.Formatter)) *Miner {
	m := &Miner{
		Config: conf,
		Samples: samples,
		MinPosSupport: pos,
		MaxNegSupport: neg,
	}
	m.Neg, _ = negLoader(m.PrFormatter())
	return m
}

func (m *Miner) PrFormatter() lattice.PrFormatter {
	return nil
}

func (m *Miner) Init(dt lattice.DataType, rptr miners.Reporter) (err error) {
	errors.Logf("INFO", "about to load singleton nodes")
	m.Pos = dt
	m.Rptr = rptr
	return nil
}

func (m *Miner) Close() error {
	errors := make(chan error)
	go func() {
		errors <- m.Pos.Close()
	}()
	go func() {
		errors <- m.Neg.Close()
	}()
	go func() {
		errors <- m.Rptr.Close()
	}()
	for i := 0; i < 2; i++ {
		err := <-errors
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Miner) Mine(dt lattice.DataType, rptr miners.Reporter, fmtr lattice.Formatter) error {
	err := m.Init(dt, rptr)
	if err != nil {
		return err
	}
	errors.Logf("INFO", "finished initialization, starting walk")
	err = m.mine()
	if err != nil {
		return err
	}
	errors.Logf("INFO", "exiting Mine")
	return nil
}

func (m *Miner) mine() (err error) {
	for i := 0; i < m.Samples; i++ {
		s, err := m.walk()
		if err != nil {
			return err
		}
		if !m.Pos.Acceptable(s) {
			i--
			continue
		}
		err = m.Rptr.Report(s)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Miner) mineAll() (err error) {
	seen, err := m.Config.BytesIntMultiMap("stack-seen")
	if err != nil {
		return err
	}
	add := func(stack []lattice.Node, n lattice.Node) ([]lattice.Node, error) {
		err := seen.Add(n.Pattern().Label(), 1)
		if err != nil {
			return nil, err
		}
		return append(stack, n), nil
	}
	pop := func(stack []lattice.Node) ([]lattice.Node, lattice.Node) {
		return stack[:len(stack)-1], stack[len(stack)-1]
	}
	stack := make([]lattice.Node, 0, 10)
	stack, err = add(stack, m.Pos.Root())
	for len(stack) > 0 {
		var n lattice.Node
		stack, n = pop(stack)
		errors.Logf("DEBUG", "cur %v", n)
		kids, err := m.filterNegs(n.Children())
		if err != nil {
			return err
		}
		max := len(kids) == 0
		if max && m.Pos.Acceptable(n) {
			err = m.Rptr.Report(n)
			if err != nil {
				return err
			}
		}
		for _, k := range kids {
			if has, err := seen.Has(k.Pattern().Label()); err != nil {
				return err
			} else if !has {
				stack, err = add(stack, k)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
func (m *Miner) walk() (max lattice.Node, err error) {
	cur := m.Pos.Root()
	prev := cur
	for cur != nil {
		errors.Logf("DEBUG", "cur %v", cur)
		next, err := uniform(m.filterNegs(cur.Children()))
		if err != nil {
			return nil, err
		}
		prev = cur
		cur = next
	}
	return prev, nil
}

func (m *Miner) filterNegs(slice []lattice.Node, err error) ([]lattice.Node, error) {
	if err != nil {
		return nil, err
	}
	filtered := make([]lattice.Node, 0, len(slice))
	count := 0
	for _, n := range slice {
		pat := n.Pattern()
		size, negSupport, err := m.Neg.SupportOf(pat)
		if err != nil {
			return nil, err
		}
		// errors.Logf("DEBUG", "%v %v of pat %v", size, support, pat)
		if float64(size)/float64(pat.Level()) >= .90 && pat.Level() > 1 {
			// skip it
		} else if pat.Level() == 0 && negSupport > m.MaxNegSupport {
			// skip it
		} else if float64(size)/float64(pat.Level()) <= .001 {
			filtered = append(filtered, n)
			count++
		} else if negSupport <= m.MaxNegSupport {
			filtered = append(filtered, n)
			count++
		}
	}
	// errors.Logf("DEBUG", "fitered %v from %v : %v", len(filtered), len(slice), count)
	return filtered, nil
}

func uniform(slice []lattice.Node, err error) (lattice.Node, error) {
	// errors.Logf("DEBUG", "children %v", slice)
	if err != nil {
		return nil, err
	}
	if len(slice) > 0 {
		return slice[rand.Intn(len(slice))], nil
	}
	return nil, nil
}
