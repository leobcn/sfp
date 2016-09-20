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
		cur := m.Pos.Root()
		prev := cur
		for cur != nil {
			prev = cur
			cur, err = uniform(m.filterNegs(cur.Children()))
			if err != nil {
				return err
			}
		}
		err = m.Rptr.Report(prev)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Miner) filterNegs(slice []lattice.Node, err error) ([]lattice.Node, error) {
	if err != nil {
		return nil, err
	}
	filtered := make([]lattice.Node, 0, len(slice))
	count := 0
	for _, n := range slice {
		negSupported, err := m.Neg.SupportedAt(n.Pattern(), m.MaxNegSupport)
		if err != nil {
			return nil, err
		}
		if negSupported {
			continue
		}
		filtered = append(filtered, n)
		count++
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
