package config

import (
	"path/filepath"
	"sync"
)

import (
	"github.com/timtadh/goiso"
)

import (
	"github.com/timtadh/sfp/stacks/subgraph"
	"github.com/timtadh/sfp/stores/bytes_bytes"
	"github.com/timtadh/sfp/stores/bytes_float"
	"github.com/timtadh/sfp/stores/bytes_int"
	"github.com/timtadh/sfp/stores/bytes_subgraph"
	"github.com/timtadh/sfp/stores/int_edge"
	"github.com/timtadh/sfp/stores/int_int"
	"github.com/timtadh/sfp/stores/int_json"
	"github.com/timtadh/sfp/stores/ints_int"
	"github.com/timtadh/sfp/stores/ints_ints"
)

type Config struct {
	Cache            string
	Output           string
	Support, Samples int
	Unique           bool
	AsyncTasks       sync.WaitGroup
}

func (c *Config) Copy() *Config {
	return &Config{
		Cache:   c.Cache,
		Output:  c.Output,
		Support: c.Support,
		Samples: c.Samples,
		Unique:  c.Unique,
	}
}

func (c *Config) CacheFile(name string) string {
	return filepath.Join(c.Cache, name)
}

func (c *Config) OutputFile(name string) string {
	return filepath.Join(c.Output, name)
}

func (c *Config) MultiMap(name string) (bytes_bytes.MultiMap, error) {
	if c.Cache == "" {
		return bytes_bytes.AnonBpTree()
	} else {
		return bytes_bytes.NewBpTree(c.CacheFile(name + ".bptree"))
	}
}

func (c *Config) SubgraphList(
	name string,
	deserializeValue func([]byte) *goiso.SubGraph,
) (subgraph.List, error) {
	if c.Cache == "" {
		return subgraph.AnonList(bytes_subgraph.SerializeSubGraph, deserializeValue)
	} else {
		return subgraph.NewList(c.CacheFile(name+".mmlist"), bytes_subgraph.SerializeSubGraph, deserializeValue)
	}
}

func (c *Config) BytesSubgraphMultiMap(
	name string,
	deserializeValue func([]byte) *goiso.SubGraph,
) (bytes_subgraph.MultiMap, error) {
	if c.Cache == "" {
		return bytes_subgraph.AnonBpTree(bytes_subgraph.Identity, bytes_subgraph.SerializeSubGraph, bytes_subgraph.Identity, deserializeValue)
	} else {
		return bytes_subgraph.NewBpTree(c.CacheFile(name+".bptree"), bytes_subgraph.Identity, bytes_subgraph.SerializeSubGraph, bytes_subgraph.Identity, deserializeValue)
	}
}

func (c *Config) BytesFloatMultiMap(name string) (bytes_float.MultiMap, error) {
	if c.Cache == "" {
		return bytes_float.AnonBpTree()
	} else {
		return bytes_float.NewBpTree(c.CacheFile(name + ".bptree"))
	}
}

func (c *Config) BytesIntMultiMap(name string) (bytes_int.MultiMap, error) {
	if c.Cache == "" {
		return bytes_int.AnonBpTree()
	} else {
		return bytes_int.NewBpTree(c.CacheFile(name + ".bptree"))
	}
}

func (c *Config) IntEdgeMultiMap(name string) (int_edge.MultiMap, error) {
	if c.Cache == "" {
		return int_edge.AnonBpTree()
	} else {
		return int_edge.NewBpTree(c.CacheFile(name + ".bptree"))
	}
}

func (c *Config) IntIntMultiMap(name string) (int_int.MultiMap, error) {
	if c.Cache == "" {
		return int_int.AnonBpTree()
	} else {
		return int_int.NewBpTree(c.CacheFile(name + ".bptree"))
	}
}

func (c *Config) IntJsonMultiMap(name string) (int_json.MultiMap, error) {
	if c.Cache == "" {
		return int_json.AnonBpTree()
	} else {
		return int_json.NewBpTree(c.CacheFile(name + ".bptree"))
	}
}

func (c *Config) IntsIntMultiMap(name string) (ints_int.MultiMap, error) {
	if c.Cache == "" {
		return ints_int.AnonBpTree()
	} else {
		return ints_int.NewBpTree(c.CacheFile(name + ".bptree"))
	}
}

func (c *Config) IntsIntsMultiMap(name string) (ints_ints.MultiMap, error) {
	if c.Cache == "" {
		return ints_ints.AnonBpTree()
	} else {
		return ints_ints.NewBpTree(c.CacheFile(name + ".bptree"))
	}
}
