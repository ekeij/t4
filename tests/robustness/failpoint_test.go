package robustness

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

// failpoint is a failure injected into a running cluster while traffic runs.
// Inject returns once the cluster has recovered.
type failpoint interface {
	Name() string
	// Available reports whether the failpoint applies to a cluster this size.
	Available(size int) bool
	Inject(ctx context.Context, c *cluster, bin string) error
}

// killFailpoint SIGKILLs nodes and restarts them after a pause.
type killFailpoint struct {
	name string
	pick func(ctx context.Context, c *cluster) ([]*node, error)
	// multiNode marks failpoints that only make sense with followers.
	multiNode bool
}

func (f killFailpoint) Name() string { return f.name }

func (f killFailpoint) Available(size int) bool { return !f.multiNode || size > 1 }

func (f killFailpoint) Inject(ctx context.Context, c *cluster, bin string) error {
	victims, err := f.pick(ctx, c)
	if err != nil {
		return err
	}
	for _, n := range victims {
		n.kill()
	}
	time.Sleep(time.Duration(rand.Int63n(int64(time.Second))))
	for _, n := range victims {
		if err := n.start(bin); err != nil {
			return err
		}
	}
	return c.waitReady(ctx)
}

// stopFailpoint restarts the leader gracefully, so it hands leadership over
// rather than having it taken away.
type stopFailpoint struct{}

func (stopFailpoint) Name() string { return "RestartLeader" }

func (stopFailpoint) Available(size int) bool { return size > 1 }

func (stopFailpoint) Inject(ctx context.Context, c *cluster, bin string) error {
	l, err := c.leader(ctx)
	if err != nil {
		return err
	}
	l.stop()
	if err := l.start(bin); err != nil {
		return err
	}
	return c.waitReady(ctx)
}

var failpoints = []failpoint{
	killFailpoint{
		name: "KillRandomNode",
		pick: func(_ context.Context, c *cluster) ([]*node, error) {
			return []*node{c.nodes[rand.Intn(len(c.nodes))]}, nil
		},
	},
	killFailpoint{
		name:      "KillLeader",
		multiNode: true,
		pick: func(ctx context.Context, c *cluster) ([]*node, error) {
			l, err := c.leader(ctx)
			if err != nil {
				return nil, err
			}
			return []*node{l}, nil
		},
	},
	killFailpoint{
		name:      "KillFollower",
		multiNode: true,
		pick: func(ctx context.Context, c *cluster) ([]*node, error) {
			l, err := c.leader(ctx)
			if err != nil {
				return nil, err
			}
			var followers []*node
			for _, n := range c.nodes {
				if n != l {
					followers = append(followers, n)
				}
			}
			return []*node{followers[rand.Intn(len(followers))]}, nil
		},
	},
	killFailpoint{
		name:      "KillAllNodes",
		multiNode: true,
		pick: func(_ context.Context, c *cluster) ([]*node, error) {
			return c.nodes, nil
		},
	},
	stopFailpoint{},
}

func failpointByName(name string) (failpoint, error) {
	for _, f := range failpoints {
		if f.Name() == name {
			return f, nil
		}
	}
	return nil, fmt.Errorf("unknown failpoint %q", name)
}
