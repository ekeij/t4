package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/t4db/t4/pkg/object"
)

// The t4 binary on Google Cloud Storage. These tests run with
// T4_GCS_TEST_BUCKET (an existing bucket, with Application Default
// Credentials or T4_GCS_TEST_CREDENTIALS_FILE) or STORAGE_EMULATOR_HOST (an
// emulator such as fake-gcs-server; the test creates a bucket), and are
// skipped otherwise. The t4 processes inherit the environment.

type gcsTestConfig struct {
	bin    string
	bucket string
	prefix string
}

func (c gcsTestConfig) args() []string {
	args := []string{"--gcs-bucket", c.bucket, "--gcs-prefix", c.prefix}
	if f := os.Getenv("T4_GCS_TEST_CREDENTIALS_FILE"); f != "" {
		args = append(args, "--gcs-credentials-file", f)
	}
	return args
}

func newGCSTestConfig(t *testing.T) gcsTestConfig {
	t.Helper()
	bucket := os.Getenv("T4_GCS_TEST_BUCKET")
	if bucket == "" && os.Getenv("STORAGE_EMULATOR_HOST") == "" {
		t.Skip("set T4_GCS_TEST_BUCKET (real bucket, ADC) or STORAGE_EMULATOR_HOST (emulator) to run the GCS e2e tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if bucket == "" {
		client, err := storage.NewClient(ctx)
		if err != nil {
			t.Fatal(err)
		}
		bucket = fmt.Sprintf("t4-e2e-%d", time.Now().UnixNano())
		if err := client.Bucket(bucket).Create(ctx, "t4-test", nil); err != nil {
			t.Fatalf("create bucket: %v", err)
		}
		t.Cleanup(func() { _ = client.Close() })
	}
	cfg := gcsTestConfig{
		bin:    envOr("T4_SMOKE_BIN", ""),
		bucket: bucket,
		prefix: fmt.Sprintf("t4-e2e/%s-%d", t.Name(), time.Now().UnixNano()),
	}
	if cfg.bin == "" {
		cfg.bin = buildT4(t, ctx, t.TempDir())
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		s, err := object.NewGCSStoreFromConfig(ctx, object.GCSConfig{Bucket: bucket, CredentialsFile: os.Getenv("T4_GCS_TEST_CREDENTIALS_FILE")})
		if err != nil {
			return
		}
		defer func() { _ = s.Close() }()
		if keys, err := s.List(ctx, cfg.prefix+"/"); err == nil {
			_ = s.DeleteMany(ctx, keys)
		}
	})
	return cfg
}

type gcsNode struct {
	name    string
	listen  string
	metrics string
	cmd     *exec.Cmd
	log     *bytes.Buffer
}

func startGCSNode(t *testing.T, cfg gcsTestConfig, name, dataDir string, extra ...string) *gcsNode {
	t.Helper()
	n := &gcsNode{name: name, listen: freeAddr(t), metrics: freeAddr(t), log: &bytes.Buffer{}}
	args := append([]string{
		"run",
		"--data-dir", dataDir,
		"--listen", n.listen,
		"--metrics-addr", n.metrics,
		"--node-id", name,
		"--checkpoint-entries", "1",
		"--segment-max-age-sec", "1",
		"--log-level", "info",
	}, cfg.args()...)
	n.cmd = exec.Command(cfg.bin, append(args, extra...)...)
	n.cmd.Stdout = n.log
	n.cmd.Stderr = n.log
	if err := n.cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	t.Cleanup(func() {
		stopNode(n.cmd)
		if t.Failed() {
			t.Logf("%s log:\n%s", name, n.log.String())
		}
	})
	return n
}

func (n *gcsNode) kill() {
	_ = n.cmd.Process.Signal(syscall.SIGKILL)
	_ = n.cmd.Wait()
}

// metric returns the value of the sample whose name and labels are exactly
// series (e.g. `t4_role{role="leader"}`), or -1 if the node does not answer.
func (n *gcsNode) metric(series string) float64 {
	c := http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get("http://" + n.metrics + "/metrics")
	if err != nil {
		return -1
	}
	defer func() { _ = resp.Body.Close() }()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		if v, ok := strings.CutPrefix(sc.Text(), series+" "); ok {
			f, err := strconv.ParseFloat(v, 64)
			if err == nil {
				return f
			}
		}
	}
	return 0
}

func etcdGet(ctx context.Context, listen, key string) (string, error) {
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{listen}, DialTimeout: 2 * time.Second})
	if err != nil {
		return "", err
	}
	defer func() { _ = cli.Close() }()
	resp, err := cli.Get(ctx, key)
	if err != nil {
		return "", err
	}
	if len(resp.Kvs) == 0 {
		return "", fmt.Errorf("%s not found", key)
	}
	return string(resp.Kvs[0].Value), nil
}

// TestGCSSingleNodeAndCLI runs a single node on GCS, replaces its data
// directory, and checks that a new node restores every write from the
// bucket. Then it runs the maintenance commands against the same prefix.
func TestGCSSingleNodeAndCLI(t *testing.T) {
	cfg := newGCSTestConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	n := startGCSNode(t, cfg, "single", filepath.Join(t.TempDir(), "data"))
	if err := waitForEtcd(ctx, n.listen); err != nil {
		t.Fatalf("node did not start: %v\n%s", err, n.log.String())
	}
	if err := writeKeysWithPrefix(ctx, n.listen, "/gcs/", 5); err != nil {
		t.Fatal(err)
	}
	if got := n.metric(`t4_role{role="single"}`); got != 1 {
		t.Fatalf(`t4_role{role="single"} = %v, want 1`, got)
	}
	stopNode(n.cmd)

	// A fresh data directory: everything comes back from GCS.
	n2 := startGCSNode(t, cfg, "single", filepath.Join(t.TempDir(), "data"))
	if err := waitForEtcd(ctx, n2.listen); err != nil {
		t.Fatalf("node did not restart: %v\n%s", err, n2.log.String())
	}
	for i := range 5 {
		if v, err := etcdGet(ctx, n2.listen, fmt.Sprintf("/gcs/%d", i)); err != nil || v != fmt.Sprintf("value-%d", i) {
			t.Fatalf("/gcs/%d after restore from GCS: %q, %v", i, v, err)
		}
	}
	stopNode(n2.cmd)

	run := func(args ...string) string {
		t.Helper()
		out, err := exec.CommandContext(ctx, cfg.bin, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("t4 %s: %v\n%s", strings.Join(args, " "), err, out)
		}
		return string(out)
	}
	if out := run(append([]string{"status"}, cfg.args()...)...); !strings.Contains(out, "GCS status  gs://"+cfg.bucket+"/"+cfg.prefix) || !strings.Contains(out, "checkpoint/") {
		t.Fatalf("t4 status:\n%s", out)
	}
	if out := run(append([]string{"restore", "list"}, cfg.args()...)...); !strings.Contains(out, "(latest)") {
		t.Fatalf("t4 restore list:\n%s", out)
	}
	restoreDir := filepath.Join(t.TempDir(), "restored")
	run(append([]string{"restore", "checkpoint", "--data-dir", restoreDir}, cfg.args()...)...)
	if out := run("inspect", "count", "--data-dir", restoreDir, "--prefix", "/gcs/"); lastNonEmptyLine(out) != "/gcs/: 5" {
		t.Fatalf("t4 inspect count on the restored checkpoint:\n%s", out)
	}
	cpKey := lastNonEmptyLine(run(append([]string{"branch", "fork", "--branch-id", "b1"}, cfg.args()...)...))
	if !strings.HasPrefix(cpKey, "checkpoint/") {
		t.Fatalf("t4 branch fork printed %q", cpKey)
	}
	if out := run(append([]string{"status"}, cfg.args()...)...); !strings.Contains(out, "b1") {
		t.Fatalf("t4 status does not list the branch:\n%s", out)
	}

	// A branch node in its own prefix of the same bucket, from the fork.
	branch := gcsTestConfig{bin: cfg.bin, bucket: cfg.bucket, prefix: cfg.prefix + "-branch"}
	bn := startGCSNode(t, branch, "branch", filepath.Join(t.TempDir(), "branch"), "--branch-prefix", cfg.prefix, "--branch-checkpoint", cpKey)
	if err := waitForEtcd(ctx, bn.listen); err != nil {
		t.Fatalf("branch node did not start: %v\n%s", err, bn.log.String())
	}
	if v, err := etcdGet(ctx, bn.listen, "/gcs/4"); err != nil || v != "value-4" {
		t.Fatalf("/gcs/4 on the branch: %q, %v", v, err)
	}
	stopNode(bn.cmd)

	run(append([]string{"branch", "unfork", "--branch-id", "b1"}, cfg.args()...)...)
	if out := run(append([]string{"gc", "--keep", "1"}, cfg.args()...)...); !strings.Contains(out, "GC complete") {
		t.Fatalf("t4 gc:\n%s", out)
	}
}

// TestGCSClusterLeaderKilled runs three nodes on GCS, kills the leader with
// SIGKILL, and checks through each node's own metrics that exactly one
// survivor becomes leader.
func TestGCSClusterLeaderKilled(t *testing.T) {
	cfg := newGCSTestConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	nodes := make([]*gcsNode, 3)
	for i := range nodes {
		peer := freeAddr(t)
		nodes[i] = startGCSNode(t, cfg, fmt.Sprintf("gcs-node-%d", i), filepath.Join(t.TempDir(), "data"),
			"--peer-listen", peer, "--advertise-peer", peer, "--follower-max-retries", "2")
	}
	for _, n := range nodes {
		if err := waitForEtcd(ctx, n.listen); err != nil {
			t.Fatalf("%s did not start: %v\n%s", n.name, err, n.log.String())
		}
	}

	leaders := func(nodes []*gcsNode) []*gcsNode {
		var out []*gcsNode
		for _, n := range nodes {
			if n.metric(`t4_role{role="leader"}`) == 1 {
				out = append(out, n)
			}
		}
		return out
	}
	var leader *gcsNode
	for deadline := time.Now().Add(time.Minute); leader == nil && time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if l := leaders(nodes); len(l) == 1 {
			leader = l[0]
		}
	}
	if leader == nil {
		t.Fatal("no single leader in the metrics of three nodes")
	}
	if err := writeKeysWithPrefix(ctx, leader.listen, "/cluster/", 10); err != nil {
		t.Fatal(err)
	}
	var survivors []*gcsNode
	for _, n := range nodes {
		if n != leader {
			survivors = append(survivors, n)
			if won := n.metric(`t4_elections_total{outcome="won"}`); won != 0 {
				t.Fatalf("follower %s has won %v elections", n.name, won)
			}
		}
	}

	leader.kill()
	killed := time.Now()
	var newLeader *gcsNode
	for deadline := time.Now().Add(2 * time.Minute); newLeader == nil && time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		switch l := leaders(survivors); len(l) {
		case 0:
		case 1:
			newLeader = l[0]
		default:
			t.Fatalf("%s and %s both report t4_role{role=\"leader\"} 1", l[0].name, l[1].name)
		}
	}
	if newLeader == nil {
		t.Fatal("no survivor became leader")
	}
	t.Logf("%s leads %v after the leader was killed", newLeader.name, time.Since(killed))
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if l := leaders(survivors); len(l) != 1 || l[0] != newLeader {
			t.Fatalf("leaders among survivors changed to %d nodes", len(l))
		}
	}
	for _, n := range survivors {
		want := 0.0
		if n == newLeader {
			want = 1
		}
		if won := n.metric(`t4_elections_total{outcome="won"}`); won != want {
			t.Fatalf(`%s: t4_elections_total{outcome="won"} = %v, want %v`, n.name, won, want)
		}
	}
	for _, n := range survivors {
		if v, err := etcdGet(ctx, n.listen, "/cluster/9"); err != nil || v != "value-9" {
			t.Fatalf("/cluster/9 on %s after failover: %q, %v", n.name, v, err)
		}
	}
	if err := writeKeysWithPrefix(ctx, newLeader.listen, "/after/", 1); err != nil {
		t.Fatalf("write after failover: %v", err)
	}
}
