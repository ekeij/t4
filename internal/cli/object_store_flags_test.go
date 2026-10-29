package cli

import (
	"io"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/t4db/t4/pkg/object"
)

// objectStoreTestCmd returns a command with the object store flags whose RunE
// records that it ran.
func objectStoreTestCmd(required bool) (*cobra.Command, *objectStoreFlags, *bool) {
	ran := false
	cmd := &cobra.Command{
		Use:  "test",
		RunE: func(*cobra.Command, []string) error { ran = true; return nil },
	}
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	f := addObjectStoreFlags(cmd, required)
	return cmd, f, &ran
}

func TestObjectStoreFlagsValidation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		required bool
		args     []string
		env      map[string]string
		wantErr  string
		wantKind string
	}{
		{name: "S3", required: true, args: []string{"--s3-bucket", "b", "--s3-prefix", "p"}, wantKind: "s3"},
		{name: "GCS", required: true, args: []string{"--gcs-bucket", "b", "--gcs-prefix", "p"}, wantKind: "gcs"},
		{name: "GCSFromEnv", required: true, env: map[string]string{"T4_GCS_BUCKET": "b", "T4_GCS_PREFIX": "p"}, wantKind: "gcs"},
		{name: "NoneOptional", wantKind: ""},
		{name: "NoneRequired", required: true, wantErr: "set --s3-bucket (T4_S3_BUCKET) or --gcs-bucket (T4_GCS_BUCKET)"},
		{name: "BothBuckets", args: []string{"--s3-bucket", "a", "--gcs-bucket", "b"}, wantErr: "--s3-bucket and --gcs-bucket are both set"},
		{name: "BothBucketsFromEnv", args: []string{"--gcs-bucket", "b"}, env: map[string]string{"T4_S3_BUCKET": "a"}, wantErr: "--s3-bucket and --gcs-bucket are both set"},
		{name: "GCSWithS3Flags", args: []string{"--gcs-bucket", "b", "--s3-prefix", "p", "--s3-region", "r"}, wantErr: "unset --s3-prefix, --s3-region"},
		{name: "GCSWithS3Env", args: []string{"--gcs-bucket", "b"}, env: map[string]string{"T4_S3_ENDPOINT": "https://storage.googleapis.com"}, wantErr: "unset --s3-endpoint"},
		{name: "S3WithGCSFlags", args: []string{"--s3-bucket", "b", "--gcs-credentials-file", "f"}, wantErr: "unset --gcs-credentials-file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, env := range []string{"T4_S3_BUCKET", "T4_S3_PREFIX", "T4_S3_ENDPOINT", "T4_S3_REGION", "T4_GCS_BUCKET", "T4_GCS_PREFIX", "T4_GCS_CREDENTIALS_FILE"} {
				t.Setenv(env, tc.env[env])
			}
			cmd, f, ran := objectStoreTestCmd(tc.required)
			cmd.SetArgs(tc.args)
			err := cmd.Execute()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("got %v, want an error containing %q", err, tc.wantErr)
				}
				if *ran {
					t.Fatal("command ran despite the invalid configuration")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := f.kind(); got != tc.wantKind {
				t.Fatalf("kind = %q, want %q", got, tc.wantKind)
			}
			if tc.wantKind != "" && f.prefix() != "p" {
				t.Fatalf("prefix = %q, want %q", f.prefix(), "p")
			}
		})
	}
}

func TestObjectStoreFlagsOpenGCS(t *testing.T) {
	// Talk to an emulator address so that no credentials are needed; the
	// constructor makes no requests.
	t.Setenv("STORAGE_EMULATOR_HOST", "127.0.0.1:1")
	f := &objectStoreFlags{s3: &s3Flags{}, gcs: &gcsFlags{Bucket: "b", Prefix: "cluster"}}
	if got := f.url(); got != "gs://b/cluster" {
		t.Fatalf("url = %q", got)
	}
	// A branch node opens the same bucket under the source prefix.
	s, err := f.open(t.Context(), "source")
	if err != nil {
		t.Fatal(err)
	}
	gs, ok := s.(*object.GCSStore)
	if !ok {
		t.Fatalf("open returned %T, want *object.GCSStore", s)
	}
	_ = gs.Close()
	if _, ok := s.(object.ConditionalStore); !ok {
		t.Fatal("GCS store is not a ConditionalStore")
	}
}

// TestObjectStoreCommandsAcceptGCS checks that every command with --s3-bucket
// also has the GCS flags and accepts --gcs-bucket in its place.
func TestObjectStoreCommandsAcceptGCS(t *testing.T) {
	var withS3 []string
	var walk func(cmd *cobra.Command)
	walk = func(cmd *cobra.Command) {
		if cmd.Flags().Lookup("s3-bucket") != nil {
			withS3 = append(withS3, cmd.CommandPath())
			for _, name := range []string{"gcs-bucket", "gcs-prefix", "gcs-credentials-file"} {
				if cmd.Flags().Lookup(name) == nil {
					t.Errorf("%s has --s3-bucket but not --%s", cmd.CommandPath(), name)
				}
			}
		}
		for _, c := range cmd.Commands() {
			walk(c)
		}
	}
	walk(NewRootCmd())
	want := []string{"t4 branch fork", "t4 branch unfork", "t4 gc", "t4 restore checkpoint", "t4 restore list", "t4 run", "t4 status"}
	if strings.Join(withS3, ",") != strings.Join(want, ",") {
		t.Fatalf("commands with --s3-bucket: %v, want %v", withS3, want)
	}
}

func TestRunRejectsS3AndGCS(t *testing.T) {
	root := NewRootCmd()
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	root.SetArgs([]string{"run", "--data-dir", t.TempDir(), "--s3-bucket", "a", "--gcs-bucket", "b"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "--s3-bucket and --gcs-bucket are both set") {
		t.Fatalf("got %v, want a startup error naming both buckets", err)
	}
}

func TestIsGCSS3Endpoint(t *testing.T) {
	for endpoint, want := range map[string]bool{
		"https://storage.googleapis.com":      true,
		"https://STORAGE.googleapis.com:443/": true,
		"https://s3.amazonaws.com":            false,
		"http://minio:9000":                   false,
		"":                                    false,
	} {
		if got := isGCSS3Endpoint(endpoint); got != want {
			t.Errorf("isGCSS3Endpoint(%q) = %v, want %v", endpoint, got, want)
		}
	}
}

func TestRunMode(t *testing.T) {
	for _, tc := range []struct {
		store, peer, branch, want string
	}{
		{"", "", "", "single-node"},
		{"s3", "", "", "single-node+s3"},
		{"gcs", "", "", "single-node+gcs"},
		{"gcs", "0.0.0.0:3380", "", "multi-node"},
		{"gcs", "", "checkpoint/x", "branch"},
	} {
		if got := runMode(tc.store, tc.peer, tc.branch); got != tc.want {
			t.Errorf("runMode(%q, %q, %q) = %q, want %q", tc.store, tc.peer, tc.branch, got, tc.want)
		}
	}
}
