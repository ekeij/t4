package cli

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/t4db/t4"
	"github.com/t4db/t4/pkg/object"
)

// s3Flags holds the parsed values of a set of S3 configuration flags.
type s3Flags struct {
	Bucket          string
	Prefix          string
	Endpoint        string
	Region          string
	Profile         string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	CABundle        string
}

// config converts the flag values into an object.S3Config ready for
// object.NewS3StoreFromConfig.
func (f *s3Flags) config() object.S3Config {
	return object.S3Config{
		Bucket:          f.Bucket,
		Prefix:          f.Prefix,
		Endpoint:        f.Endpoint,
		Region:          f.Region,
		Profile:         f.Profile,
		AccessKeyID:     f.AccessKeyID,
		SecretAccessKey: f.SecretAccessKey,
		SessionToken:    f.SessionToken,
		CABundle:        f.CABundle,
	}
}

// addS3Flags registers --s3-bucket/prefix/endpoint/region/profile on cmd,
// wires their T4_S3_BUCKET/PREFIX/ENDPOINT/REGION/PROFILE env vars via
// PreRunE, and returns a pointer to the struct that holds the values.
// Commands register it through addObjectStoreFlags, which also offers GCS.
func addS3Flags(cmd *cobra.Command, bucketRequired bool) *s3Flags {
	f := &s3Flags{}
	bucketHelp := "S3 bucket (env: T4_S3_BUCKET)"
	if bucketRequired {
		bucketHelp = "S3 bucket; this or --gcs-bucket is required (env: T4_S3_BUCKET)"
	}
	cmd.Flags().StringVar(&f.Bucket, "s3-bucket", "", bucketHelp)
	cmd.Flags().StringVar(&f.Prefix, "s3-prefix", "", "key prefix inside the S3 bucket (env: T4_S3_PREFIX)")
	cmd.Flags().StringVar(&f.Endpoint, "s3-endpoint", "", "custom S3 endpoint URL, e.g. for S3-compatible stores (env: T4_S3_ENDPOINT)")
	cmd.Flags().StringVar(&f.Region, "s3-region", "", "AWS region (env: T4_S3_REGION)")
	cmd.Flags().StringVar(&f.Profile, "s3-profile", "", "enable the ambient AWS credentials chain (env vars → ~/.aws/credentials[profile] → EC2/EKS IMDS); SSO and AssumeRole profiles are not supported; use 'default' to opt in to the default profile (env: T4_S3_PROFILE)")
	cmd.Flags().StringVar(&f.AccessKeyID, "s3-access-key-id", "", "t4 S3 access key ID; when set with --s3-secret-access-key, uses static credentials (env: T4_S3_ACCESS_KEY_ID)")
	cmd.Flags().StringVar(&f.SecretAccessKey, "s3-secret-access-key", "", "AWS secret access key (env: T4_S3_SECRET_ACCESS_KEY)")
	cmd.Flags().StringVar(&f.SessionToken, "s3-session-token", "", "session token for temporary STS credentials; honored with --s3-access-key-id/--s3-secret-access-key (env: T4_S3_SESSION_TOKEN)")
	cmd.Flags().StringVar(&f.CABundle, "s3-ca-bundle", "", "PEM CA bundle file to trust for HTTPS to the S3 endpoint; use this for S3-compatible stores running behind a self-signed CA (env: T4_S3_CA_BUNDLE)")
	prependPreRunE(cmd, func(cmd *cobra.Command, _ []string) error {
		return applyEnvVars(cmd, map[string]string{
			"s3-bucket":            "T4_S3_BUCKET",
			"s3-prefix":            "T4_S3_PREFIX",
			"s3-endpoint":          "T4_S3_ENDPOINT",
			"s3-region":            "T4_S3_REGION",
			"s3-profile":           "T4_S3_PROFILE",
			"s3-access-key-id":     "T4_S3_ACCESS_KEY_ID",
			"s3-secret-access-key": "T4_S3_SECRET_ACCESS_KEY",
			"s3-session-token":     "T4_S3_SESSION_TOKEN",
			"s3-ca-bundle":         "T4_S3_CA_BUNDLE",
		})
	})
	return f
}

// gcsFlags holds the parsed values of a set of GCS configuration flags.
type gcsFlags struct {
	Bucket          string
	Prefix          string
	CredentialsFile string
}

// config converts the flag values into an object.GCSConfig ready for
// object.NewGCSStoreFromConfig.
func (f *gcsFlags) config() object.GCSConfig {
	return object.GCSConfig{
		Bucket:          f.Bucket,
		Prefix:          f.Prefix,
		CredentialsFile: f.CredentialsFile,
	}
}

// addGCSFlags registers --gcs-bucket/prefix/credentials-file on cmd, wires
// their T4_GCS_BUCKET/PREFIX/CREDENTIALS_FILE env vars via PreRunE, and
// returns a pointer to the struct that holds the values. Commands register
// it through addObjectStoreFlags.
func addGCSFlags(cmd *cobra.Command, bucketRequired bool) *gcsFlags {
	f := &gcsFlags{}
	bucketHelp := "Google Cloud Storage bucket, used through the native GCS API; mutually exclusive with --s3-bucket (env: T4_GCS_BUCKET)"
	if bucketRequired {
		bucketHelp = "Google Cloud Storage bucket, used through the native GCS API; this or --s3-bucket is required (env: T4_GCS_BUCKET)"
	}
	cmd.Flags().StringVar(&f.Bucket, "gcs-bucket", "", bucketHelp)
	cmd.Flags().StringVar(&f.Prefix, "gcs-prefix", "", "key prefix inside the GCS bucket (env: T4_GCS_PREFIX)")
	cmd.Flags().StringVar(&f.CredentialsFile, "gcs-credentials-file", "", "Google credentials JSON file (service_account, authorized_user, external_account or impersonated_service_account) to use instead of Application Default Credentials, which cover GKE Workload Identity (env: T4_GCS_CREDENTIALS_FILE)")
	prependPreRunE(cmd, func(cmd *cobra.Command, _ []string) error {
		return applyEnvVars(cmd, map[string]string{
			"gcs-bucket":           "T4_GCS_BUCKET",
			"gcs-prefix":           "T4_GCS_PREFIX",
			"gcs-credentials-file": "T4_GCS_CREDENTIALS_FILE",
		})
	})
	return f
}

// objectStoreFlags is the object store a command is configured with: an S3
// bucket or a GCS bucket, never both.
type objectStoreFlags struct {
	s3  *s3Flags
	gcs *gcsFlags
}

// addObjectStoreFlags registers the S3 and GCS flags on cmd. Its PreRunE,
// which runs after their environment variables are applied, rejects a
// configuration that sets both an S3 and a GCS flag, and, if bucketRequired,
// one that sets neither bucket.
//
// Typical usage:
//
//	store := addObjectStoreFlags(cmd, true)
//	...
//	obj, err := store.open(ctx, store.prefix())
func addObjectStoreFlags(cmd *cobra.Command, bucketRequired bool) *objectStoreFlags {
	// Prepended before the flags' env var hooks, so it runs after them.
	prependPreRunE(cmd, func(cmd *cobra.Command, _ []string) error {
		return validateObjectStoreFlags(cmd, bucketRequired)
	})
	return &objectStoreFlags{
		s3:  addS3Flags(cmd, bucketRequired),
		gcs: addGCSFlags(cmd, bucketRequired),
	}
}

func validateObjectStoreFlags(cmd *cobra.Command, bucketRequired bool) error {
	changed := func(prefix string) []string {
		var names []string
		cmd.Flags().Visit(func(f *pflag.Flag) {
			if strings.HasPrefix(f.Name, prefix) {
				names = append(names, "--"+f.Name)
			}
		})
		return names
	}
	s3Set, gcsSet := changed("s3-"), changed("gcs-")
	s3Bucket, gcsBucket := cmd.Flags().Changed("s3-bucket"), cmd.Flags().Changed("gcs-bucket")
	switch {
	case s3Bucket && gcsBucket:
		return fmt.Errorf("--s3-bucket and --gcs-bucket are both set (by flag or T4_S3_BUCKET/T4_GCS_BUCKET); configure one object store")
	case gcsBucket && len(s3Set) > 0:
		return fmt.Errorf("--gcs-bucket is set, so S3 flags do not apply; unset %s", strings.Join(s3Set, ", "))
	case s3Bucket && len(gcsSet) > 0:
		return fmt.Errorf("--s3-bucket is set, so GCS flags do not apply; unset %s", strings.Join(gcsSet, ", "))
	case bucketRequired && !s3Bucket && !gcsBucket:
		return fmt.Errorf("an object store is required: set --s3-bucket (T4_S3_BUCKET) or --gcs-bucket (T4_GCS_BUCKET)")
	}
	return nil
}

// configured reports whether a bucket is set.
func (f *objectStoreFlags) configured() bool {
	return f.s3.Bucket != "" || f.gcs.Bucket != ""
}

// kind is "gcs", "s3", or "" when no bucket is set.
func (f *objectStoreFlags) kind() string {
	switch {
	case f.gcs.Bucket != "":
		return "gcs"
	case f.s3.Bucket != "":
		return "s3"
	default:
		return ""
	}
}

// prefix is the configured key prefix.
func (f *objectStoreFlags) prefix() string {
	if f.gcs.Bucket != "" {
		return f.gcs.Prefix
	}
	return f.s3.Prefix
}

// url names the configured bucket and prefix, e.g. "gs://bucket/prefix".
func (f *objectStoreFlags) url() string {
	if f.gcs.Bucket != "" {
		return fmt.Sprintf("gs://%s/%s", f.gcs.Bucket, f.gcs.Prefix)
	}
	return fmt.Sprintf("s3://%s/%s", f.s3.Bucket, f.s3.Prefix)
}

// open returns a store for the configured bucket under prefix, which may
// differ from the configured one (a branch node's source).
func (f *objectStoreFlags) open(ctx context.Context, prefix string) (object.Store, error) {
	if f.gcs.Bucket != "" {
		cfg := f.gcs.config()
		cfg.Prefix = prefix
		s, err := object.NewGCSStoreFromConfig(ctx, cfg)
		if err != nil {
			return nil, fmt.Errorf("init GCS: %w", err)
		}
		return s, nil
	}
	cfg := f.s3.config()
	cfg.Prefix = prefix
	s, err := object.NewS3StoreFromConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("init S3: %w", err)
	}
	return s, nil
}

type objectStoreEncryptionFlags struct {
	KeyFile string
	KeyEnv  string
}

func addObjectStoreEncryptionFlags(cmd *cobra.Command) *objectStoreEncryptionFlags {
	f := &objectStoreEncryptionFlags{}
	cmd.Flags().StringVar(&f.KeyFile, "object-store-encryption-key-file", "", "file containing a 32-byte AES-256 object-store encryption key as raw bytes, hex, or base64 (env: T4_OBJECT_STORE_ENCRYPTION_KEY_FILE)")
	cmd.Flags().StringVar(&f.KeyEnv, "object-store-encryption-key-env", "", "environment variable holding a 32-byte AES-256 object-store encryption key as raw bytes, hex, or base64 (env: T4_OBJECT_STORE_ENCRYPTION_KEY_ENV)")
	prependPreRunE(cmd, func(cmd *cobra.Command, _ []string) error {
		return applyEnvVars(cmd, map[string]string{
			"object-store-encryption-key-file": "T4_OBJECT_STORE_ENCRYPTION_KEY_FILE",
			"object-store-encryption-key-env":  "T4_OBJECT_STORE_ENCRYPTION_KEY_ENV",
		})
	})
	return f
}

func (f *objectStoreEncryptionFlags) enabled() bool {
	return f != nil && (f.KeyFile != "" || f.KeyEnv != "")
}

func (f *objectStoreEncryptionFlags) config() (*t4.ObjectStoreEncryptionConfig, error) {
	if !f.enabled() {
		return nil, nil
	}
	kp, err := f.keyProvider()
	if err != nil {
		return nil, err
	}
	return &t4.ObjectStoreEncryptionConfig{KeyProvider: kp}, nil
}

func (f *objectStoreEncryptionFlags) wrapStore(store object.Store) (object.Store, error) {
	if !f.enabled() || store == nil {
		return store, nil
	}
	kp, err := f.keyProvider()
	if err != nil {
		return nil, err
	}
	return object.NewEncryptedStore(store, kp), nil
}

func newObjectStoreForCommand(ctx context.Context, f *objectStoreFlags, enc *objectStoreEncryptionFlags) (object.Store, error) {
	rawStore, err := f.open(ctx, f.prefix())
	if err != nil {
		return nil, err
	}
	store, err := enc.wrapStore(rawStore)
	if err != nil {
		return nil, fmt.Errorf("encryption key: %w", err)
	}
	return store, nil
}

func (f *objectStoreEncryptionFlags) keyProvider() (*object.StaticKeyProvider, error) {
	raw, err := f.keyMaterial()
	if err != nil {
		return nil, err
	}
	decoded, err := decodeKeyMaterial(raw)
	if err != nil {
		return nil, err
	}
	return object.NewStaticKeyProvider(decoded)
}

func (f *objectStoreEncryptionFlags) keyMaterial() ([]byte, error) {
	if f.KeyFile != "" {
		data, err := os.ReadFile(f.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("read object-store encryption key file: %w", err)
		}
		return data, nil
	}
	val := os.Getenv(f.KeyEnv)
	if val == "" {
		return nil, fmt.Errorf("environment variable %q is empty or not set", f.KeyEnv)
	}
	return []byte(val), nil
}

func decodeKeyMaterial(raw []byte) ([]byte, error) {
	if len(raw) == 32 {
		return append([]byte(nil), raw...), nil
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 32 {
		return append([]byte(nil), raw...), nil
	}
	if len(raw) == 64 {
		dst := make([]byte, 32)
		if _, err := hex.Decode(dst, raw); err == nil {
			return dst, nil
		}
	}
	if dst, err := base64.StdEncoding.DecodeString(string(raw)); err == nil && len(dst) == 32 {
		return dst, nil
	}
	if dst, err := base64.RawStdEncoding.DecodeString(string(raw)); err == nil && len(dst) == 32 {
		return dst, nil
	}
	return nil, fmt.Errorf("object-store encryption key must be 32 raw bytes, 64 hex chars, or base64 for 32 bytes; got %d bytes", len(raw))
}

// prependPreRunE prepends fn to cmd's existing PreRunE hook, if any.
// This allows multiple callers to register pre-run logic without overwriting
// each other.
func prependPreRunE(cmd *cobra.Command, fn func(*cobra.Command, []string) error) {
	prev := cmd.PreRunE
	cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		if err := fn(cmd, args); err != nil {
			return err
		}
		if prev != nil {
			return prev(cmd, args)
		}
		return nil
	}
}

// applyEnvVars sets flag values from environment variables for flags that have
// not been explicitly provided on the command line. mapping is a map of flag
// name → environment variable name (e.g. "s3-bucket" → "T4_S3_BUCKET").
// Flags marked as required pass cobra's validation when the env var is set,
// because pflag.Set marks the flag as changed.
func applyEnvVars(cmd *cobra.Command, mapping map[string]string) error {
	for flag, env := range mapping {
		if !cmd.Flags().Changed(flag) {
			if v := os.Getenv(env); v != "" {
				if err := cmd.Flags().Set(flag, v); err != nil {
					return fmt.Errorf("env %s: %w", env, err)
				}
			}
		}
	}
	return nil
}

func valueOrDisabled(v string) string {
	if v == "" {
		return "disabled"
	}
	return v
}

func valueOrNone(v string) string {
	if v == "" {
		return "(none)"
	}
	return v
}

func valueOrDefault(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}
