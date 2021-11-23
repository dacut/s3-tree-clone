package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/user"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/gabriel-vasile/mimetype"
	"golang.org/x/sync/semaphore"
)

type S3TreeClone struct {
	ctx              context.Context
	sem              *semaphore.Weighted
	waitGroup        *sync.WaitGroup
	s3Client         S3Interface
	storageClass     s3Types.StorageClass
	encAlg           s3Types.ServerSideEncryption
	ignoreTimestamps bool
	kmsKey           string
	bucket           string
	prefix           string
	rootUID          uint32
	rootGID          uint32
	baseDir          string
	verbose          bool
}

type Hashes struct {
	MD5    []byte
	SHA1   []byte
	SHA256 []byte
	SHA512 []byte
}

// S3Interface encapsulates the required APIs for our functionality. We use this for unit testing.
type S3Interface interface {
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	GetBucketLocation(context.Context, *s3.GetBucketLocationInput, ...func(*s3.Options)) (*s3.GetBucketLocationOutput, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
}

// main is the entrypoint for s3-tree-clone.
func main() {
	ctx := context.Background()
	os.Exit(run(ctx, os.Args[1:], nil))
}

// run executes s3-tree-clone, but allows for test injection.
func run(ctx context.Context, arguments []string, s3Client S3Interface) int {
	flagSet := flag.NewFlagSet("s3-tree-clone", flag.ContinueOnError)

	checkBucket := flagSet.Bool("check-bucket", true, "Call GetBucketLocation to verify the bucket location.")
	region := flagSet.String("region", "", "The AWS region to use. Defaults to $AWS_REGION, $AWS_DEFAULT_REGION, the configured region for the profile, or the instance region, whichever is appropriate.")
	profile := flagSet.String("profile", "", "The credentials profile to use.")
	storageClass := flagSet.String("storage-class", "STANDARD", "The S3 storage class to use. One of 'STANDARD', 'STANDARD_IA', 'ONEZONE_IA', 'INTELLIGENT_TIERING', 'GLACIER', 'DEEP_ARCHIVE', or 'OUTPOSTS'.")
	encAlg := flagSet.String("encryption-algorithm", "AES256", "The S3 server-side encryption algorithm to use. This must be either 'AES256' or 'aws:kms'.")
	kmsKey := flagSet.String("kms-key", "aws/s3", "If -encryption-algorithm is 'aws:kms', the KMS key ID to use. Defaults to aws/s3.")
	ignoreTimestamps := flagSet.Bool("ignore-timestamps", false, "Ignore file timestamps when comparing files.")
	maxConcurrent := flagSet.Int("max-concurrent", 30, "The maximum number of concurrent S3 requests to make.")
	maxRetries := flagSet.Int("max-retries", 10, "The maximum number of retries.")
	maxBackoffDelayString := flagSet.String("max-backoff-delay", "60s", "The maximum retry backoff delay. Specify a duration such as '1.5m', '1m30s', etc.")
	rootSquash := flagSet.Bool("root-squash", false, "Change files owned by root to nfsnobody.")
	help := flagSet.Bool("help", false, "Show this usage information.")
	verbose := flagSet.Bool("verbose", false, "Show verbose details.")
	stc := S3TreeClone{ctx: ctx}

	if err := flagSet.Parse(arguments); err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing arguments: %s\n", err)
		printUsage(flagSet)
		return 1
	}

	if *help {
		flagSet.SetOutput(os.Stdout)
		printUsage(flagSet)
		return 0
	}

	args := flagSet.Args()
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "Missing source and destination\n")
		printUsage(flagSet)
		return 2
	}

	if len(args) == 1 {
		fmt.Fprint(os.Stderr, "Missing destination\n")
		printUsage(flagSet)
		return 2
	}

	if len(args) > 2 {
		fmt.Fprintf(os.Stderr, "Unexpected argument: %s\n", args[2])
		printUsage(flagSet)
		return 2
	}

	var firstFilter string
	stc.baseDir, firstFilter = path.Split(args[0])
	dest := args[1]

	if firstFilter == "." {
		firstFilter = ""
	}

	if stc.baseDir == "" {
		stc.baseDir = "."
	}

	err := stc.SetBucketAndPrefix(dest)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Destination is not a valid S3 URL: %s\n", dest)
		return 2
	}

	if *storageClass != string(s3Types.StorageClassStandard) && *storageClass != string(s3Types.StorageClassStandardIa) && *storageClass != string(s3Types.StorageClassOnezoneIa) && *storageClass != string(s3Types.StorageClassIntelligentTiering) && *storageClass != string(s3Types.StorageClassGlacier) && *storageClass != string(s3Types.StorageClassDeepArchive) && *storageClass != string(s3Types.StorageClassOutposts) {
		fmt.Fprintf(os.Stderr, "Invalid -storage-class value: %s\n", *storageClass)
		printUsage(flagSet)
		return 1
	}

	stc.storageClass = s3Types.StorageClass(*storageClass)

	if *encAlg != string(s3Types.ServerSideEncryptionAes256) && *encAlg != string(s3Types.ServerSideEncryptionAwsKms) {
		fmt.Fprintf(os.Stderr, "Invalid -encryption-algorithm value: %s\n", *encAlg)
		printUsage(flagSet)
		return 1
	}

	stc.encAlg = s3Types.ServerSideEncryption(*encAlg)
	stc.kmsKey = *kmsKey

	stc.ignoreTimestamps = *ignoreTimestamps
	stc.verbose = *verbose

	// Check the -max-retries flag
	if *maxRetries < 0 {
		fmt.Fprintf(os.Stderr, "Invalid -max-retries value: %d\n", *maxRetries)
		printUsage(flagSet)
		return 1
	}

	// Check the -max-backoff-delay flag
	var maxBackoffDelay time.Duration
	if *maxRetries > 0 {
		maxBackoffDelay, err = time.ParseDuration(*maxBackoffDelayString)
		if err != nil || maxBackoffDelay <= time.Duration(0) {
			fmt.Fprintf(os.Stderr, "Invalid -max-backoff-delay value: %s\n", *maxBackoffDelayString)
			printUsage(flagSet)
			return 1
		}
	}

	// If AWS_DEFAULT_REGION is set but AWS_REGION is not, set AWS_REGION to AWS_DEFAULT_REGION to be compatible with other SDKs.
	if _, found := os.LookupEnv("AWS_REGION"); !found {
		if aws_default_region, found := os.LookupEnv("AWS_DEFAULT_REGION"); found {
			os.Setenv("AWS_REGION", aws_default_region)
		}
	}

	var configOptions []func(*config.LoadOptions) error
	if *region != "" {
		configOptions = append(configOptions, config.WithRegion(*region))
	}

	if *profile != "" {
		configOptions = append(configOptions, config.WithSharedConfigProfile(*profile))
	}

	if *rootSquash {
		err = stc.SetRootFromNFSNobody()
		if err != nil {
			return 1
		}
	}

	var retrierFunc func() aws.Retryer
	if *maxRetries == 0 {
		retrierFunc = func() aws.Retryer { return aws.NopRetryer{} }
	} else {
		retrierFunc = func() aws.Retryer {
			return retry.NewStandard(func(opts *retry.StandardOptions) {
				opts.MaxAttempts = *maxRetries
				opts.MaxBackoff = maxBackoffDelay
				opts.RateLimiter = ratelimit.NewTokenRateLimit(uint(*maxConcurrent))
			})
		}
	}
	configOptions = append(configOptions, config.WithRetryer(retrierFunc))

	if s3Client != nil {
		stc.s3Client = s3Client
	} else {
		awsConfig, err := config.LoadDefaultConfig(ctx, configOptions...)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to load AWS config: %v\n", err)
			return 1
		}

		stc.s3Client = s3.NewFromConfig(awsConfig)

		if *checkBucket {
			err = stc.ReconfigureS3ClientFromBucketLocation(configOptions)
			if err != nil {
				return 1
			}
		}
	}

	sourceDir, err := os.OpenFile(stc.baseDir, os.O_RDONLY, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to open source directory %s: %v\n", stc.baseDir, err)
		return 1
	}
	sourceDir.Close()

	stc.sem = semaphore.NewWeighted(int64(*maxConcurrent))
	stc.waitGroup = &sync.WaitGroup{}

	err = stc.WalkDirectory("", stc.baseDir, firstFilter)
	if err != nil {
		fmt.Fprintf(os.Stderr, "walkDirectory failed: %v\n", err)
		return 1
	}

	stc.waitGroup.Wait()
	return 0
}

func printUsage(flagSet *flag.FlagSet) {
	var out = flagSet.Output()
	fmt.Fprintf(out,
		`s3-tree-clone [options] <src-dir> s3://<bucket>/<prefix>
Copy the filesystem tree rooted at <src-dir> to the given S3 destination.
If <prefix> is non-empty, it will have a slash appended if necessary.

The <src-dir> argument is interpreted similarly to rsync: if it ends with a /,
no directory is created in the S3 destination. If it does not end with a /,
the directory at the end of <src-dir> is created.
`)

	flagSet.PrintDefaults()
}

func (stc *S3TreeClone) SetRootFromNFSNobody() error {
	nobody, err := user.Lookup("nfsnobody")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Use nfsnobody does not exist: %s\n", err)
		return err
	}

	rootUID, err := strconv.ParseUint(nobody.Uid, 10, 32)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to convert nfsnobody UID to int: %s: %v\n", nobody.Uid, err)
		return err
	}

	rootGID, err := strconv.ParseUint(nobody.Gid, 10, 32)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to convert nfsnobody GID to int: %s: %v\n", nobody.Gid, err)
		return err
	}

	stc.rootUID = uint32(rootUID)
	stc.rootGID = uint32(rootGID)
	return nil
}

func (stc *S3TreeClone) SetBucketAndPrefix(dest string) error {
	if !strings.HasPrefix(dest, "s3://") {
		return fmt.Errorf("Destination must be an S3 URL\n")
	}

	bucketAndPrefix := strings.TrimPrefix(dest, "s3://")
	bucketAndPrefixParts := strings.SplitN(bucketAndPrefix, "/", 2)

	if len(bucketAndPrefixParts) != 2 {
		stc.bucket = bucketAndPrefixParts[0]
		stc.prefix = ""
	} else {
		stc.bucket = bucketAndPrefixParts[0]
		stc.prefix = strings.TrimRight(bucketAndPrefixParts[1], "/")
		if len(stc.prefix) > 0 {
			stc.prefix += "/"
		}
	}

	return nil
}

func (stc *S3TreeClone) ReconfigureS3ClientFromBucketLocation(configOptions []func(*config.LoadOptions) error) error {
	// Make sure the bucket exists and we have basic permissions for it.
	gblo, err := stc.s3Client.GetBucketLocation(stc.ctx, &s3.GetBucketLocationInput{Bucket: &stc.bucket})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to get location for S3 bucket %s: %v\n", stc.bucket, err)
		return err
	}

	var bucketRegion string
	if gblo.LocationConstraint == s3Types.BucketLocationConstraintEu {
		bucketRegion = "eu-west-1"
	} else if gblo.LocationConstraint == "" {
		bucketRegion = "us-east-1"
	} else {
		bucketRegion = string(gblo.LocationConstraint)
	}

	configOptions = append(configOptions, config.WithRegion(bucketRegion))
	awsConfig, err := config.LoadDefaultConfig(stc.ctx, configOptions...)
	if err != nil {
		panic(err)
	}

	stc.s3Client = s3.NewFromConfig(awsConfig)
	return nil
}

func (stc *S3TreeClone) WalkDirectory(relPath string, dirName string, filter string) error {
	var dir *os.File
	var err error

	dir, err = os.OpenFile(dirName, os.O_RDONLY, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to open directory %s: %v\n", dirName, err)
		return err
	}

	for {
		var names []string
		names, err = dir.Readdirnames(16)
		if len(names) == 0 {
			if err == io.EOF {
				break
			} else {
				fmt.Fprintf(os.Stderr, "Unable to read directory %s: %v\n", dirName, err)
				return err
			}
		}

		for _, name := range names {
			if filter != "" && name != filter {
				continue
			}

			go stc.HandleFile(relPath, dirName, name)
			stc.waitGroup.Add(1)
		}
	}

	return nil
}

func (stc *S3TreeClone) HandleFile(relPath, dirName, filename string) {
	defer stc.waitGroup.Done()

	pathname := path.Join(dirName, filename)
	if strings.Contains(pathname, "//") {
		panic(fmt.Sprintf("HandleFile encountered a pathname with '//': relPath=%#v dirName=%#v filename=%#v pathname=%#v", relPath, dirName, filename, pathname))
	}
	fileinfo, err := os.Stat(pathname)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to get status of %s: %v\n", pathname, err)
		return
	}
	stat := fileinfo.Sys().(*syscall.Stat_t)
	mode := fileinfo.Mode()
	uploadRequired := false

	if !mode.IsDir() && !mode.IsRegular() {
		// Skip devices, pipes, sockets, etc.
		if stc.verbose {
			fmt.Printf("Skipping non-regular file %s\n", pathname)
		}
		return
	}

	// Check what we have in S3
	key := path.Join(stc.prefix, relPath, filename)

	if mode.IsDir() {
		key += "/"
	}

	// Check out a semaphore to ensure we're not overloading S3 with too many concurrent requests
	err = stc.sem.Acquire(stc.ctx, 1)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to acquire S3 semaphore: %v\n", err)
		return
	}

	if stc.verbose {
		fmt.Printf("Comparing %s against s3://%s/%s\n", pathname, stc.bucket, key)
	}

	hoo, err := stc.s3Client.HeadObject(stc.ctx, &s3.HeadObjectInput{Bucket: &stc.bucket, Key: &key})
	stc.sem.Release(1)

	if err != nil {
		// Assume the object must be resynced.
		var smithyError smithy.APIError
		showError := true
		if errors.As(err, &smithyError) {
			if smithyError.ErrorCode() == "NotFound" {
				showError = false
			}
		}

		if showError {
			fmt.Fprintf(os.Stderr, "HeadObject on s3://%s/%s failed; will resync object: %v\n", stc.bucket, key,
				err)
		} else if stc.verbose {
			fmt.Printf("s3://%s/%s does not exist; will resync object\n", stc.bucket, key)
		}

		uploadRequired = true
	} else if !stc.FileMetadataEqual(hoo, stat, pathname, key, mode.IsDir()) {
		uploadRequired = true
	}

	if !mode.IsDir() {
		// Get the hashes for the file.
		var hashes *Hashes

		if hoo != nil {
			var hashesEqual bool
			hashes, hashesEqual, err = compareFileHashes(hoo, pathname)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Unable to get hashes for %s: %v\n", pathname, err)
				return
			}

			if !hashesEqual {
				fmt.Fprintf(os.Stderr, "File hashes differ for s3://%s/%s and %s; will resync object\n", stc.bucket, key, pathname)
				uploadRequired = true
			} else if stc.verbose {
				fmt.Printf("Hash values for %s and s3://%s/%s match\n", pathname, stc.bucket, key)
			}
		}

		if uploadRequired {
			stc.UploadFile(pathname, key, stat, hashes)
		}
	} else {
		if uploadRequired {
			stc.UploadDir(pathname, key, stat)
		}
		// Walk this directory
		fmt.Fprintf(os.Stderr, "Walking directory %s\n", pathname)
		subdir := path.Join(relPath, filename)
		_ = stc.WalkDirectory(subdir, pathname, "")
		return
	}
}

func (stc *S3TreeClone) FileMetadataEqual(hoo *s3.HeadObjectOutput, stat *syscall.Stat_t, pathname, key string, isDir bool) bool {
	// Check size
	if !isDir && hoo.ContentLength != stat.Size {
		fmt.Fprintf(os.Stderr, "Content size mismatch: s3://%s/%s has size %d; %s has size %d; will resync\n", stc.bucket, key, hoo.ContentLength, pathname, stat.Size)
		return false
	}

	uid := stat.Uid
	gid := stat.Gid

	if uid == 0 {
		uid = stc.rootUID
	}

	if gid == 0 {
		gid = stc.rootGID
	}

	// Make sure uid/gid ownership match
	if !fileOwnershipEqual(hoo, uid, stc.bucket, key, pathname, "file-owner") || !fileOwnershipEqual(hoo, gid, stc.bucket, key, pathname, "file-group") {
		return false
	}

	// Check permissions
	s3PermsStr, isPresent := hoo.Metadata["file-permissions"]
	if !isPresent {
		fmt.Fprintf(os.Stderr, "No file-permissions specified for s3://%s/%s; will resync\n", stc.bucket, key)
		return false
	}

	s3Perms, err := strconv.ParseUint(s3PermsStr, 8, 16)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Non-integer value for file-permissions for s3://%s/%s; will resync: %s\n", stc.bucket, key, s3PermsStr)
		return false
	}

	if uint16(s3Perms) != uint16(stat.Mode&07777) {
		fmt.Fprintf(os.Stderr, "Permissions mismatch: s3://%s/%s has %04o; %s has %04o; will resync\n", stc.bucket, key, s3Perms, pathname, stat.Mode&07777)
		return false
	}

	// Check timestamps if requested
	if !stc.ignoreTimestamps {
		if !fileTimestampEqual(hoo, getCtime(stat), stc.bucket, key, pathname, "file-ctime") || !fileTimestampEqual(hoo, getMtime(stat), stc.bucket, key, pathname, "file-mtime") {
			return false
		}
	}

	if stc.verbose {
		fmt.Printf("Metadata for %s and s3://%s/%s matches\n", pathname, stc.bucket, key)
	}

	return true
}

func fileOwnershipEqual(hoo *s3.HeadObjectOutput, id uint32, bucket, key, pathname, ownerType string) bool {
	s3OwnerStr, isPresent := hoo.Metadata[ownerType]
	if !isPresent {
		fmt.Fprintf(os.Stderr, "No %s specified for s3://%s/%s; will resync\n", ownerType, bucket, key)
		return false
	}

	s3Owner, err := strconv.ParseUint(s3OwnerStr, 10, 32)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Non-integer value for %s for s3://%s/%s; will resync: %s\n", ownerType, bucket, key, s3OwnerStr)
		return false
	}

	if uint32(s3Owner) != id {
		fmt.Fprintf(os.Stderr, "Ownership mismatch: s3://%s/%s has %s %d; %s has %s %d; will resync\n", bucket, key, ownerType, s3Owner, pathname, ownerType, id)
		return false
	}

	return true
}

// fileTimestampEqual determines whether the timestamps on the local file and S3 object are
// identical. If the timestamp metadata is missing from S3, it is assumed the timestamps are not
// identical.
func fileTimestampEqual(hoo *s3.HeadObjectOutput, timestamp int64, bucket, key, pathname, field string) bool {
	s3TimestampStr, isPresent := hoo.Metadata[field]
	if !isPresent {
		fmt.Fprintf(os.Stderr, "No %s specified for s3://%s/%s; will resync\n", field, bucket, key)
		return false
	}

	s3Timestamp, err := time.ParseDuration(s3TimestampStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot parse %s for s3://%s/%s; will resync: %s: %v", field, bucket, key, s3TimestampStr, err)
		return false
	}

	timestampNS := time.Duration(timestamp)

	if s3Timestamp != timestampNS {
		fmt.Fprintf(os.Stderr, "Timestamp mismatch: s3://%s/%s has %s %d ns; %s has %s %d ns; will resync\n", bucket, key, field, int64(s3Timestamp), pathname, field, int64(timestampNS))
		return false
	}

	return true
}

// UploadDir creates a directory entry in S3 with the given key, using the permissions, ownership,
// and timestamp from the source directory.
func (stc *S3TreeClone) UploadDir(pathname, key string, stat *syscall.Stat_t) {
	uid := stat.Uid
	gid := stat.Gid

	// Substitute root UID/GID if necessary.
	if uid == 0 {
		uid = stc.rootUID
	}

	if gid == 0 {
		gid = stc.rootGID
	}

	// File Gateway always uses 4-digit octal modes.
	modeStr := fmt.Sprintf("%04o", stat.Mode&07777)

	// File Gateway always uses nanosecond timestamps since the Unix epoch.
	ctimeStr := fmt.Sprintf("%dns", getCtime(stat))
	mtimeStr := fmt.Sprintf("%dns", getMtime(stat))

	// File Gateway uses the generic "application/octet-stream" for the content-type
	mtypeStr := "application/octet-stream"

	metadata := make(map[string]string)
	metadata["file-owner"] = fmt.Sprintf("%d", uid)
	metadata["file-group"] = fmt.Sprintf("%d", gid)
	metadata["file-permissions"] = modeStr
	metadata["file-ctime"] = ctimeStr
	metadata["file-mtime"] = mtimeStr
	metadata["user-agent"] = "s3-tree-clone"

	// We don't need parallelism here.
	err := stc.sem.Acquire(stc.ctx, 1)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to acquire S3 semaphore: %v\n", err)
		return
	}
	defer stc.sem.Release(1)

	poi := &s3.PutObjectInput{
		Bucket:               &stc.bucket,
		Key:                  &key,
		Body:                 &bytes.Reader{},
		ContentType:          &mtypeStr,
		Metadata:             metadata,
		ServerSideEncryption: stc.encAlg,
		StorageClass:         stc.storageClass,
	}

	if stc.encAlg == s3Types.ServerSideEncryptionAwsKms {
		poi.SSEKMSKeyId = &stc.kmsKey
	}

	_, err = stc.s3Client.PutObject(stc.ctx, poi)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to upload %s: %v\n", pathname, err)
		return
	}

	fmt.Fprintf(os.Stderr, "Uploaded %s to s3://%s/%s\n", pathname, stc.bucket, key)
}

// UploadFile creates an object in S3 with the given key, using the permissions, ownership, and
// timestamp from the source file to set the metadata. The file is uploaded as the S3 object
// content. The Content-Type is set using MIME detection.
func (stc *S3TreeClone) UploadFile(pathname, key string, stat *syscall.Stat_t, hashes *Hashes) {
	uid := stat.Uid
	gid := stat.Gid

	// Substitute root UID/GID if necessary.
	if uid == 0 {
		uid = stc.rootUID
	}

	if gid == 0 {
		gid = stc.rootGID
	}

	// File Gateway always uses 4-digit octal modes.
	modeStr := fmt.Sprintf("%04o", stat.Mode&07777)

	// File Gateway always uses nanosecond timestamps since the Unix epoch.
	ctimeStr := fmt.Sprintf("%dns", getCtime(stat))
	mtimeStr := fmt.Sprintf("%dns", getMtime(stat))

	mtype, err := mimetype.DetectFile(pathname)
	var mtypeStr string
	if err != nil {
		fmt.Fprintf(os.Stderr, "Cannot detect mime-type for %s: %v\n", pathname, err)
		mtypeStr = "application/octet-stream"
	} else {
		mtypeStr = mtype.String()
	}

	metadata := make(map[string]string)
	metadata["file-owner"] = fmt.Sprintf("%d", uid)
	metadata["file-group"] = fmt.Sprintf("%d", gid)
	metadata["file-permissions"] = modeStr
	metadata["file-ctime"] = ctimeStr
	metadata["file-mtime"] = mtimeStr
	metadata["user-agent"] = "s3-tree-clone"

	fd, err := os.Open(pathname)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to open %s: %v\n", pathname, err)
		return
	}

	defer fd.Close()

	// If we don't already have hashes for the file, gather them and add them to the metadata.
	if hashes == nil {
		hashes, err = getFileHashes(fd)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to get hashes of %s: %v\n", pathname, err)
			return
		}
		_, err = fd.Seek(0, io.SeekStart)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to seek to start of %s: %v\n", pathname, err)
			return
		}
	}

	metadata["md5"] = hex.EncodeToString(hashes.MD5)
	metadata["sha1"] = hex.EncodeToString(hashes.SHA1)
	metadata["sha256"] = hex.EncodeToString(hashes.SHA256)
	metadata["sha512"] = hex.EncodeToString(hashes.SHA512)

	uploader := manager.NewUploader(stc.s3Client)
	uploader.Concurrency = 5
	err = stc.sem.Acquire(stc.ctx, 5)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to acquire S3 semaphore: %v\n", err)
		return
	}
	defer stc.sem.Release(5)

	poi := &s3.PutObjectInput{
		Bucket:               &stc.bucket,
		Key:                  &key,
		Body:                 fd,
		ContentType:          &mtypeStr,
		Metadata:             metadata,
		ServerSideEncryption: stc.encAlg,
		StorageClass:         stc.storageClass,
	}

	if stc.encAlg == s3Types.ServerSideEncryptionAwsKms {
		poi.SSEKMSKeyId = &stc.kmsKey
	}

	_, err = uploader.Upload(stc.ctx, poi)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to upload %s: %v\n", pathname, err)
		return
	}

	fmt.Fprintf(os.Stderr, "Uploaded %s to s3://%s/%s\n", pathname, stc.bucket, key)
}

// getFileHashes simultaneously calculates the MD5, SHA1, SHA256, and SHA512 hashes of a given file.
func getFileHashes(fd io.Reader) (*Hashes, error) {
	hashMd5 := md5.New()
	hashSha1 := sha1.New()
	hashSha256 := sha256.New()
	hashSha512 := sha512.New()

	buffer := make([]byte, 1024*1024)
	for {
		var nRead, nWritten int
		var err error
		nRead, err = fd.Read(buffer)
		if nRead <= 0 {
			if err == io.EOF {
				break
			} else {
				return nil, err
			}
		}

		nWritten, err = hashMd5.Write(buffer[:nRead])
		if nWritten != nRead {
			return nil, fmt.Errorf("Failed to write %d bytes to MD5 hash: %v", nRead, err)
		}

		nWritten, err = hashSha1.Write(buffer[:nRead])
		if nWritten != nRead {
			return nil, fmt.Errorf("Failed to write %d bytes to SHA1 hash: %v", nRead, err)
		}

		hashSha256.Write(buffer[:nRead])
		if nWritten != nRead {
			return nil, fmt.Errorf("Failed to write %d bytes to SHA256 hash: %v", nRead, err)
		}

		hashSha512.Write(buffer[:nRead])
		if nWritten != nRead {
			return nil, fmt.Errorf("Failed to write %d bytes to SHA512 hash: %v", nRead, err)
		}
	}

	return &Hashes{
		MD5:    hashMd5.Sum(nil),
		SHA1:   hashSha1.Sum(nil),
		SHA256: hashSha256.Sum(nil),
		SHA512: hashSha512.Sum(nil),
	}, nil
}

// compareFileHashes attempts to compare the local file vs the file stored in S3 using (in order)
// SHA-512, SHA-256, SHA-1, then MD5 (according to the first hash metadata marker found).
// If hash metadata is not present, this check is skipped; we do this because AWS File Gateway
// does not store hashes in the metadata.
//
// Note that the S3 ETag header is useless for this purpose -- for encrypted buckets, this is *not*
// the MD5 of the plaintext file. (Even for non-encrypted buckets, it's not guaranteed to be the
// MD5 sum of the file, or the MD5 sum of the MD5 sums of multipart uploads.)
func compareFileHashes(hoo *s3.HeadObjectOutput, pathname string) (*Hashes, bool, error) {
	metadata := hoo.Metadata
	s3SHA512 := metadata["sha512"]
	s3SHA256 := metadata["sha256"]
	s3SHA1 := metadata["sha1"]
	s3MD5 := metadata["md5"]

	if s3SHA512 == "" && s3SHA256 == "" && s3SHA1 == "" && s3MD5 == "" {
		// None of our hashes are in the metadata; no comparison is possible.
		// We optimistically assume the file is ok if all other checks (length, mtime, ctime) pass.
		return nil, true, nil
	}

	fd, err := os.Open(pathname)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to open %s: %v\n", pathname, err)
		return nil, false, err
	}
	defer fd.Close()

	hashes, err := getFileHashes(fd)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to get hashes for %s: %v\n", pathname, err)
		return nil, false, err
	}

	localSHA512 := hex.EncodeToString(hashes.SHA512)
	localSHA256 := hex.EncodeToString(hashes.SHA256)
	localSHA1 := hex.EncodeToString(hashes.SHA1)
	localMD5 := hex.EncodeToString(hashes.MD5)

	if s3SHA512 != "" {
		return hashes, s3SHA512 == localSHA512, nil
	}

	if s3SHA256 != "" {
		return hashes, s3SHA256 == localSHA256, nil
	}

	// Less desirable algorithms, but better than nothing.
	if s3SHA1 != "" {
		return hashes, s3SHA1 == localSHA1, nil
	}

	return hashes, s3MD5 == localMD5, nil
}
