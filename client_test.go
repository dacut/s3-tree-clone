package main

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/base32"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

type s3TestObject struct {
	CacheControl       *string
	ContentDisposition *string
	ContentEncoding    *string
	ContentLanguage    *string
	ContentLength      int64
	ContentType        *string
	DeleteMarker       bool
	ETag               *string
	Expires            *time.Time
	LastModified       *time.Time
	Metadata           map[string]string
	MissingMeta        int32
	PartsCount         int32
	VersionId          *string
}

type s3TestBucket struct {
	Name     string
	Location s3Types.BucketLocationConstraint
	Objects  map[string]*s3TestObject
}

type s3TestClientBase struct {
	Buckets map[string]*s3TestBucket
}

func (c *s3TestClientBase) AbortMultipartUpload(ctx context.Context, input *s3.AbortMultipartUploadInput, opts ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	return &s3.AbortMultipartUploadOutput{}, nil
}

func (c *s3TestClientBase) CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	return &s3.CompleteMultipartUploadOutput{
		Bucket:               input.Bucket,
		Location:             aws.String(fmt.Sprintf("https://%s/%s", *input.Bucket, *input.Key)),
		Key:                  input.Key,
		ETag:                 aws.String("\"00000000000000000000000000000000\""),
		VersionId:            aws.String("000000000000"),
		ServerSideEncryption: s3Types.ServerSideEncryptionAes256,
	}, nil
}

func (c *s3TestClientBase) CreateMultipartUpload(ctx context.Context, input *s3.CreateMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	return &s3.CreateMultipartUploadOutput{
		Bucket:               input.Bucket,
		Key:                  input.Key,
		ServerSideEncryption: s3Types.ServerSideEncryptionAes256,
		UploadId:             aws.String("00000000"),
	}, nil
}

func (c *s3TestClientBase) GetBucketLocation(ctx context.Context, input *s3.GetBucketLocationInput, opts ...func(*s3.Options)) (*s3.GetBucketLocationOutput, error) {
	if c.Buckets == nil {
		c.Buckets = make(map[string]*s3TestBucket)
	}

	bucket, found := c.Buckets[*input.Bucket]
	if !found {
		err := makeS3Error("GetBucketLocation", 404, "Not Found", "NoSuchBucket", "The specified bucket does not exist")
		return nil, err
	}

	return &s3.GetBucketLocationOutput{
		LocationConstraint: bucket.Location,
	}, nil
}

func (c *s3TestClientBase) HeadObject(ctx context.Context, input *s3.HeadObjectInput, opts ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if c.Buckets == nil {
		c.Buckets = make(map[string]*s3TestBucket)
	}

	bucket, found := c.Buckets[*input.Bucket]
	if !found {
		return nil, makeS3Error("HeadObject", 404, "Not Found", "NotFound", "Not Found")
	}

	if bucket.Objects == nil {
		bucket.Objects = make(map[string]*s3TestObject)
	}

	object, found := bucket.Objects[*input.Key]
	if !found {
		return nil, makeS3Error("HeadObject", 404, "Not Found", "NotFound", "Not Found")
	}

	return &s3.HeadObjectOutput{
		CacheControl:       copyAWSString(object.CacheControl),
		ContentDisposition: copyAWSString(object.ContentDisposition),
		ContentEncoding:    copyAWSString(object.ContentEncoding),
		ContentLanguage:    copyAWSString(object.ContentLanguage),
		ContentLength:      object.ContentLength,
		ContentType:        copyAWSString(object.ContentType),
		DeleteMarker:       object.DeleteMarker,
		ETag:               copyAWSString(object.ETag),
		Expires:            object.Expires,
		LastModified:       copyAWSTime(object.LastModified),
		Metadata:           copyAWSMapStringString(object.Metadata),
		MissingMeta:        object.MissingMeta,
		PartsCount:         object.PartsCount,
		VersionId:          object.VersionId,
	}, nil
}

func (stc *s3TestClientBase) PutObject(ctx context.Context, input *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	bucket, found := stc.Buckets[*input.Bucket]
	if !found {
		bucket = &s3TestBucket{
			Name: *input.Bucket,
		}
		stc.Buckets[*input.Bucket] = bucket
	}

	hasher := md5.New()
	buffer := make([]byte, 65536)
	var totalSize int64
	for {
		n, err := input.Body.Read(buffer)
		if err != nil {
			break
		}
		hasher.Write(buffer[:n])
		totalSize += int64(n)
	}

	object := &s3TestObject{
		CacheControl:       copyAWSString(input.CacheControl),
		ContentDisposition: copyAWSString(input.ContentDisposition),
		ContentEncoding:    copyAWSString(input.ContentEncoding),
		ContentLanguage:    copyAWSString(input.ContentLanguage),
		ContentLength:      totalSize,
		ContentType:        copyAWSString(input.ContentType),
		ETag:               aws.String(fmt.Sprintf("\"%s\"", hex.EncodeToString(hasher.Sum(nil)))),
		Expires:            copyAWSTime(input.Expires),
		LastModified:       aws.Time(time.Now().UTC()),
		Metadata:           copyAWSMapStringString(input.Metadata),
		VersionId:          aws.String("000000000000"),
	}

	bucket.Objects[*input.Key] = object

	return &s3.PutObjectOutput{
		ETag:                 copyAWSString(object.ETag),
		ServerSideEncryption: s3Types.ServerSideEncryptionAes256,
		VersionId:            copyAWSString(object.VersionId),
	}, nil
}

func (stc *s3TestClientBase) UploadPart(ctx context.Context, input *s3.UploadPartInput, opts ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	return &s3.UploadPartOutput{
		ETag:                 aws.String("\"00000000000000000000000000000000\""),
		ServerSideEncryption: s3Types.ServerSideEncryptionAes256,
	}, nil
}

// S3TestResponseError implements S3ResponseError
type S3TestResponseError struct {
	*awshttp.ResponseError
	HostID string
}

func (stre *S3TestResponseError) Error() string {
	return fmt.Sprintf(
		"https response error StatusCode: %d, RequestID: %s, HostID: %s, %v",
		stre.Response.StatusCode, stre.RequestID, stre.HostID, stre.Err)
}

func (stre *S3TestResponseError) ServiceHostID() string {
	return stre.HostID
}

func (stre *S3TestResponseError) ServiceRequestID() string {
	return stre.RequestID
}

func (stre *S3TestResponseError) As(target interface{}) bool {
	return errors.As(stre.ResponseError, target)
}

type testEmptyDotDirClient struct {
	s3TestClientBase
}

func makeS3Error(operation string, statusCode int, statusReason, errorCode, errorMessage string) *smithy.OperationError {
	requestID := generateRequestID()
	amzID2 := generateAmzID2()

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC1123)

	header := http.Header{
		"X-Amz-Request-Id": []string{requestID},
		"X-Amz-Id-2":       []string{amzID2},
		"Content-Type":     []string{"application/xml"},
		"Date":             []string{nowStr},
		"Server":           []string{"AmazonS3"},
	}

	return &smithy.OperationError{
		ServiceID:     "S3",
		OperationName: operation,
		Err: &S3TestResponseError{
			ResponseError: &awshttp.ResponseError{
				ResponseError: &smithyhttp.ResponseError{
					Response: &smithyhttp.Response{
						Response: &http.Response{
							Status:     fmt.Sprintf("%03d %s", statusCode, statusReason),
							StatusCode: statusCode,
							Proto:      "HTTP/1.1",
							ProtoMajor: 1,
							ProtoMinor: 1,
							Header:     header,
						},
					},
					Err: &smithy.GenericAPIError{
						Code:    errorCode,
						Message: errorMessage,
					},
				},
				RequestID: generateRequestID(),
			},
			HostID: "localhost",
		},
	}
}

func generateRequestID() string {
	reqIDRaw := make([]byte, 10)
	_, err := rand.Read(reqIDRaw)
	if err != nil {
		panic(fmt.Sprintf("Failed to read %d random bytes", len(reqIDRaw)))
	}

	return base32.StdEncoding.EncodeToString(reqIDRaw)
}

func generateAmzID2() string {
	reqIDRaw := make([]byte, 56)
	_, err := rand.Read(reqIDRaw)
	if err != nil {
		panic(fmt.Sprintf("Failed to read %d random bytes", len(reqIDRaw)))
	}

	return base64.StdEncoding.EncodeToString(reqIDRaw)
}

func copyAWSString(s *string) *string {
	if s == nil {
		return nil
	}
	return aws.String(*s)
}

func copyAWSTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	return aws.Time(*t)
}

func copyAWSMapStringString(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	result := make(map[string]string, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}
