package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func runCapture(args []string, s3i S3Interface) (int, []byte, []byte) {
	origStdout := os.Stdout
	origStderr := os.Stderr

	defer func() {
		os.Stdout = origStdout
		os.Stderr = origStderr
	}()

	var err error
	var capturedOut, capturedErr *os.File

	capturedOut, err = os.CreateTemp("", "*.out")
	if err != nil {
		fmt.Fprintf(origStderr, "Failed to capture stdout: %v\n", err)
	} else {
		os.Stdout = capturedOut
	}

	capturedErr, err = os.CreateTemp("", "*.err")
	if err != nil {
		fmt.Fprintf(origStderr, "Failed to capture stderr: %v\n", err)
	} else {
		os.Stderr = capturedErr
	}

	result := run(context.Background(), args, s3i)
	outBytes := readCaptureFile(capturedOut, origStderr, "stdout")
	errBytes := readCaptureFile(capturedErr, origStderr, "stderr")

	return result, outBytes, errBytes
}

func readCaptureFile(f *os.File, origStderr io.Writer, name string) []byte {
	if f == nil {
		return make([]byte, 0)
	}

	_, err := f.Seek(0, io.SeekStart)
	if err != nil {
		fmt.Fprintf(origStderr, "Failed to seek %s: %v\n", name, err)
		return make([]byte, 0)
	}

	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		fmt.Fprintf(origStderr, "Failed to read %s: %v\n", name, err)
		return make([]byte, 0)
	}

	return bytes
}

func runExpect(t *testing.T, args []string, s3i S3Interface, returnCode int, outExpect []byte, errExpect []byte) {
	result, out, err := runCapture(args, s3i)
	if result != returnCode {
		t.Errorf("Expected returncode %d, got %d\nStdout: %#v\nStderr: %#v\n", returnCode, result, string(out), string(err))
	}

	if len(outExpect) > 0 && !bytes.Contains(out, outExpect) {
		t.Errorf("Expected %#v in stdout: %#v", string(outExpect), string(out))
	}

	if len(errExpect) > 0 && !bytes.Contains(err, errExpect) {
		t.Errorf("Expected %#v in stderr: %#v", string(errExpect), string(err))
	}
}

func TestNoArgs(t *testing.T) {
	runExpect(t, []string{}, nil, 2, nil, []byte("Missing source and destination"))
}

func TestOneArg(t *testing.T) {
	runExpect(t, []string{"."}, nil, 2, nil, []byte("Missing destination"))
}

func TestThreeArgs(t *testing.T) {
	runExpect(t, []string{".", "s3://test/foo", "what"}, nil, 2, nil, []byte("Unexpected argument"))
}

func TestInvalidDestURL(t *testing.T) {
	runExpect(t, []string{".", "not-an-s3-url"}, nil, 2, nil, []byte("Destination is not a valid S3 URL"))
}

func TestEmptyDotDir(t *testing.T) {
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get working directory: %v", err)
	}
	defer func() {
		err := os.Chdir(oldWD)
		if err != nil {
			t.Fatalf("Failed to chdir back to %s: %v", oldWD, err)
		}
	}()

	tmpDir, err := os.MkdirTemp("", "test-empty-dot-dir-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
		return
	}
	defer os.RemoveAll(tmpDir)

	err = os.Chdir(tmpDir)
	if err != nil {
		t.Fatalf("Failed to chdir to temporary directory %s: %v", tmpDir, err)
	}

	client := &testEmptyDotDirClient{}
	client.Buckets = make(map[string]*s3TestBucket)
	bucket := &s3TestBucket{Name: "hello"}
	client.Buckets["hello"] = bucket
	runExpect(t, []string{".", "s3://hello"}, client, 0, nil, nil)
}

func TestDotDirWithFiles(t *testing.T) {
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get working directory: %v", err)
	}
	defer func() {
		err := os.Chdir(oldWD)
		if err != nil {
			t.Fatalf("Failed to chdir back to %s: %v", oldWD, err)
		}
	}()

	tmpDir, err := os.MkdirTemp("", "test-empty-dot-dir-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
		return
	}
	defer os.RemoveAll(tmpDir)

	err = os.Chdir(tmpDir)
	if err != nil {
		t.Fatalf("Failed to chdir to temporary directory %s: %v", tmpDir, err)
	}

	for i := 0; i < 100; i++ {
		filename := fmt.Sprintf("file-%d.txt", i)
		err = ioutil.WriteFile(filename, []byte("hello"), 0644)
		if err != nil {
			t.Fatalf("Failed to write file %s: %v", filename, err)
		}
	}

	client := &testEmptyDotDirClient{}
	client.Buckets = make(map[string]*s3TestBucket)
	bucket := &s3TestBucket{Name: "hello"}
	client.Buckets["hello"] = bucket
	runExpect(t, []string{".", "s3://hello"}, client, 0, nil, nil)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("file-%d.txt", i)
		obj, found := bucket.Objects[key]
		if !found {
			t.Errorf("Expected to find object %s in bucket %s", key, bucket.Name)
		}

		if obj.ContentLength != 5 {
			t.Errorf("Expected Content-Length of %s to be 5: %d", key, obj.ContentLength)
		}
	}
}
