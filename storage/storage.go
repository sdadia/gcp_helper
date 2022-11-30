package storage

import (
	"bytes"
	"cloud.google.com/go/storage"
	"context"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
	"io"
	"os"
)

func CreateClient(ctx context.Context) (*storage.Client, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	} else {
		return client, nil
	}
}

func CreateBucket(storageClient *storage.Client, ctx context.Context, projectId string, bucketName string, bktAttrs *storage.BucketAttrs) error {
	// check if bucket exists
	var err error = nil
	_, err = storageClient.Bucket(bucketName).Attrs(ctx)
	// only create if the bucket does not exist
	if err == storage.ErrBucketNotExist {
		log.Debugf("Creating bucket %s", bucketName)
		// create the bucket
		err = storageClient.Bucket(bucketName).Create(ctx, projectId, bktAttrs)
		if err != nil {
			log.Fatalf("Error while creating bucket. Error is %v", err)
			return err
		}
		return nil
	}
	if err != nil {
		return err
	}
	return nil
}

func ListBuckets(storageClient *storage.Client, ctx context.Context, projectId string) ([]string, error) {
	// container to hold all bucket names
	var bucketNames []string

	// list buckets
	var bktIterator = storageClient.Buckets(ctx, projectId)
	for {
		var bucket, err = bktIterator.Next()
		// Break at end
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("Error while iterating thorugh buckets. Error is %v\n", err)
			return bucketNames, err
		} else {
			bucketNames = append(bucketNames, bucket.Name)
		}
	}
	return bucketNames, nil
}

func DeleteBucket(storageClient *storage.Client, ctx context.Context, bucketName string) error {
	// check if bucket exists
	var err error = nil
	_, err = storageClient.Bucket(bucketName).Attrs(ctx)
	// only delete if the bucket exists
	if err == storage.ErrBucketNotExist {
		log.Fatalf("Bucket does not exist. Error is %v", err)
		return err
	}
	if err != nil {
		return err
	}
	log.Warnf("Deleting bucket %s", bucketName)
	err = storageClient.Bucket(bucketName).Delete(ctx)
	if err != nil {
		return err
	}
	return nil
}

func UploadLocalFile(storageClient *storage.Client, ctx context.Context, bucketName string, localFilePath string, KeyName string) error {

	// check if bucket exists
	var _, err = storageClient.Bucket(bucketName).Attrs(ctx)
	if err == storage.ErrBucketNotExist {
		log.Fatalf("The bucket %s does not exist\n", bucketName)
		return err
	}
	if err != nil {
		log.Fatalf("Error while loading bucket %s\n", bucketName)
		return err
	}

	// create writer object
	var writer = storageClient.Bucket(bucketName).Object(KeyName).NewWriter(ctx)

	// load local file
	body, err := os.ReadFile(localFilePath)
	if err != nil {
		log.Fatalf("Error while opening file %s. Error is : %v\n", localFilePath, err)
		return err
	}

	// write to object
	if _, err = io.Copy(writer, bytes.NewBuffer(body)); err != nil {
		log.Fatalf("Error while copying data to object. Error is %v\n", err)
		return err
	}

	// Data can continue to be added to the file until the writer is closed.
	err = writer.Close()
	if err != nil {
		log.Fatalf("Error while writing data to file. Error is %v\n", err)
		return err
	}
	return nil
}

func ReadFile(storageClient *storage.Client, ctx context.Context, bucketName string, objectName string) ([]byte, error) {
	// Read the object from bucket.
	rc, err := storageClient.Bucket(bucketName).Object(objectName).NewReader(ctx)
	if err == storage.ErrObjectNotExist {
		log.Fatalf("Object %s does not exist", objectName)
		return nil, err
	}
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	defer rc.Close()
	body, err := io.ReadAll(rc)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	return body, nil
}

func listObjects(storageClient *storage.Client, ctx context.Context, bucketName string) ([]string, error) {
	var objectNames []string
	var objIterator = storageClient.Bucket(bucketName).Objects(ctx, nil)
	for {
		objAttr, err := objIterator.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("Error reading objects from %s. Error is %v\n", bucketName, err)
		} else {
			objectNames = append(objectNames, objAttr.Name)
		}
	}
	return objectNames, nil
}
