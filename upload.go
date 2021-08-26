// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	storage "cloud.google.com/go/storage"
)

func main() {
	ctx := context.Background()

	// parse args
	sourceFileName := flag.String("sourceFile", "", "The file to upload.")
	destinationURL := flag.String("destinationURL", "", "The URL to upload to.")
	tempPrefix := flag.String("tempPrefix", "temp/", "The prefix to put temporary component objects.")
	flag.Parse()
	fmt.Println("source file: ", *sourceFileName)
	fmt.Println("destination URL: ", *destinationURL)
	fmt.Println("temp prefix: ", *tempPrefix)

	// get a file handle
	sourceFile, err := os.Open(*sourceFileName)
	if err != nil {
		panic(err)
	}
	defer sourceFile.Close()

	// get a gcs client
	gcs, err := storage.NewClient(ctx)
	if err != nil {
		panic(err)
	}
	defer gcs.Close()

	// good enough, let's try the upload
	start := time.Now()
	bytes, err := CompositeUpload(ctx, gcs, sourceFile, *destinationURL, *tempPrefix)
	if err != nil {
		panic(err)
	}
	elapsed := time.Since(start)
	log.Printf("Uploaded %v bytes in %.3f seconds", bytes, elapsed.Seconds())
	bytesPerSecond := float64(bytes) / elapsed.Seconds()
	bitsPerSecond := bytesPerSecond * 8
	Gbps := bitsPerSecond / (1024 * 1024 * 1024)
	log.Printf("Approximately %.2fGbps", Gbps)
}

// CompositeUpload uploads a file to GCS in multiple threads using the compose API. It returns the size of the file uploaded in bytes.
func CompositeUpload(ctx context.Context, gcs *storage.Client, sourceFile *os.File, destinationURL string, tempPrefix string) (int64, error) {
	// some tunables
	// less than 16 MiB, you start to lose goodput
	minimumComponentSize := int64(16 * 1000 * 1000)
	// if we can't do at least a couple slices, we should do a straight shot instead.
	minimumFileSize := minimumComponentSize * 2
	// how many components relative to the number of CPUs
	overloadFactor := 2

	// get the file's size
	fi, err := sourceFile.Stat()
	if err != nil {
		panic(err)
	}
	// validate that this isn't better done in one shot. RPCs take time.
	if fi.Size() < minimumFileSize {
		panic(fmt.Errorf("CompositeUpload: file %v is small enough (<%v bytes) that it's better to upload in a single-shot", sourceFile.Name(), minimumFileSize))
	}

	// get the number of cores, multiply by an overload factor
	// to keep things simple on compose, will set a ceiling of 32, but you can compose
	// infinitely https://cloud.google.com/community/tutorials/cloud-storage-infinite-compose
	maxComponentCount := min(32, int64(runtime.NumCPU()*overloadFactor))
	// calculate the component size.
	componentSize := max(minimumComponentSize, fi.Size()/int64(maxComponentCount))
	// the number of components will be simple division, plus one for the last partial component
	componentCount := fi.Size()/componentSize + 1
	// we need to make a threadsafe slice that can accept the component names
	components := make([]string, componentCount)

	// get the bucket handle where we want to put the data
	bucketName, finalObject := parseGCSURL(destinationURL)
	bucket := gcs.Bucket(bucketName)

	// now set up copies from each component range to a GCS component blob
	var componentWG sync.WaitGroup
	offset := int64(0)
	for componentNumber := int64(0); componentNumber < componentCount; componentNumber++ {
		componentWG.Add(1)
		go func(wg *sync.WaitGroup, cn int64, seekTo int64) {
			defer wg.Done()

			//client-per-goroutine. You shouldn't need this, but you do. They seem to share something.
			g, err := storage.NewClient(ctx)
			if err != nil {
				panic(err)
			}
			defer g.Close()
			b := g.Bucket(bucketName)

			// set up component object & writer
			componentName := tempPrefix + finalObject + "cmpnt_" + fmt.Sprint(cn)
			object := b.Object(componentName)
			components[cn] = componentName // we will need this later!
			log.Printf("uploading gs://%v/%v\n", object.BucketName(), object.ObjectName())
			c, cancel := context.WithTimeout(ctx, time.Second*50)
			defer cancel()
			objectWriter := object.NewWriter(c)

			// get a unique file handle for this work
			fh, err := os.Open(sourceFile.Name())
			if err != nil {
				panic(err)
			}
			defer fh.Close()

			// seek and limit the reader
			fh.Seek(seekTo, 0)
			limitedFh := io.LimitReader(fh, componentSize)

			// copy it on up
			if _, err = io.Copy(objectWriter, limitedFh); err != nil {
				panic(fmt.Errorf("io.Copy: %v", err))
			}
			if err := objectWriter.Close(); err != nil {
				panic(fmt.Errorf("Writer.Close: %v", err))
			}
			log.Printf("uploaded gs://%v/%v\n", object.BucketName(), object.ObjectName())
		}(&componentWG, componentNumber, offset)

		// next goroutine should use a range beginning at the end of the last
		offset = offset + componentSize + 1
	}
	componentWG.Wait()
	log.Printf("component uploads complete\n")

	// compose the components into the desired object
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	doCompose(ctx, bucket, finalObject, components)
	log.Printf("compose complete\n")

	// cleanup
	doDeletes(ctx, bucket, components)
	log.Printf("deletes complete\n")

	return fi.Size(), nil
}

// doCompose composes the objects in sourceNames into finalObject.
func doCompose(ctx context.Context, bucket *storage.BucketHandle, finalObject string, sourceNames []string) error {
	destination := bucket.Object(finalObject)
	sources := namesToObjects(sourceNames, bucket)
	_, err := destination.ComposerFrom(sources...).Run(ctx)
	if err != nil {
		return fmt.Errorf("ComposerFrom: %v", err)
	}
	return nil
}

// doDeletes deletes the objects in the componentNames slice. They have to be in bucket.
func doDeletes(ctx context.Context, bucket *storage.BucketHandle, componentNames []string) {
	var deleteWG sync.WaitGroup
	components := namesToObjects(componentNames, bucket)
	for _, component := range components {
		deleteWG.Add(1)

		go func(wg *sync.WaitGroup, obj *storage.ObjectHandle) {
			defer wg.Done()
			err := obj.Delete(ctx)
			if err != nil {
				panic(fmt.Errorf("delete: %v", err))
			}
		}(&deleteWG, component)

	}
	deleteWG.Wait()
}

// namesToObjects converts a slice of string object names to object handles.
func namesToObjects(names []string, bucket *storage.BucketHandle) []*storage.ObjectHandle {
	var objects []*storage.ObjectHandle
	for _, name := range names {
		objects = append(objects, bucket.Object(name))
	}
	return objects
}

// max returns the larger of x or y.
func max(x, y int64) int64 {
	if x < y {
		return y
	}
	return x
}

// min returns the smaller of x or y.
func min(x, y int64) int64 {
	if x > y {
		return y
	}
	return x
}

// parseGCSURL returns a bucket part and an object name part from a gs:// URL
func parseGCSURL(url string) (string, string) {
	url = strings.Replace(url, "gs://", "", 1)
	parts := strings.SplitN(url, "/", 2)
	return parts[0], parts[1]
}
