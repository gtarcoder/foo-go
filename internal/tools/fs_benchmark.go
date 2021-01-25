package tools

import (
	"flag"
	"fmt"
	"foo-go/pkg/ceph"
	"foo-go/pkg/utils"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

var (
	s3Region       string
	s3Endpoint     string
	packageBucket  string
	s3AccessKey    string
	s3AccessSecret string

	fileSizeKB  int
	fileCount   int
	threadCount int

	storeType      string
	operateType    string
	fsDir          string
	dataPathPrefix string
)

func init() {
	// ceph configs
	flag.StringVar(&s3Region, "s3Region", "as", "ceph s3 region name")
	flag.StringVar(&s3Endpoint, "s3Endpoint", "", "ceph s3 bucket name")
	flag.StringVar(&packageBucket, "packageBucket", "mlpipeline-test", "package bucket name")
	flag.StringVar(&s3AccessKey, "s3AccessKey", "", "s3 access key")
	flag.StringVar(&s3AccessSecret, "s3AccessSecret", "", "s3 secret access key id")
	flag.IntVar(&fileSizeKB, "fileSizeKB", 4, "file size kB")
	flag.IntVar(&fileCount, "fileCount", 3000, "file count")
	flag.IntVar(&threadCount, "threadCount", 2, "thread count")

	flag.StringVar(&storeType, "storeType", "s3", "store type")
	flag.StringVar(&operateType, "operateType", "read", "operate type")
	flag.StringVar(&fsDir, "fsDir", "/tmp", "fs dir")
	flag.StringVar(&dataPathPrefix, "dataPathPrefix", "", "data path prefix")
	flag.Parse()
}

func genTempFile(fileSize int) (string, string, error) {
	dir, err := ioutil.TempDir("/tmp", "working_dir")
	if err != nil {
		logrus.Fatal("Failed to create temporary direcotry, error: ", err.Error())
		return "", "", err
	}
	fileName := fmt.Sprintf("%s/tmpfile", dir)
	file, err := os.Create(fileName)
	file.WriteString(utils.RandomString(fileSize))
	defer file.Close()
	return dir, fileName, nil
}

func writeToFsBench(dir string, prefix string, fileSize int, fileCount int, threadCount int) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if os.Mkdir(dir, 0755) != nil {
			logrus.Errorf("Failed to create directory %s", dir)
			return err
		}
	}

	var wg sync.WaitGroup
	wg.Add(threadCount)
	fileContent := utils.RandomString(fileSize)
	var writeRates = make([]float64, threadCount)
	for i := 0; i < threadCount; i++ {
		go func(idx int) {
			writeToFs(dir, fmt.Sprintf("%s_%d", prefix, idx), fileContent, idx, fileSize, fileCount, writeRates)
			wg.Done()
		}(i)
	}
	wg.Wait()
	var totalWr float64 = 0
	for _, v := range writeRates {
		totalWr += v
	}
	logrus.Infof("average write rate: %.2f KB", totalWr/float64(threadCount)/1024)
	return nil
}

func writeToFs(dir string, prefix string, fileContent string, threadIdx int, fileSize int, fileCount int, writeRates []float64) error {
	startTime := time.Now()
	for i := 0; i < fileCount; i++ {
		fileName := fmt.Sprintf("%s/%s_%d", dir, prefix, i)
		file, err := os.Create(fileName)
		if err != nil {
			logrus.Warnf("Failed to create file %s, error: %s", fileName, err.Error())
			return err
		}
		file.WriteString(fileContent)
		file.Sync()
		file.Close()
	}
	elapsed := time.Since(startTime)
	rate := float64(fileCount*fileSize) * 1000 / (0.00000001 + float64(elapsed.Milliseconds()))
	logrus.Infof("thread index: %d, file size: %d, file count per thread: %d, time cost: %s, write rate: %.2f KB/s",
		threadIdx, fileSize, fileCount, elapsed, rate/1024)
	writeRates[threadIdx] = rate
	return nil
}

func writeToS3Bench(cephClient *ceph.CephClient, prefix string, fileSize int, fileCount int, threadCount int) error {
	dir, fileName, err := genTempFile(fileSize)
	defer os.RemoveAll(dir)

	if err != nil {
		logrus.Warn("Failed to open file, error: ", err.Error())
		return err
	}
	var wg sync.WaitGroup
	wg.Add(threadCount)

	var writeRates = make([]float64, threadCount)
	for i := 0; i < threadCount; i++ {
		go func(idx int) {
			writeToS3(cephClient, fmt.Sprintf("%s_%d", prefix, idx), fileName, idx, fileSize, fileCount, writeRates)
			wg.Done()
		}(i)
	}
	wg.Wait()
	var totalWr float64 = 0
	for _, v := range writeRates {
		totalWr += v
	}
	logrus.Infof("average write rate: %.2f KB", totalWr/float64(threadCount)/1024)
	return nil
}

func writeToS3(cephClient *ceph.CephClient, prefix string, fileName string, threadIdx int, fileSize int, fileCount int, writeRates []float64) error {
	file, err := os.Open(fileName)
	defer file.Close()
	if err != nil {
		logrus.Warn("Failed to open file, error: ", err.Error())
		return err
	}
	startTime := time.Now()
	for i := 0; i < fileCount; i++ {
		file.Seek(0, 0)
		cephClient.UploadFileToS3ObjectStore(file, packageBucket, fmt.Sprintf("%s_%d", prefix, i))
	}
	elapsed := time.Since(startTime)
	rate := float64(fileCount*fileSize) * 1000 / (0.00000001 + float64(elapsed.Milliseconds()))
	logrus.Infof("thread index: %d, file size: %d, file count per thread: %d, time cost: %s, write rate: %d KB/s",
		threadIdx, fileSize, fileCount, elapsed, rate/1024)
	writeRates[threadIdx] = rate
	return nil
}

func readFromS3Bench(cephClient *ceph.CephClient, prefix string, fileSize int, fileCount int, threadCount int) error {
	var wg sync.WaitGroup
	wg.Add(threadCount)

	var readRates = make([]float64, threadCount)
	for i := 0; i < threadCount; i++ {
		go func(idx int) {
			readFromS3(cephClient, fmt.Sprintf("%s_%d", prefix, idx), idx, fileSize, fileCount, readRates)
			wg.Done()
		}(i)
	}
	wg.Wait()
	var totalWr float64 = 0
	for _, v := range readRates {
		totalWr += v
	}
	logrus.Infof("average read rate: %.2f KB", totalWr/float64(threadCount)/1024)
	return nil
}
func readFromS3(cephClient *ceph.CephClient, prefix string, threadIdx int, fileSize int, fileCount int, readRates []float64) error {
	startTime := time.Now()
	buffer := make([]byte, fileSize)
	for i := 0; i < fileCount; i++ {
		cephClient.ReadFromS3ObjectStore(packageBucket, fmt.Sprintf("%s_%d", prefix, i), buffer)
	}
	elapsed := time.Since(startTime)
	rate := float64(fileCount*fileSize) * 1000 / (0.00000001 + float64(elapsed.Milliseconds()))
	logrus.Infof("thread index: %d, file size: %d, file count per thread: %d, time cost: %s, read rate: %d KB/s",
		threadIdx, fileSize, fileCount, elapsed, rate/1024)
	readRates[threadIdx] = rate
	return nil
}

func readFromFsBench(dir string, prefix string, fileSize int, fileCount int, threadCount int) error {
	var wg sync.WaitGroup
	wg.Add(threadCount)

	var readRates = make([]float64, threadCount)
	for i := 0; i < threadCount; i++ {
		go func(idx int) {
			readFromFs(dir, fmt.Sprintf("%s_%d", prefix, idx), idx, fileSize, fileCount, readRates)
			wg.Done()
		}(i)
	}
	wg.Wait()
	var totalWr float64 = 0
	for _, v := range readRates {
		totalWr += v
	}
	logrus.Infof("average read rate: %.2f KB", totalWr/float64(threadCount)/1024)
	return nil
}
func readFromFs(dir string, prefix string, threadIdx int, fileSize int, fileCount int, readRates []float64) error {
	startTime := time.Now()
	buffer := make([]byte, fileSize)
	for i := 0; i < fileCount; i++ {
		fileName := fmt.Sprintf("%s/%s_%d", dir, prefix, i)
		file, err := os.Open(fileName)
		if err != nil {
			logrus.Errorf("Failed to open file %s, error: %s", fileName, err.Error())
			return err
		}
		n, err := file.Read(buffer)
		if err != nil {
			logrus.Errorf("Failed to read from file %s, error %s", fileName, err.Error())
			return err
		}
		if n != fileSize {
			logrus.Errorf("read file %s, get file content size: %d, fileSize: %d", fileName, n, fileSize)
		}
		file.Close()
	}
	elapsed := time.Since(startTime)
	rate := float64(fileCount*fileSize) * 1000 / (0.00000001 + float64(elapsed.Milliseconds()))
	logrus.Infof("thread index: %d, file size: %d, file count per thread: %d, time cost: %s, read rate: %d KB/s",
		threadIdx, fileSize, fileCount, elapsed, rate/1024)
	readRates[threadIdx] = rate
	return nil
}

func FsBenchmark() {
	if len(dataPathPrefix) == 0 {
		panic("dataPathPrefix should not be empty")
	}
	if storeType == "s3" {
		cephClient := ceph.NewCephClient(s3Region, s3Endpoint, s3AccessKey, s3AccessSecret)
		if operateType == "write" {
			writeToS3Bench(cephClient, dataPathPrefix, fileSizeKB*1024, fileCount, threadCount)
		} else {
			readFromS3Bench(cephClient, dataPathPrefix, fileSizeKB*1024, fileCount, threadCount)
		}
	} else if storeType == "fs" {
		if operateType == "write" {
			writeToFsBench(fsDir, dataPathPrefix, fileSizeKB*1024, fileCount, threadCount)
		} else if operateType == "read" {
			readFromFsBench(fsDir, dataPathPrefix, fileSizeKB*1024, fileCount, threadCount)
		}
	}
}
