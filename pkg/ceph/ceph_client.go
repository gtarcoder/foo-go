package ceph

import (
	"io"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

type CephClient struct {
	S3Client   *s3.S3
	S3Endpoint string
}

func NewCephClient(region string, S3Endpoint string, accessKey string, accessSecret string) *CephClient {
	s3client := newS3Client(region, S3Endpoint, accessKey, accessSecret)

	manager := CephClient{S3Client: s3client, S3Endpoint: S3Endpoint}
	return &manager
}

func newS3Client(region string, endpoint string, accessKey string, accessSecret string) *s3.S3 {
	config := &aws.Config{
		Region:                        aws.String(region),
		Endpoint:                      aws.String(endpoint),
		CredentialsChainVerboseErrors: aws.Bool(true),
		DisableSSL:                    aws.Bool(true),
		S3ForcePathStyle:              aws.Bool(true),
		Credentials:                   credentials.NewStaticCredentials(accessKey, accessSecret, ""),
		S3Disable100Continue:          aws.Bool(true),
	}
	sess := session.Must(session.NewSession(config))
	return s3.New(sess)
}

func (manager *CephClient) UploadFileToS3ObjectStore(file *os.File, bucket string, objectName string) error {
	_, err := manager.S3Client.PutObject(
		(&s3.PutObjectInput{}).SetBucket(bucket).
			SetKey(objectName).
			SetBody(file).
			SetWebsiteRedirectLocation(""))
	if err != nil {
		log.Warn("Failed to put object, error: ", err.Error())
		return err
	}
	return nil
}

func (manager *CephClient) DownloadFileFromS3ObjectStore(bucket string, objectName string, path string) error {
	input := s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectName),
	}

	output, err := manager.S3Client.GetObject(&input)
	if err != nil {
		log.Warn("Failed to get s3 object, error: ", err.Error())
		return err
	}
	outFile, err := os.Create(path)
	if err != nil {
		log.Warn("Failed to create file: ", path, ", error: ", err.Error())
		return err
	}
	defer outFile.Close()
	_, err = io.Copy(outFile, output.Body)
	if err != nil {
		log.Warn("Failed to stream s3 getobject output to file, error: ", err.Error())
		return err
	}
	return nil
}

func (manager *CephClient) ReadFromS3ObjectStore(bucket string, objectName string, buffer []byte) error {
	input := s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectName),
	}

	output, err := manager.S3Client.GetObject(&input)
	if err != nil {
		log.Warn("Failed to get s3 object, error: ", err.Error())
		return err
	}
	totalBytes := 0
	var n = 0
	var readErr error = nil
	for readErr != io.EOF {
		n, readErr = output.Body.Read(buffer)
		totalBytes += n
	}
	if totalBytes != len(buffer) {
		logrus.Errorf("Failed to read object from s3, buf size: %d, total read bytes: %d", len(buffer), totalBytes)
	}
	return nil
}
