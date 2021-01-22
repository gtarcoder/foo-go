package main

import (
	"foo-go/internal/tools"
)

func main() {
	// ./fs-go  -fileCount=2000  -storeType=fs -fsDir=/mnt/juicefs-rados -dataPathPrefix=juiefs-test-4thread-4KB -operateType=write -threadCount=4 -fileSizeKB=4
	tools.FsBenchmark()
}
