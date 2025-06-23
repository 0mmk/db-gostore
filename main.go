package main

import (
	"fmt"
	"lsm"
	"os"
	"runtime/pprof"
	"strconv"
	"time"
)

const randomKey = "09j290jf293jf290jfskljdfapioefiwjef2039fj2iefjs0djf239jfsdkfj2093jf209jfdosijf0923jf209ejf0sd9jf0923jf90sdjf0w9jf2390fj"

func readWrite(numEntries int) {
	// remove everything under data/
	os.RemoveAll("data")
	lsm.Init()
	start := time.Now()
	for i := range numEntries {
		lsm.Put(fmt.Sprintf("key%d", i), randomKey+strconv.Itoa(i))
	}
	lsm.Flush() // ensure leftovers are flushed
	fmt.Printf("%d entries written in %s\n", numEntries, time.Since(start))

	start = time.Now()
	success := 0
	for i := range numEntries {
		val, err := lsm.Get("key" + strconv.Itoa(i))
		if err == nil && val == randomKey+strconv.Itoa(i) {
			success++
		}
	}
	fmt.Printf("%d entries read in %s, %d/%d correct\n", numEntries, time.Since(start), success, numEntries)
}

func main() {
	f, _ := os.Create("cpu.prof")
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	// power of 2, up to 2^20
	for i := 0; i < 20; i++ {
		numEntries := 1 << i
		readWrite(numEntries)
	}
}
