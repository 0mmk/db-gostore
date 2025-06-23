package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"lsm" // Assuming the refactored lsm package is in this path
	"os"
	"runtime/pprof"
	"strconv"
	"time"
)

// generateRandomValue creates a random byte slice of a given size and returns it as a hex-encoded string.
func generateRandomValue(size int) (string, error) {
	bytes := make([]byte, size)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

var randomValue, _ = generateRandomValue(256)

// readWrite performs a benchmark for a given number of entries.
// It cleans the data directory, writes the entries, closes and re-opens the DB,
// then reads and verifies all entries.
func readWrite(dbDir string, numEntries int) {
	fmt.Printf("--- Testing with %d entries ---\n", numEntries)

	// Clean up previous data for a fresh run
	if err := os.RemoveAll("data"); err != nil {
		log.Fatalf("Failed to clean data directory: %v", err)
	}

	// --- WRITE PHASE ---
	db, err := lsm.Open(dbDir, nil) // Use default config
	if err != nil {
		log.Fatalf("Failed to open DB for writing: %v", err)
	}

	expectedValues := make(map[string]string, numEntries)
	start := time.Now()

	for i := 0; i < numEntries; i++ {
		key := "key" + strconv.Itoa(i)
		value := randomValue
		expectedValues[key] = value

		if err := db.Put(key, value); err != nil {
			log.Fatalf("Put failed for key %s: %v", key, err)
		}
	}

	// Close the DB to ensure all data is flushed from memtable to SSTables
	if err := db.Close(); err != nil {
		log.Fatalf("Failed to close DB after writing: %v", err)
	}
	fmt.Printf("%d entries written in %s\n", numEntries, time.Since(start))

	// --- READ PHASE ---
	// Re-open the DB to simulate reading from a persistent state
	db, err = lsm.Open(dbDir, nil)
	if err != nil {
		log.Fatalf("Failed to open DB for reading: %v", err)
	}
	defer db.Close() // Ensure DB is closed after reading

	start = time.Now()
	success := 0
	for i := 0; i < numEntries; i++ {
		key := "key" + strconv.Itoa(i)
		val, err := db.Get(key)
		if err == nil {
			if expected, ok := expectedValues[key]; ok && val == expected {
				success++
			} else {
				fmt.Printf("Mismatch for key %s\n", key)
			}
		} else {
			fmt.Printf("Error getting key %s: %v\n", key, err)
		}
	}
	falsePositives := db.GetBloomFalsePositives()
	fmt.Printf("%d entries read in %s, %d/%d correct, %d false positives\n\n", numEntries, time.Since(start), success, numEntries, falsePositives)
}

func main() {
	// Set up CPU profiling
	f, err := os.Create("cpu.prof")
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	defer f.Close()
	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}
	defer pprof.StopCPUProfile()

	// Benchmark with increasing numbers of entries (powers of 2, up to 2^18)
	for i := 0; i <= 20; i++ {
		numEntries := 1 << i
		readWrite("data", numEntries)
	}
	// readWrite("data", 1<<20)
}
