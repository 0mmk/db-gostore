package lsm

import (
	"bufio"
	"bytes"
	"cache"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// IndexEntry represents a key's location within a data file.
type IndexEntry struct {
	Key    string
	Offset uint64
}

// SSTable holds the metadata for a single Sorted String Table on disk.
type SSTable struct {
	ChunkID       string
	Index         []IndexEntry // nil for old tables that are not fully loaded
	Bloom         *BloomFilter // Always present
	HashTable     []byte       // loaded hash table (nil for old tables)
	HashTableSize int          // number of slots in hash table
}

// Config holds the configuration options for a DB instance.
type Config struct {
	MaxMemTableSize  int // Max number of entries in a memtable before it's rotated.
	MaxOpenFiles     int // Max number of open file handles in the LRU cache.
	MaxCacheSize     int // Max number of key-value pairs in the LRU cache.
	CacheLoadSize    int // Number of items to prefetch into cache on a disk read.
	MaxLoadedIndexes int // Number of SSTable indexes to keep fully loaded in memory.
	BloomBitsPerKey  int // Bits per key for Bloom filters (e.g., 10 is a good default).
	IndexLoadSize    int // Number of indexes to load in a batch from disk
}

// MemTable is an in-memory table of key-value pairs.
type MemTable struct {
	entries  map[string]string
	keyOrder []string
}

func NewMemTable() *MemTable {
	return &MemTable{
		entries:  make(map[string]string),
		keyOrder: make([]string, 0),
	}
}

// --- Optimized Bloom Filter Implementation ---
type BloomFilter struct {
	bits   []byte
	hashes int
}

func NewBloomFilter(numItems int, bitsPerKey int) *BloomFilter {
	size := numItems * bitsPerKey
	if size < 64 {
		size = 64
	}
	// k = m/n * ln(2)
	hashes := max(1, int(float64(size)/float64(numItems)*0.7))
	return &BloomFilter{
		bits:   make([]byte, (size+7)/8),
		hashes: hashes,
	}
}
func (bf *BloomFilter) Add(data []byte) {
	h1 := fnv.New64a()
	h1.Write(data)
	hash1 := h1.Sum64()

	h2 := fnv.New64()
	h2.Write(data)
	hash2 := h2.Sum64()

	for i := 0; i < bf.hashes; i++ {
		idx := (hash1 + uint64(i)*hash2) % (uint64(len(bf.bits)) * 8)
		bf.bits[idx/8] |= 1 << (idx % 8)
	}
}
func (bf *BloomFilter) Test(data []byte) bool {
	h1 := fnv.New64a()
	h1.Write(data)
	hash1 := h1.Sum64()

	h2 := fnv.New64()
	h2.Write(data)
	hash2 := h2.Sum64()

	for i := 0; i < bf.hashes; i++ {
		idx := (hash1 + uint64(i)*hash2) % (uint64(len(bf.bits)) * 8)
		if (bf.bits[idx/8] & (1 << (idx % 8))) == 0 {
			return false
		}
	}
	return true
}
func (bf *BloomFilter) WriteTo(w io.Writer) error {
	// Write header: version, number of hashes, size of bit array
	header := []uint32{1, uint32(bf.hashes), uint32(len(bf.bits))}
	if err := binary.Write(w, binary.LittleEndian, header); err != nil {
		return err
	}
	_, err := w.Write(bf.bits)
	return err
}
func ReadBloomFilterFrom(r io.Reader) (*BloomFilter, error) {
	header := make([]uint32, 3)
	if err := binary.Read(r, binary.LittleEndian, &header); err != nil {
		return nil, err
	}
	if header[0] != 1 {
		return nil, fmt.Errorf("unsupported bloom filter version: %d", header[0])
	}
	bf := &BloomFilter{
		hashes: int(header[1]),
		bits:   make([]byte, header[2]),
	}
	_, err := io.ReadFull(r, bf.bits)
	return bf, err
}

// DB is the main structure for the LSM-Tree.
type DB struct {
	mu         sync.RWMutex
	dataDir    string
	indexDir   string
	memTable   *MemTable
	sstables   []SSTable
	chunkCount int
	config     Config
	flushWg    sync.WaitGroup
	openFiles  *cache.LRUCache // Using cache from package
	lruCache   *cache.LRUCache // Using cache from package

	bloomFalsePositives     int64                   // Counter for Bloom filter false positives
	batchLoadedIndexes      map[string][]IndexEntry // map from chunkID to loaded index (for batch)
	batchStartIdx           int                     // the starting index of the current batch
	binarySearchMisses      int64                   // Counter for binary search fallback after hash miss
	hashIndexFalsePositives int64
}

// Open initializes a database instance.
func Open(baseDir string, config *Config) (*DB, error) {
	dataDir := filepath.Join(baseDir, "data")
	indexDir := filepath.Join(baseDir, "index")
	for _, dir := range []string{baseDir, dataDir, indexDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	if config == nil {
		config = &Config{
			MaxMemTableSize:  8192,
			MaxOpenFiles:     256,
			MaxCacheSize:     4096,
			CacheLoadSize:    512,
			MaxLoadedIndexes: 64,
			BloomBitsPerKey:  32,
			IndexLoadSize:    16,
		}
	}

	db := &DB{
		dataDir:            dataDir,
		indexDir:           indexDir,
		memTable:           NewMemTable(),
		sstables:           []SSTable{},
		config:             *config,
		openFiles:          cache.NewLRUCache(config.MaxOpenFiles), // Using cache from package
		lruCache:           cache.NewLRUCache(config.MaxCacheSize), // Using cache from package
		batchLoadedIndexes: make(map[string][]IndexEntry),
		batchStartIdx:      -1,
	}

	indexFiles, err := os.ReadDir(db.indexDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read index directory: %w", err)
	}

	var allChunkIDs []string
	for _, f := range indexFiles {
		if strings.HasSuffix(f.Name(), ".index.txt") {
			allChunkIDs = append(allChunkIDs, strings.TrimSuffix(f.Name(), ".index.txt"))
		}
	}
	sort.Strings(allChunkIDs)

	for i, chunkID := range allChunkIDs {
		// Load the bloom filter from its dedicated file. This is fast.
		bloomPath := filepath.Join(db.indexDir, chunkID+".bloom")
		bfFile, err := os.Open(bloomPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: could not open bloom filter for %s: %v\n", chunkID, err)
			continue
		}
		bloom, err := ReadBloomFilterFrom(bfFile)
		bfFile.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: could not read bloom filter for %s: %v\n", chunkID, err)
			continue
		}

		var index []IndexEntry
		// Only load the full index for the N most recent tables.
		if i >= len(allChunkIDs)-db.config.MaxLoadedIndexes {
			index, err = db.loadIndexFromDisk(chunkID)
			if err != nil {
				fmt.Fprintf(os.Stderr, "warning: could not load index for %s: %v\n", chunkID, err)
			}
		}
		db.sstables = append(db.sstables, SSTable{ChunkID: chunkID, Index: index, Bloom: bloom})
	}

	if len(db.sstables) > 0 {
		lastChunkID := db.sstables[len(db.sstables)-1].ChunkID
		countStr := strings.TrimPrefix(lastChunkID, "sst-")
		lastCount, _ := strconv.Atoi(countStr)
		db.chunkCount = lastCount + 1
	}

	return db, nil
}

func (db *DB) Put(key, value string) error {
	db.mu.Lock()
	if _, exists := db.memTable.entries[key]; !exists {
		db.memTable.keyOrder = append(db.memTable.keyOrder, key)
	}
	db.memTable.entries[key] = value
	db.lruCache.Put(key, value)

	shouldFlush := len(db.memTable.entries) >= db.config.MaxMemTableSize
	var toFlush *MemTable
	if shouldFlush {
		toFlush = db.memTable
		db.memTable = NewMemTable()
	}
	db.mu.Unlock()

	if shouldFlush {
		if err := db.flush(toFlush); err != nil {
			fmt.Fprintf(os.Stderr, "error during flush: %v\n", err)
		}
	}
	return nil
}

func (db *DB) flush(memTable *MemTable) error {
	// All work that does not require locking
	sort.Strings(memTable.keyOrder)

	chunkID := fmt.Sprintf("sst-%010d", db.chunkCount)
	db.mu.Lock()
	db.chunkCount++
	db.mu.Unlock()

	dataPath := filepath.Join(db.dataDir, chunkID+".data.txt")
	indexPath := filepath.Join(db.indexDir, chunkID+".index.txt")
	bloomPath := filepath.Join(db.indexDir, chunkID+".bloom")

	dataFile, err := os.Create(dataPath)
	if err != nil {
		return err
	}
	defer dataFile.Close()
	dataWriter := bufio.NewWriter(dataFile)

	indexFile, err := os.Create(indexPath)
	if err != nil {
		return err
	}
	defer indexFile.Close()
	indexWriter := bufio.NewWriter(indexFile)

	bloom := NewBloomFilter(len(memTable.keyOrder), db.config.BloomBitsPerKey)
	var offset uint64
	var newIndex []IndexEntry
	keyHashes := make([]uint64, 0, len(memTable.keyOrder))
	for _, key := range memTable.keyOrder {
		value := memTable.entries[key]
		line := fmt.Sprintf("%s\t%s\n", key, value)
		n, err := dataWriter.WriteString(line)
		if err != nil {
			return err
		}
		offsetBase64 := base64.StdEncoding.EncodeToString(binary.BigEndian.AppendUint64(nil, offset))
		_, err = fmt.Fprintf(indexWriter, "%s\t%s\n", key, offsetBase64)
		if err != nil {
			return err
		}
		newIndex = append(newIndex, IndexEntry{Key: key, Offset: offset})
		bloom.Add([]byte(key))
		offset += uint64(n)
		keyHash := fnvHash64([]byte(key))
		keyHashes = append(keyHashes, keyHash)
	}

	if err := dataWriter.Flush(); err != nil {
		return err
	}
	if err := indexWriter.Flush(); err != nil {
		return err
	}

	bloomFile, err := os.Create(bloomPath)
	if err != nil {
		return err
	}
	defer bloomFile.Close()
	if err := bloom.WriteTo(bloomFile); err != nil {
		return err
	}

	// Build and write hash table
	tableSize := nextPowerOfTwo(len(memTable.keyOrder) * 2)
	hashTable := make([]byte, tableSize*16)
	for i := range memTable.keyOrder {
		hash := keyHashes[i]
		offset := newIndex[i].Offset
		for j := 0; j < tableSize; j++ {
			slot := (int(hash) + j) & (tableSize - 1)
			pos := slot * 16
			existing := binary.BigEndian.Uint64(hashTable[pos : pos+8])
			if existing == 0 {
				binary.BigEndian.PutUint64(hashTable[pos:pos+8], hash)
				binary.BigEndian.PutUint64(hashTable[pos+8:pos+16], offset)
				break
			}
		}
	}
	hashPath := filepath.Join(db.indexDir, chunkID+".index.hash.txt")
	hashFile, err := os.Create(hashPath)
	if err != nil {
		return err
	}
	_, err = hashFile.Write(hashTable)
	hashFile.Close()
	if err != nil {
		return err
	}

	// Only lock for critical section
	var finalIndex []IndexEntry
	var finalHashTable []byte
	var finalHashTableSize int
	if len(db.sstables) < db.config.MaxLoadedIndexes {
		finalIndex = newIndex
		finalHashTable = hashTable
		finalHashTableSize = tableSize
	}
	db.mu.Lock()
	db.sstables = append(db.sstables, SSTable{ChunkID: chunkID, Index: finalIndex, Bloom: bloom, HashTable: finalHashTable, HashTableSize: finalHashTableSize})
	sort.Slice(db.sstables, func(i, j int) bool {
		return db.sstables[i].ChunkID < db.sstables[j].ChunkID
	})
	db.mu.Unlock()
	return nil
}

func (db *DB) Get(key string) (string, error) {
	if val, ok := db.lruCache.Get(key); ok {
		return val.(string), nil
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	keyBytes := []byte(key)

	for i := len(db.sstables) - 1; i >= 0; i-- {
		sst := db.sstables[i]
		if !sst.Bloom.Test(keyBytes) {
			continue // Definitely not in this table.
		}
		var index []IndexEntry
		var hashTable []byte
		var hashTableSize int
		var err error
		if sst.Index != nil {
			index = sst.Index
			hashTable = sst.HashTable
			hashTableSize = sst.HashTableSize
		} else {
			if idxEntries, ok := db.batchLoadedIndexes[sst.ChunkID]; ok {
				index = idxEntries
				if sst.HashTable != nil {
					hashTable = sst.HashTable
					hashTableSize = sst.HashTableSize
				} else {
					hashPath := filepath.Join(db.indexDir, sst.ChunkID+".index.hash.txt")
					hashFile, err := os.Open(hashPath)
					if err == nil {
						hashTable, _ = io.ReadAll(hashFile)
						hashFile.Close()
						hashTableSize = len(hashTable) / 16
					}
				}
			} else {
				batchSize := db.config.IndexLoadSize
				if batchSize <= 0 {
					batchSize = 4
				}
				pos := -1
				for j := range db.sstables {
					if db.sstables[j].ChunkID == sst.ChunkID {
						pos = j
						break
					}
				}
				if pos == -1 {
					continue
				}
				db.batchLoadedIndexes = make(map[string][]IndexEntry)
				db.batchStartIdx = pos
				index = db.batchLoadedIndexes[sst.ChunkID]
			}
		}

		// Try hash table lookup
		if hashTable != nil && hashTableSize > 0 {
			hash := fnvHash64(keyBytes)
			found := false
			var offset uint64
			for j := 0; j < hashTableSize; j++ {
				slot := (int(hash) + j) & (hashTableSize - 1)
				pos := slot * 16
				h := binary.BigEndian.Uint64(hashTable[pos : pos+8])
				if h == 0 {
					break // not found
				}
				if h == hash {
					offset = binary.BigEndian.Uint64(hashTable[pos+8 : pos+16])
					found = true
					break
				}
			}
			if found {
				file, err := db.getFileHandle(sst.ChunkID)
				if err != nil {
					continue
				}
				_, err = file.Seek(int64(offset), 0)
				if err != nil {
					continue
				}
				line, err := bufio.NewReader(file).ReadString('\n')
				if err != nil && err != io.EOF {
					continue
				}
				parts := strings.SplitN(strings.TrimSpace(line), "\t", 2)
				if len(parts) == 2 && parts[0] == key {
					db.lruCache.Put(key, parts[1])
					return parts[1], nil
				}
				// else: hash collision, fall through to binary search
				db.hashIndexFalsePositives++
				fmt.Fprintf(os.Stderr, "[DEBUG] Hash lookup for key '%s' in chunk '%s' failed: offset=%d, line='%s', parts=%v\n", key, sst.ChunkID, offset, line, parts)
			}
		}
		// Fallback: binary search
		idx := binarySearch(index, key)
		if idx == -1 {
			// Bloom filter gave a false positive
			db.bloomFalsePositives++
			// now continue with the next sstable
			continue
		}
		if hashTable != nil && hashTableSize > 0 {
			db.binarySearchMisses++
		}
		file, err := db.getFileHandle(sst.ChunkID)
		if err != nil {
			return "", err
		}
		_, err = file.Seek(int64(index[idx].Offset), 0)
		if err != nil {
			return "", err
		}
		line, err := bufio.NewReader(file).ReadString('\n')
		if err != nil && err != io.EOF {
			return "", err
		}
		parts := strings.SplitN(strings.TrimSpace(line), "\t", 2)
		if len(parts) == 2 && parts[0] == key {
			db.lruCache.Put(key, parts[1])
			return parts[1], nil
		}
	}

	return "", fmt.Errorf("key not found: %s", key)
}

func (db *DB) loadIndexFromDisk(chunkID string) ([]IndexEntry, error) {
	indexPath := filepath.Join(db.indexDir, chunkID+".index.txt")
	f, err := os.Open(indexPath)
	if err != nil {
		return nil, err
	}

	// Read the entire file into memory to reduce I/O syscalls.
	fileBytes, err := io.ReadAll(f)
	f.Close()
	if err != nil {
		return nil, err
	}

	// Split the byte slice by newlines. This is more efficient than converting to a string first.
	lines := bytes.Split(fileBytes, []byte("\n"))
	index := make([]IndexEntry, 0, len(lines)) // Pre-allocate slice capacity

	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		parts := bytes.SplitN(line, []byte("\t"), 2)
		if len(parts) == 2 {
			key := string(parts[0])
			offset, err := base64.StdEncoding.DecodeString(string(parts[1]))
			offsetUInt64 := binary.BigEndian.Uint64(offset)
			if err != nil {
				// Malformed line, skip it.
				continue
			}
			index = append(index, IndexEntry{Key: key, Offset: offsetUInt64})
		}
	}

	return index, nil
}

func (db *DB) loadIndexFromDiskArena(chunkID string, _ interface{}) ([]IndexEntry, []byte, int, error) {
	indexPath := filepath.Join(db.indexDir, chunkID+".index.txt")
	hashPath := filepath.Join(db.indexDir, chunkID+".index.hash.txt")
	f, err := os.Open(indexPath)
	if err != nil {
		return nil, nil, 0, err
	}
	fileBytes, err := io.ReadAll(f)
	f.Close()
	if err != nil {
		return nil, nil, 0, err
	}
	lines := bytes.Split(fileBytes, []byte("\n"))
	index := make([]IndexEntry, 0, len(lines))
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		parts := bytes.SplitN(line, []byte("\t"), 2)
		if len(parts) == 2 {
			key := string(parts[0])
			offset, err := base64.StdEncoding.DecodeString(string(parts[1]))
			if err != nil {
				continue
			}
			offsetUInt64 := binary.BigEndian.Uint64(offset)
			index = append(index, IndexEntry{Key: key, Offset: offsetUInt64})
		}
	}
	// Load hash table
	hashFile, err := os.Open(hashPath)
	if err != nil {
		return index, nil, 0, nil // fallback: no hash table
	}
	hashTable, err := io.ReadAll(hashFile)
	hashFile.Close()
	if err != nil {
		return index, nil, 0, nil
	}
	tableSize := len(hashTable) / 16
	return index, hashTable, tableSize, nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	memTable := db.memTable
	db.memTable = nil
	db.mu.Unlock()
	if memTable != nil && len(memTable.entries) > 0 {
		if err := db.flush(memTable); err != nil {
			fmt.Fprintf(os.Stderr, "error during final flush: %v\n", err)
		}
	}
	db.openFiles.Clear()
	db.lruCache.Clear()
	db.batchLoadedIndexes = nil
	return nil
}

func binarySearch(index []IndexEntry, key string) int {
	low, high := 0, len(index)-1
	for low <= high {
		mid := low + (high-low)/2
		if index[mid].Key == key {
			return mid
		}
		if index[mid].Key < key {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return -1
}

func (db *DB) getFileHandle(chunkID string) (*os.File, error) {
	if val, ok := db.openFiles.Get(chunkID); ok {
		file := val.(*os.File)
		_, err := file.Seek(0, io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("failed to seek file from cache: %w", err)
		}
		return file, nil
	}
	path := filepath.Join(db.dataDir, chunkID+".data.txt")
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	db.openFiles.Put(chunkID, f)
	return f, nil
}

// max returns the larger of x or y.
func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

// GetBloomFalsePositives returns the number of Bloom filter false positives (misses)
func (db *DB) GetBloomFalsePositives() int64 {
	return db.bloomFalsePositives
}

// GetBinarySearchMisses returns the number of binary search misses
func (db *DB) GetBinarySearchMisses() int64 {
	return db.binarySearchMisses
}

// GetHashIndexFalsePositives returns the number of hash index false positives
func (db *DB) GetHashIndexFalsePositives() int64 {
	return db.hashIndexFalsePositives
}

// Hash function
func fnvHash64(data []byte) uint64 {
	h := fnv.New64a()
	h.Write(data)
	return h.Sum64()
}

// Utility: next power of two
func nextPowerOfTwo(x int) int {
	if x <= 0 {
		return 1
	}
	x--
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	if ^uint(0)>>32 != 0 {
		x |= x >> 32
	}
	return x + 1
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
