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

	"github.com/ortuman/nuke"
)

// IndexEntry represents a key's location within a data file.
type IndexEntry struct {
	Key    string
	Offset uint64
}

// SSTable holds the metadata for a single Sorted String Table on disk.
type SSTable struct {
	ChunkID string
	Index   []IndexEntry // nil for old tables that are not fully loaded
	Bloom   *BloomFilter // Always present
}

// Config holds the configuration options for a DB instance.
type Config struct {
	MemtableCount    int // Number of memtables to keep in memory (1 active, N-1 immutable).
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
	arena    nuke.Arena
	entries  map[string]string
	keyOrder []string
}

func NewMemTable() *MemTable {
	a := nuke.NewMonotonicArena(256*1024, 80) // 256KB buffer, 80MB max
	return &MemTable{
		arena:    a,
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
	memTables  []*MemTable
	sstables   []SSTable
	chunkCount int
	config     Config
	flushWg    sync.WaitGroup
	openFiles  *cache.LRUCache // Using cache from package
	lruCache   *cache.LRUCache // Using cache from package

	bloomFalsePositives int64                   // Counter for Bloom filter false positives
	batchLoadedIndexes  map[string][]IndexEntry // map from chunkID to loaded index (for batch)
	batchArena          nuke.Arena              // arena for current batch
	batchStartIdx       int                     // the starting index of the current batch
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
			MemtableCount:    1,
			MaxMemTableSize:  32384,
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
		memTables:          []*MemTable{NewMemTable()},
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
	defer db.mu.Unlock()

	activeTable := db.memTables[0]
	if _, exists := activeTable.entries[key]; !exists {
		activeTable.keyOrder = append(activeTable.keyOrder, key)
	}
	activeTable.entries[key] = value
	db.lruCache.Put(key, value)

	if len(activeTable.entries) >= db.config.MaxMemTableSize {
		newActiveTable := NewMemTable()
		db.memTables = append([]*MemTable{newActiveTable}, db.memTables...)

		if len(db.memTables) > db.config.MemtableCount {
			oldestTableIndex := len(db.memTables) - 1
			tableToFlush := db.memTables[oldestTableIndex]
			db.memTables = db.memTables[:oldestTableIndex]

			db.flushWg.Add(1)
			go func(mt *MemTable) {
				defer db.flushWg.Done()
				if err := db.flush(mt); err != nil {
					fmt.Fprintf(os.Stderr, "error during background flush: %v\n", err)
				}
			}(tableToFlush)
		}
	}
	return nil
}

func (db *DB) flush(memTable *MemTable) error {
	db.mu.Lock()
	chunkID := fmt.Sprintf("sst-%010d", db.chunkCount)
	db.chunkCount++
	db.mu.Unlock()

	sort.Strings(memTable.keyOrder)

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
	for _, key := range memTable.keyOrder {
		// Allocate key/value in arena
		keyArena := nuke.New[string](memTable.arena)
		*keyArena = key
		valueArena := nuke.New[string](memTable.arena)
		*valueArena = memTable.entries[key]
		line := fmt.Sprintf("%s\t%s\n", *keyArena, *valueArena)
		n, err := dataWriter.WriteString(line)
		if err != nil {
			return err
		}
		// no int conversion, just encode the offset directly as binary to base64
		offsetBase64 := base64.StdEncoding.EncodeToString(binary.BigEndian.AppendUint64(nil, offset))
		_, err = fmt.Fprintf(indexWriter, "%s\t%s\n", *keyArena, offsetBase64)
		if err != nil {
			return err
		}
		newIndex = append(newIndex, IndexEntry{Key: *keyArena, Offset: offset})
		bloom.Add([]byte(*keyArena))
		offset += uint64(n)
	}

	if err := dataWriter.Flush(); err != nil {
		return err
	}
	if err := indexWriter.Flush(); err != nil {
		return err
	}

	// Write bloom filter to disk
	bloomFile, err := os.Create(bloomPath)
	if err != nil {
		return err
	}
	defer bloomFile.Close()
	if err := bloom.WriteTo(bloomFile); err != nil {
		return err
	}

	db.mu.Lock()
	// Decide if the new index should be loaded into memory or not
	var finalIndex []IndexEntry
	if len(db.sstables) < db.config.MaxLoadedIndexes {
		finalIndex = newIndex
	}
	db.sstables = append(db.sstables, SSTable{ChunkID: chunkID, Index: finalIndex, Bloom: bloom})
	sort.Slice(db.sstables, func(i, j int) bool {
		return db.sstables[i].ChunkID < db.sstables[j].ChunkID
	})
	// Free the arena for this memtable
	memTable.arena.Reset(true)
	memTable.arena = nil
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

	for _, memTable := range db.memTables {
		if val, ok := memTable.entries[key]; ok {
			db.lruCache.Put(key, val)
			return val, nil
		}
	}

	for i := len(db.sstables) - 1; i >= 0; i-- {
		sst := db.sstables[i]
		if !sst.Bloom.Test(keyBytes) {
			continue // Definitely not in this table.
		}

		var index []IndexEntry
		var err error
		if sst.Index != nil {
			index = sst.Index // Use in-memory index
		} else {
			// Batch index loading logic with arena
			if idxEntries, ok := db.batchLoadedIndexes[sst.ChunkID]; ok {
				index = idxEntries
			} else {
				// Need to load a new batch
				batchSize := db.config.IndexLoadSize
				if batchSize <= 0 {
					batchSize = 4
				}
				// Find the position of this chunkID in db.sstables
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
				// Evict previous batch and free its arena
				if db.batchArena != nil {
					db.batchArena.Reset(true)
					db.batchArena = nil
				}
				db.batchLoadedIndexes = make(map[string][]IndexEntry)
				db.batchStartIdx = pos
				// Create a new arena for this batch
				batchArena := nuke.NewMonotonicArena(256*1024, 80)
				for j := pos; j < pos+batchSize && j < len(db.sstables); j++ {
					cid := db.sstables[j].ChunkID
					idx, err := db.loadIndexFromDiskArena(cid, batchArena)
					if err == nil {
						db.batchLoadedIndexes[cid] = idx
					}
				}
				db.batchArena = batchArena
				index = db.batchLoadedIndexes[sst.ChunkID]
			}
		}

		idx := binarySearch(index, key)
		if idx == -1 {
			// Bloom filter gave a false positive
			db.bloomFalsePositives++
			continue
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

func (db *DB) loadIndexFromDiskArena(chunkID string, a nuke.Arena) ([]IndexEntry, error) {
	indexPath := filepath.Join(db.indexDir, chunkID+".index.txt")
	f, err := os.Open(indexPath)
	if err != nil {
		return nil, err
	}
	fileBytes, err := io.ReadAll(f)
	f.Close()
	if err != nil {
		return nil, err
	}
	lines := bytes.Split(fileBytes, []byte("\n"))
	index := nuke.MakeSlice[IndexEntry](a, 0, len(lines))
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		parts := bytes.SplitN(line, []byte("\t"), 2)
		if len(parts) == 2 {
			keyPtr := nuke.New[string](a)
			*keyPtr = string(parts[0])
			offset, err := base64.StdEncoding.DecodeString(string(parts[1]))
			if err != nil {
				continue
			}
			offsetUInt64 := binary.BigEndian.Uint64(offset)
			entry := nuke.New[IndexEntry](a)
			entry.Key = *keyPtr
			entry.Offset = offsetUInt64
			index = nuke.SliceAppend(a, index, *entry)
		}
	}
	return index, nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	for _, memTable := range db.memTables {
		if len(memTable.entries) > 0 {
			db.flushWg.Add(1)
			go func(mt *MemTable) {
				defer db.flushWg.Done()
				db.flush(mt)
			}(memTable)
		}
	}
	db.memTables = nil
	db.mu.Unlock()
	db.flushWg.Wait()
	db.openFiles.Clear()
	db.lruCache.Clear()
	db.batchLoadedIndexes = nil
	if db.batchArena != nil {
		db.batchArena.Reset(true)
		db.batchArena = nil
	}
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
