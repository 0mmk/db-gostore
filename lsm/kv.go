package lsm

import (
	"bufio"
	"container/list"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type SSTable struct {
	ChunkID string
	Index   []IndexEntry
}

type IndexEntry struct {
	Key    string
	Offset int64
}

var (
	memTable   = make(map[string]string)
	keyOrder   []string
	sstables   []SSTable
	chunkCount = 0
	maxChunk   = 4096
	dataDir    = "data"

	openFiles = make(map[string]*os.File)
	openList  = list.New() // LRU for open files
	maxOpen   = 256

	cache       = make(map[string]string)
	cacheList   = list.New() // LRU for key-value cache
	maxCacheLen = 1024
)

func Init() error {
	err := os.MkdirAll(dataDir, 0755)
	if err != nil {
		return err
	}
	files, err := os.ReadDir(dataDir)
	if err != nil {
		return err
	}
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".index.txt") {
			chunk := strings.TrimSuffix(f.Name(), ".index.txt")
			loadIndex(chunk)
		}
	}
	return nil
}

func loadIndex(chunk string) {
	f, err := os.Open(filepath.Join(dataDir, chunk+".index.txt"))
	if err != nil {
		return
	}
	defer f.Close()

	var index []IndexEntry
	s := bufio.NewScanner(f)
	for s.Scan() {
		parts := strings.Split(s.Text(), "\t")
		if len(parts) == 2 {
			offset, _ := strconv.ParseInt(parts[1], 10, 64)
			index = append(index, IndexEntry{Key: parts[0], Offset: offset})
		}
	}
	sstables = append(sstables, SSTable{ChunkID: chunk, Index: index})
}

func Put(key, value string) error {
	if _, exists := memTable[key]; !exists {
		keyOrder = append(keyOrder, key)
	}
	memTable[key] = value
	cachePut(key, value) // update cache immediately on put
	if len(memTable) >= maxChunk {
		return Flush()
	}
	return nil
}

func Flush() error {
	sort.Strings(keyOrder)
	chunkID := fmt.Sprintf("sst-%010d", chunkCount)
	chunkCount++

	df, err := os.Create(filepath.Join(dataDir, chunkID+".data.txt"))
	if err != nil {
		return err
	}
	defer df.Close()

	ix, err := os.Create(filepath.Join(dataDir, chunkID+".index.txt"))
	if err != nil {
		return err
	}
	defer ix.Close()

	var offset int64
	var index []IndexEntry

	for _, key := range keyOrder {
		val := memTable[key]
		line := fmt.Sprintf("%s\t%s\n", key, val)
		_, err = df.WriteString(line)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(ix, "%s\t%d\n", key, offset)
		if err != nil {
			return err
		}
		index = append(index, IndexEntry{Key: key, Offset: offset})
		offset += int64(len(line))
	}
	memTable = make(map[string]string)
	keyOrder = nil
	sstables = append(sstables, SSTable{ChunkID: chunkID, Index: index})
	return nil
}

func Get(key string) (string, error) {
	// Check cache first
	if val, ok := cache[key]; ok {
		cacheList.MoveToFront(findCacheElement(key))
		return val, nil
	}

	// Search SSTables from newest to oldest
	for i := len(sstables) - 1; i >= 0; i-- {
		sst := sstables[i]
		idx := binarySearch(sst.Index, key)
		if idx >= 0 {
			entry := sst.Index[idx]
			f, err := getFileHandle(sst.ChunkID)
			if err != nil {
				return "", err
			}

			r := bufio.NewReader(f)
			_, err = f.Seek(entry.Offset, 0)
			if err != nil {
				return "", err
			}

			limit := idx + 10
			if limit > len(sst.Index) {
				limit = len(sst.Index)
			}

			// Read target key + next 9 keys and cache them
			for j := idx; j < limit; j++ {
				line, _, err := r.ReadLine()
				if err != nil {
					break
				}
				parts := strings.Split(string(line), "\t")
				if len(parts) == 2 {
					cachePut(parts[0], parts[1])
				}
			}

			if val, ok := cache[key]; ok {
				return val, nil
			}
		}
	}
	return "", fmt.Errorf("not found")
}

func binarySearch(index []IndexEntry, key string) int {
	low, high := 0, len(index)-1
	for low <= high {
		mid := (low + high) / 2
		if index[mid].Key == key {
			return mid
		} else if index[mid].Key < key {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return -1
}

func getFileHandle(chunkID string) (*os.File, error) {
	if f, ok := openFiles[chunkID]; ok {
		moveFileToFront(chunkID)
		return f, nil
	}

	path := filepath.Join(dataDir, chunkID+".data.txt")
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	if openList.Len() >= maxOpen {
		back := openList.Back()
		if back != nil {
			oldest := back.Value.(string)
			openFiles[oldest].Close()
			delete(openFiles, oldest)
			openList.Remove(back)
		}
	}

	openFiles[chunkID] = f
	openList.PushFront(chunkID)
	return f, nil
}

func moveFileToFront(chunkID string) {
	for e := openList.Front(); e != nil; e = e.Next() {
		if e.Value.(string) == chunkID {
			openList.MoveToFront(e)
			return
		}
	}
}

func cachePut(key, val string) {
	if len(val) > 1024 {
		// NOTE: Don't cache too large values
		return
	}

	if _, ok := cache[key]; ok {
		cacheList.MoveToFront(findCacheElement(key))
		cache[key] = val
		return
	}

	if cacheList.Len() >= maxCacheLen {
		oldest := cacheList.Back()
		if oldest != nil {
			delete(cache, oldest.Value.(string))
			cacheList.Remove(oldest)
		}
	}

	cache[key] = val
	cacheList.PushFront(key)
}

func findCacheElement(key string) *list.Element {
	for e := cacheList.Front(); e != nil; e = e.Next() {
		if e.Value.(string) == key {
			return e
		}
	}
	return nil
}
