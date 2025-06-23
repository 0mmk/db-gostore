package cache

import (
	"container/list"
	"os"
	"sync"
)

type cacheEntry struct {
	key   string
	value interface{} // Changed to interface{} to store any type
}

type LRUCache struct {
	capacity int
	ll       *list.List
	cache    map[string]*list.Element
	mu       sync.Mutex // Added mutex for concurrent safety
}

func NewLRUCache(capacity int) *LRUCache {
	if capacity <= 0 {
		capacity = 256 // Default capacity
	}
	return &LRUCache{
		capacity: capacity,
		ll:       list.New(),
		cache:    make(map[string]*list.Element, capacity),
	}
}

// Get looks up a key's value from the cache in O(1).
func (c *LRUCache) Get(key string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.cache[key]; ok {
		c.ll.MoveToFront(elem)
		return elem.Value.(*cacheEntry).value, true
	}
	return nil, false
}

// Put adds or updates a value in the cache in O(1).
func (c *LRUCache) Put(key string, value interface{}) {
	// Reject nil values
	if value == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.cache[key]; ok {
		// Key exists, update value and move to front.
		c.ll.MoveToFront(elem)
		// If old value is a file, close it before replacing
		if oldFile, ok := elem.Value.(*cacheEntry).value.(*os.File); ok {
			oldFile.Close()
		}
		elem.Value.(*cacheEntry).value = value
	} else {
		// Key doesn't exist, add new entry.
		if c.ll.Len() >= c.capacity {
			oldest := c.ll.Back()
			if oldest != nil {
				entry := oldest.Value.(*cacheEntry)
				// If evicting a file, close it
				if file, ok := entry.value.(*os.File); ok {
					file.Close()
				}
				delete(c.cache, entry.key)
				c.ll.Remove(oldest)
			}
		}
		newElem := c.ll.PushFront(&cacheEntry{key: key, value: value})
		c.cache[key] = newElem
	}
}

// Each iterates over all items in the cache.
func (c *LRUCache) Each(f func(key string, value interface{})) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for e := c.ll.Front(); e != nil; e = e.Next() {
		entry := e.Value.(*cacheEntry)
		f(entry.key, entry.value)
	}
}

func (c *LRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Close any files that might be in the cache before clearing
	for e := c.ll.Front(); e != nil; e = e.Next() {
		if file, ok := e.Value.(*cacheEntry).value.(*os.File); ok {
			file.Close()
		}
	}
	c.ll = list.New()
	c.cache = make(map[string]*list.Element, c.capacity)
}