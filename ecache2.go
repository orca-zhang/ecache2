package ecache2

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"
)

var clock, p, n = time.Now().UnixNano(), uint16(0), uint16(1)

func now() int64 { return atomic.LoadInt64(&clock) }
func init() {
	go func() { // internal counter that reduce GC caused by `time.Now()`
		for {
			atomic.StoreInt64(&clock, time.Now().UnixNano()) // calibration every second
			for i := 0; i < 9; i++ {
				time.Sleep(100 * time.Millisecond)
				atomic.AddInt64(&clock, int64(100*time.Millisecond))
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
}

func hashBKRD(s string) (hash int32) {
	for i := 0; i < len(s); i++ {
		hash = hash*131 + int32(s[i])
	}
	return hash
}

func maskOfNextPowOf2(cap uint16) uint32 {
	if cap > 0 && cap&(cap-1) == 0 {
		return uint32(cap - 1)
	}
	cap |= (cap >> 1)
	cap |= (cap >> 2)
	cap |= (cap >> 4)
	return uint32(cap | (cap >> 8))
}

type value struct {
	i *interface{} // interface
	b []byte       // bytes
}

// Hashable is a constraint for key types that can be hashed for sharding
type Hashable interface {
	string | int64 | int32 | int | uint64 | uint32 | uint | [2]int64 | [2]int32 | [2]int | [2]uint64 | [2]uint32 | [2]uint
}

// hashKey computes the hash for sharding based on key type
// For string: uses BKRD hash
// For integer types: directly uses the key value
// For [2]int64: uses XOR of both values for better distribution
func hashKey[K Hashable](key K, mask int32) int32 {
	switch k := any(key).(type) {
	case string:
		return hashBKRD(k) & mask
	case int64:
		return int32(k) & mask
	case int32:
		return k & mask
	case int:
		return int32(k) & mask
	case uint64:
		return int32(k) & mask
	case uint32:
		return int32(k) & mask
	case uint:
		return int32(k) & mask
	case [2]int64:
		// Use XOR of both values for better distribution
		return int32(k[0]^k[1]) & mask
	case [2]int32:
		// Use XOR of both values for better distribution
		return (k[0] ^ k[1]) & mask
	case [2]int:
		// Use XOR of both values for better distribution
		return int32(k[0]^k[1]) & mask
	case [2]uint64:
		// Use XOR of both values for better distribution
		return int32(k[0]^k[1]) & mask
	case [2]uint32:
		// Use XOR of both values for better distribution
		return int32(k[0]^k[1]) & mask
	case [2]uint:
		// Use XOR of both values for better distribution
		return int32(k[0]^k[1]) & mask
	default:
		// fallback: should not happen with Hashable constraint
		return 0
	}
}

type node[K comparable] struct {
	k        K
	v        value
	expireAt int64 // nano timestamp, expireAt=0 if marked as deleted, `createdAt`=`expireAt`-`expiration`
}

type cache[K comparable] struct {
	dlnk [][2]uint16  // double link list, 0 for prev, 1 for next, the first node stands for [tail, head]
	m    []node[K]    // memory pre-allocated
	hmap map[K]uint16 // key -> idx in []node
	last uint16       // last element index when not full
}

func create[K comparable](cap uint32) *cache[K] {
	return &cache[K]{
		make([][2]uint16, cap+1),
		make([]node[K], cap),
		make(map[K]uint16, cap),
		0,
	}
}

// put a cache item into lru cache, if added return 1, updated return 0
func (c *cache[K]) put(k K, i *interface{}, b []byte, expireAt int64, on inspector[K]) int {
	if x, ok := c.hmap[k]; ok {
		c.m[x-1].v.i, c.m[x-1].v.b, c.m[x-1].expireAt = i, b, expireAt
		c.adjust(x, p, n) // refresh to head
		return 0
	}

	if c.last == uint16(cap(c.m)) {
		tail := &c.m[c.dlnk[0][p]-1]
		if (*tail).expireAt > 0 { // do not notify for mark delete ones
			on(PUT, (*tail).k, (*tail).v.i, (*tail).v.b, -1)
		}
		delete(c.hmap, (*tail).k)
		c.hmap[k], (*tail).k, (*tail).v.i, (*tail).v.b, (*tail).expireAt = c.dlnk[0][p], k, i, b, expireAt // reuse to reduce gc
		c.adjust(c.dlnk[0][p], p, n)                                                                       // refresh to head
		return 1
	}

	c.last++
	if len(c.hmap) <= 0 {
		c.dlnk[0][p] = c.last
	} else {
		c.dlnk[c.dlnk[0][n]][p] = c.last
	}
	c.m[c.last-1].k, c.m[c.last-1].v.i, c.m[c.last-1].v.b, c.m[c.last-1].expireAt, c.dlnk[c.last], c.hmap[k], c.dlnk[0][n] = k, i, b, expireAt, [2]uint16{0, c.dlnk[0][n]}, c.last, c.last
	return 1
}

// get value of key from lru cache with result
func (c *cache[K]) get(k K) (*node[K], int) {
	if x, ok := c.hmap[k]; ok {
		c.adjust(x, p, n) // refresh to head
		return &c.m[x-1], 1
	}
	return nil, 0
}

// delete item by key from lru cache
func (c *cache[K]) del(k K) (_ *node[K], _ int, e int64) {
	if x, ok := c.hmap[k]; ok && c.m[x-1].expireAt > 0 {
		c.m[x-1].expireAt, e = 0, c.m[x-1].expireAt // mark as deleted
		c.adjust(x, n, p)                           // sink to tail
		return &c.m[x-1], 1, e
	}
	return nil, 0, 0
}

// calls f sequentially for each valid item in the lru cache
func (c *cache[K]) walk(walker func(key K, iface *interface{}, bytes []byte, expireAt int64) bool) {
	for idx := c.dlnk[0][n]; idx != 0; idx = c.dlnk[idx][n] {
		if c.m[idx-1].expireAt > 0 && !walker(c.m[idx-1].k, c.m[idx-1].v.i, c.m[idx-1].v.b, c.m[idx-1].expireAt) {
			return
		}
	}
}

// when f=0, t=1, move to head, otherwise to tail
func (c *cache[K]) adjust(idx, f, t uint16) {
	if c.dlnk[idx][f] != 0 { // f=0, t=1, not head node, otherwise not tail
		c.dlnk[c.dlnk[idx][t]][f], c.dlnk[c.dlnk[idx][f]][t], c.dlnk[idx][f], c.dlnk[idx][t], c.dlnk[c.dlnk[0][t]][f], c.dlnk[0][t] = c.dlnk[idx][f], c.dlnk[idx][t], 0, c.dlnk[0][t], idx, idx
	}
}

type inspector[K comparable] func(action int, key K, iface *interface{}, bytes []byte, status int)

const (
	PUT = iota + 1
	GET
	DEL
)

// Cache - generic concurrent cache structure
// For string keys: uses BKRD hash for sharding
// For integer keys: uses key value directly for sharding (no hash calculation)
type Cache[K Hashable] struct {
	locks      []sync.Mutex
	insts      [][2]*cache[K] // level-0 for normal LRU, level-1 for LRU-2
	expiration time.Duration
	on         inspector[K]
	mask       int32
}

// NewLRUCache - create generic lru cache
// `bucketCnt` is buckets that shard items to reduce lock racing
// `capPerBkt` is length of each bucket, can store `capPerBkt * bucketCnt` count of items in Cache at most
// optional `expiration` is item alive time (and we only use lazy eviction here), default `0` stands for permanent
// For string keys: uses BKRD hash for sharding
// For integer keys: uses key value directly for sharding (no hash calculation)
func NewLRUCache[K Hashable](bucketCnt, capPerBkt uint16, expiration ...time.Duration) *Cache[K] {
	mask := maskOfNextPowOf2(bucketCnt)
	c := &Cache[K]{
		make([]sync.Mutex, mask+1),
		make([][2]*cache[K], mask+1),
		0,
		func(int, K, *interface{}, []byte, int) {},
		int32(mask),
	}
	for i := range c.insts {
		c.insts[i][0] = create[K](uint32(capPerBkt))
	}
	if len(expiration) > 0 {
		c.expiration = expiration[0]
	}
	return c
}

// LRU2 - add LRU-2 support (especially LRU-2 that when item visited twice it moves to upper-level-cache)
// `capPerBkt` is length of each LRU-2 bucket, can store extra `capPerBkt * bucketCnt` count of items in Cache at most
func (c *Cache[K]) LRU2(capPerBkt uint16) *Cache[K] {
	for i := range c.insts {
		c.insts[i][1] = create[K](uint32(capPerBkt))
	}
	return c
}

// put - put a item into cache
func (c *Cache[K]) put(key K, i *interface{}, b []byte) {
	idx := hashKey(key, c.mask)
	c.locks[idx].Lock()
	status := c.insts[idx][0].put(key, i, b, now()+int64(c.expiration), c.on)
	c.locks[idx].Unlock()
	c.on(PUT, key, i, b, status)
}

// ToInt64 - convert bytes to int64
func ToInt64(b []byte) (int64, bool) {
	if len(b) >= 8 {
		return int64(binary.LittleEndian.Uint64(b)), true
	}
	return 0, false
}

// Put - put an item into cache
func (c *Cache[K]) Put(key K, val interface{}) { c.put(key, &val, nil) }

// PutInt64 - put a digit item into cache
func (c *Cache[K]) PutInt64(key K, d int64) {
	var data [8]byte
	binary.LittleEndian.PutUint64(data[:], uint64(d))
	c.put(key, nil, data[:])
}

// PutBytes - put a bytes item into cache
func (c *Cache[K]) PutBytes(key K, b []byte) { c.put(key, nil, b) }

// Get - get value of key from cache with result
func (c *Cache[K]) Get(key K) (interface{}, bool) {
	if i, _, ok := c.get(key); ok && i != nil {
		return *i, true
	}
	return nil, false
}

// GetBytes - get bytes value of key from cache with result
func (c *Cache[K]) GetBytes(key K) ([]byte, bool) {
	if _, b, ok := c.get(key); ok {
		return b, true
	}
	return nil, false
}

// GetInt64 - get value of key from cache with result
func (c *Cache[K]) GetInt64(key K) (int64, bool) {
	if _, b, ok := c.get(key); ok && len(b) >= 8 {
		return int64(binary.LittleEndian.Uint64(b)), true
	}
	return 0, false
}

func (c *Cache[K]) _get(key K, idx, level int32) (*node[K], int) {
	if n, s := c.insts[idx][level].get(key); s > 0 && n.expireAt > 0 && (c.expiration <= 0 || now() < n.expireAt) {
		n.expireAt = now() + int64(c.expiration) // refresh expiration
		return n, s                              // no necessary to remove the expired item here, otherwise will cause GC thrashing
	}
	return nil, 0
}

func (c *Cache[K]) get(key K) (i *interface{}, b []byte, _ bool) {
	idx := hashKey(key, c.mask)
	c.locks[idx].Lock()
	n, s := (*node[K])(nil), 0
	if c.insts[idx][1] == nil { // (if LRU-2 mode not support, loss is little)
		n, s = c._get(key, idx, 0) // normal lru mode
	} else { // LRU-2 mode
		e := int64(0)
		if n, s, e = c.insts[idx][0].del(key); s <= 0 {
			n, s = c._get(key, idx, 1) // re-find in level-1
		} else {
			c.insts[idx][1].put(key, n.v.i, n.v.b, e, c.on) // find in level-0, move to level-1
		}
	}
	if s <= 0 {
		c.locks[idx].Unlock()
		c.on(GET, key, nil, nil, 0)
		return
	}
	i, b = n.v.i, n.v.b
	c.locks[idx].Unlock()
	c.on(GET, key, i, b, 1)
	return i, b, true
}

// Del - delete item by key from cache
func (c *Cache[K]) Del(key K) {
	idx := hashKey(key, c.mask)
	c.locks[idx].Lock()
	n, s, e := c.insts[idx][0].del(key)
	if c.insts[idx][1] != nil { // (if LRU-2 mode not support, loss is little)
		if n2, s2, e2 := c.insts[idx][1].del(key); n2 != nil && (n == nil || e < e2) { // callback latest added one if both exists
			n, s = n2, s2
		}
	}
	if s > 0 {
		c.on(DEL, key, n.v.i, n.v.b, 1)
		n.v.i, n.v.b = nil, nil // release now
	} else {
		c.on(DEL, key, nil, nil, 0)
	}
	c.locks[idx].Unlock()
}

// Walk - calls f sequentially for each valid item in the lru cache, return false to stop iteration for every bucket
func (c *Cache[K]) Walk(walker func(key K, iface *interface{}, bytes []byte, expireAt int64) bool) {
	for i := range c.insts {
		c.locks[i].Lock()
		if c.insts[i][0].walk(walker); c.insts[i][1] != nil {
			c.insts[i][1].walk(walker)
		}
		c.locks[i].Unlock()
	}
}

// Inspect - to inspect the actions
func (c *Cache[K]) Inspect(insptr inspector[K]) {
	old := c.on
	c.on = func(action int, key K, iface *interface{}, bytes []byte, status int) {
		old(action, key, iface, bytes, status) // call as the declared order, old first
		insptr(action, key, iface, bytes, status)
	}
}
