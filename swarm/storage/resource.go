package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/net/idna"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/contracts/ens"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
)

const (
	signatureLength     = 65
	indexSize           = 18
	DbDirName           = "resource"
	chunkSize           = 4096 // temporary until we implement DPA in the resourcehandler
	defaultStoreTimeout = 4000 * time.Millisecond
	hasherCount         = 8
	resourceHash        = SHA3Hash
)

type ResourceError struct {
	code int
	err  string
}

func (e *ResourceError) Error() string {
	return e.err
}

func (e *ResourceError) Code() int {
	return e.code
}

func NewResourceError(code int, s string) error {
	if code < 0 || code >= ErrCnt {
		panic("no such error code!")
	}
	r := &ResourceError{
		err: s,
	}
	switch code {
	case ErrNotFound, ErrIO, ErrUnauthorized, ErrInvalidValue, ErrDataOverflow, ErrNothingToReturn, ErrInvalidSignature, ErrNotSynced, ErrPeriodDepth:
		r.code = code
	}
	return r
}

type Signature [signatureLength]byte

type ResourceLookupParams struct {
	Limit bool
	Max   uint32
}

// Encapsulates an specific resource update. When synced it contains the most recent
// version of the resource update data.
type resource struct {
	name       *string
	nameHash   common.Hash
	startBlock uint64
	lastPeriod uint32
	lastKey    Key
	frequency  uint64
	version    uint32
	data       []byte
	updated    time.Time
}

// TODO Expire content after a defined period (to force resync)
func (self *resource) isSynced() bool {
	return !self.updated.IsZero()
}

func (self *resource) NameHash() common.Hash {
	return self.nameHash
}

type headerGetter interface {
	HeaderByNumber(context.Context, string, *big.Int) (*types.Header, error)
}

// Mutable resource is an entity which allows updates to a resource
// without resorting to ENS on each update.
// The update scheme is built on swarm chunks with chunk keys following
// a predictable, versionable pattern.
//
// Updates are defined to be periodic in nature, where periods are
// expressed in terms of number of blocks.
//
// The root entry of a mutable resource is tied to a unique identifier,
// typically - but not necessarily - an ens name.  The identifier must be
// an valid IDNA string. It also contains the block number
// when the resource update was first registered, and
// the block frequency with which the resource will be updated, both of
// which are stored as little-endian uint64 values in the database (for a
// total of 16 bytes).

// The root entry tells the requester from when the mutable resource was
// first added (block number) and in which block number to look for the
// actual updates. Thus, a resource update for identifier "føø.bar"
// starting at block 4200 with frequency 42 will have updates on block 4242,
// 4284, 4326 and so on.
//
// Note that the root entry is not required for the resource update scheme to
// work. A normal chunk of the blocknumber/frequency data can also be created,
// and pointed to by an external resource (ENS or manifest entry)
//
// Actual data updates are also made in the form of swarm chunks. The keys
// of the updates are the hash of a concatenation of properties as follows:
//
// sha256(period|version|namehash)
//
// The period is (currentblock - startblock) / frequency
//
// Using our previous example, this means that a period 3 will have 4326 as
// the block number.
//
// If more than one update is made to the same block number, incremental
// version numbers are used successively.
//
// A lookup agent need only know the identifier name in order to get the versions
//
// the resourcedata is:
// headerlength|period|version|identifier|data
//
// if a validator is active, the chunk data is:
// resourcedata|sign(resourcedata)
// otherwise, the chunk data is the same as the resourcedata
//
// headerlength is a 16 bit value containing the byte length of period|version|name
//
// TODO: Include modtime in chunk data + signature
type ResourceHandler struct {
	chunkStore      ChunkStore
	HashSize        int
	signer          ResourceSigner
	ethClient       headerGetter
	ensClient       *ens.ENS
	resources       map[string]*resource
	hashPool        sync.Pool
	resourceLock    sync.RWMutex
	storeTimeout    time.Duration
	queryMaxPeriods *ResourceLookupParams
}

type ResourceHandlerParams struct {
	QueryMaxPeriods *ResourceLookupParams
	Signer          ResourceSigner
	EthClient       headerGetter
	EnsClient       *ens.ENS
}

// Create or open resource update chunk store
func NewResourceHandler(params *ResourceHandlerParams) (*ResourceHandler, error) {
	if params.QueryMaxPeriods == nil {
		params.QueryMaxPeriods = &ResourceLookupParams{
			Limit: false,
		}
	}
	rh := &ResourceHandler{
		ethClient:    params.EthClient,
		ensClient:    params.EnsClient,
		resources:    make(map[string]*resource),
		storeTimeout: defaultStoreTimeout,
		signer:       params.Signer,
		hashPool: sync.Pool{
			New: func() interface{} {
				return MakeHashFunc(resourceHash)()
			},
		},
		queryMaxPeriods: params.QueryMaxPeriods,
	}

	for i := 0; i < hasherCount; i++ {
		hashfunc := MakeHashFunc(resourceHash)()
		if rh.HashSize == 0 {
			rh.HashSize = hashfunc.Size()
		}
		rh.hashPool.Put(hashfunc)
	}

	return rh, nil
}

// Sets the store backend for resource updates
func (self *ResourceHandler) SetStore(store ChunkStore) {
	self.chunkStore = store
}

// Chunk Validation method (matches ChunkValidatorFunc signature)
//
// If resource update, owner is checked against ENS record of resource name inferred from chunk data
// If parsed signature is nil, validates automatically
// If not resource update, it validates are root chunk if length is indexSize and first two bytes are 0
func (self *ResourceHandler) Validate(key *Key, data []byte) bool {
	signature, _, _, name, parseddata, err := self.parseUpdate(data)
	if err != nil {
		if len(data) == indexSize {
			if bytes.Equal(data[:2], []byte{0, 0}) {
				return true
			}
		}
		return false
	} else if signature == nil {
		return true
	}
	digest := self.keyDataHash(*key, parseddata)
	addr, err := getAddressFromDataSig(digest, *signature)
	if err != nil {
		return false
	}
	ok, _ := self.checkAccess(name, addr)
	return ok
}

// If no ens client is supplied, resource updates are not validated
func (self *ResourceHandler) IsValidated() bool {
	return self.ensClient != nil
}

// Create the resource update digest used in signatures
func (self *ResourceHandler) keyDataHash(key Key, data []byte) common.Hash {
	hasher := self.hashPool.Get().(SwarmHash)
	defer self.hashPool.Put(hasher)
	hasher.Reset()
	hasher.Write(key[:])
	hasher.Write(data)
	return common.BytesToHash(hasher.Sum(nil))
}

// Checks if current address matches owner address of ENS
func (self *ResourceHandler) checkAccess(name string, address common.Address) (bool, error) {
	if self.ensClient == nil {
		return true, nil
	}
	owneraddress, err := self.ensClient.Owner(ens.EnsNode(name))
	return (address == owneraddress), err
}

// get data from current resource
func (self *ResourceHandler) GetContent(name string) (Key, []byte, error) {
	rsrc := self.getResource(name)
	if rsrc == nil || !rsrc.isSynced() {
		return nil, nil, NewResourceError(ErrNotFound, "Resource does not exist or is not synced")
	}
	return rsrc.lastKey, rsrc.data, nil
}

func (self *ResourceHandler) GetLastPeriod(name string) (uint32, error) {
	rsrc := self.getResource(name)

	if rsrc == nil || !rsrc.isSynced() {
		return 0, NewResourceError(ErrNotFound, "Resource does not exist or is not synced")
	}
	return rsrc.lastPeriod, nil
}

func (self *ResourceHandler) GetVersion(name string) (uint32, error) {
	rsrc := self.getResource(name)
	if rsrc == nil {
		return 0, NewResourceError(ErrNotFound, "Resource does not exist")
	} else if !rsrc.isSynced() {
		return 0, NewResourceError(ErrNotSynced, "Resource is not synced")
	}
	return rsrc.version, nil
}

// \TODO should be hashsize * branches from the chosen chunker, implement with dpa
func (self *ResourceHandler) chunkSize() int64 {
	return chunkSize
}

// Creates a new root entry for a mutable resource identified by `name` with the specified `frequency`.
//
// The signature data should match the hash of the idna-converted name by the validator's namehash function, NOT the raw name bytes.
//
// The start block of the resource update will be the actual current block height of the connected network.
func (self *ResourceHandler) NewResource(ctx context.Context, name string, frequency uint64) (*resource, error) {

	// frequency 0 is invalid
	if frequency == 0 {
		return nil, NewResourceError(ErrInvalidValue, "Frequency cannot be 0")
	}

	if !isSafeName(name) {
		return nil, NewResourceError(ErrInvalidValue, fmt.Sprintf("Invalid name: '%s'", name))
	}

	nameHash := ens.EnsNode(name)

	if self.signer != nil {
		signature, err := self.signer.Sign(nameHash)
		if err != nil {
			return nil, NewResourceError(ErrInvalidSignature, fmt.Sprintf("Sign fail: %v", err))
		}
		addr, err := getAddressFromDataSig(nameHash, signature)
		if err != nil {
			return nil, NewResourceError(ErrInvalidSignature, fmt.Sprintf("Retrieve address from signature fail: %v", err))
		}
		ok, err := self.checkAccess(name, addr)
		if err != nil {
			return nil, err
		} else if !ok {
			return nil, NewResourceError(ErrUnauthorized, fmt.Sprintf("Not owner of '%s'", name))
		}
	}

	// get our blockheight at this time
	currentblock, err := self.getBlock(ctx, name)
	if err != nil {
		return nil, err
	}

	// chunk with key equal to namehash points to data of first blockheight + update frequency
	// from this we know from what blockheight we should look for updates, and how often
	chunk := NewChunk(Key(nameHash.Bytes()), nil)
	chunk.SData = make([]byte, indexSize)

	// root block has first two bytes both, which distinguishes from update bytes
	val := make([]byte, 8)
	binary.LittleEndian.PutUint64(val, currentblock)
	copy(chunk.SData[2:10], val)
	binary.LittleEndian.PutUint64(val, frequency)
	copy(chunk.SData[10:], val)
	self.chunkStore.Put(chunk)
	log.Debug("new resource", "name", name, "key", nameHash, "startBlock", currentblock, "frequency", frequency)

	rsrc := &resource{
		name:       &name,
		nameHash:   nameHash,
		startBlock: currentblock,
		frequency:  frequency,
		updated:    time.Now(),
	}
	self.setResource(name, rsrc)

	return rsrc, nil
}

// Searches and retrieves the specific version of the resource update identified by `name`
// at the specific block height
//
//
// If refresh is set to true, the resource data will be reloaded from the resource update
// root chunk.
// It is the callers responsibility to make sure that this chunk exists (if the resource
// update root data was retrieved externally, it typically doesn't)
func (self *ResourceHandler) LookupVersionByName(ctx context.Context, name string, period uint32, version uint32, refresh bool, maxLookup *ResourceLookupParams) (*resource, error) {
	return self.LookupVersion(ctx, ens.EnsNode(name), name, period, version, refresh, maxLookup)
}

func (self *ResourceHandler) LookupVersion(ctx context.Context, nameHash common.Hash, name string, period uint32, version uint32, refresh bool, maxLookup *ResourceLookupParams) (*resource, error) {
	rsrc, err := self.loadResource(nameHash, name, refresh)
	if err != nil {
		return nil, err
	}
	return self.lookup(rsrc, period, version, refresh, maxLookup)
}

// Retrieves the latest version of the resource update identified by `name`
// at the specified block height
//
// If an update is found, version numbers are iterated until failure, and the last
// successfully retrieved version is copied to the corresponding resources map entry
// and returned.
//
// See also (*ResourceHandler).LookupVersion
func (self *ResourceHandler) LookupHistoricalByName(ctx context.Context, name string, period uint32, refresh bool, maxLookup *ResourceLookupParams) (*resource, error) {
	return self.LookupHistorical(ctx, ens.EnsNode(name), name, period, refresh, maxLookup)
}

func (self *ResourceHandler) LookupHistorical(ctx context.Context, nameHash common.Hash, name string, period uint32, refresh bool, maxLookup *ResourceLookupParams) (*resource, error) {
	rsrc, err := self.loadResource(nameHash, name, refresh)
	if err != nil {
		return nil, err
	}
	return self.lookup(rsrc, period, 0, refresh, maxLookup)
}

// Retrieves the latest version of the resource update identified by `name`
// at the next update block height
//
// It starts at the next period after the current block height, and upon failure
// tries the corresponding keys of each previous period until one is found
// (or startBlock is reached, in which case there are no updates).
//
// Version iteration is done as in (*ResourceHandler).LookupHistorical
//
// See also (*ResourceHandler).LookupHistorical
func (self *ResourceHandler) LookupLatestByName(ctx context.Context, name string, refresh bool, maxLookup *ResourceLookupParams) (*resource, error) {
	return self.LookupLatest(ctx, ens.EnsNode(name), name, refresh, maxLookup)
}

func (self *ResourceHandler) LookupLatest(ctx context.Context, nameHash common.Hash, name string, refresh bool, maxLookup *ResourceLookupParams) (*resource, error) {

	// get our blockheight at this time and the next block of the update period
	rsrc, err := self.loadResource(nameHash, name, refresh)
	if err != nil {
		return nil, err
	}
	currentblock, err := self.getBlock(ctx, name)
	if err != nil {
		return nil, err
	}
	nextperiod := getNextPeriod(rsrc.startBlock, currentblock, rsrc.frequency)
	return self.lookup(rsrc, nextperiod, 0, refresh, maxLookup)
}

// base code for public lookup methods
func (self *ResourceHandler) lookup(rsrc *resource, period uint32, version uint32, refresh bool, maxLookup *ResourceLookupParams) (*resource, error) {

	if self.chunkStore == nil {
		return nil, NewResourceError(ErrInit, "Call ResourceHandler.SetStore() before performing lookups")
	}

	if period == 0 {
		return nil, NewResourceError(ErrInvalidValue, "period must be >0")
	}

	// start from the last possible block period, and iterate previous ones until we find a match
	// if we hit startBlock we're out of options
	var specificversion bool
	if version > 0 {
		specificversion = true
	} else {
		version = 1
	}

	var hops uint32
	if maxLookup == nil {
		maxLookup = self.queryMaxPeriods
	}
	for period > 0 {
		if maxLookup.Limit && hops > maxLookup.Max {
			return nil, NewResourceError(ErrPeriodDepth, fmt.Sprintf("Lookup exceeded max period hops (%d)", maxLookup.Max))
		}
		key := self.resourceHash(period, version, rsrc.nameHash)
		chunk, err := self.chunkStore.Get(key)
		if err == nil {
			if specificversion {
				return self.updateResourceIndex(rsrc, chunk)
			}
			// check if we have versions > 1. If a version fails, the previous version is used and returned.
			log.Trace("rsrc update version 1 found, checking for version updates", "period", period, "key", key)
			for {
				newversion := version + 1
				key := self.resourceHash(period, newversion, rsrc.nameHash)
				newchunk, err := self.chunkStore.Get(key)
				if err != nil {
					return self.updateResourceIndex(rsrc, chunk)
				}
				log.Trace("version update found, checking next", "version", version, "period", period, "key", key)
				chunk = newchunk
				version = newversion
			}
		}
		log.Trace("rsrc update not found, checking previous period", "period", period, "key", key)
		period--
		hops++
	}
	return nil, NewResourceError(ErrNotFound, "no updates found")
}

// load existing mutable resource into resource struct
func (self *ResourceHandler) loadResource(nameHash common.Hash, name string, refresh bool) (*resource, error) {

	if name == "" {
		name = nameHash.Hex()
	}

	// if the resource is not known to this session we must load it
	// if refresh is set, we force load
	rsrc := self.getResource(name)
	if rsrc == nil || refresh {
		rsrc = &resource{}
		// make sure our name is safe to use
		if !isSafeName(name) {
			return nil, NewResourceError(ErrInvalidValue, fmt.Sprintf("Invalid name '%s'", name))
		}
		rsrc.name = &name
		rsrc.nameHash = nameHash

		// get the root info chunk and update the cached value
		chunk, err := self.chunkStore.Get(Key(rsrc.nameHash[:]))
		if err != nil {
			return nil, err
		}

		// minimum sanity check for chunk data
		if len(chunk.SData) != indexSize {
			return nil, NewResourceError(ErrNothingToReturn, fmt.Sprintf("Invalid chunk length %d, should be %d", len(chunk.SData), indexSize))
		}
		rsrc.startBlock = binary.LittleEndian.Uint64(chunk.SData[2:10])
		rsrc.frequency = binary.LittleEndian.Uint64(chunk.SData[10:])
	} else {
		rsrc.name = self.resources[name].name
		rsrc.nameHash = self.resources[name].nameHash
		rsrc.startBlock = self.resources[name].startBlock
		rsrc.frequency = self.resources[name].frequency
	}
	return rsrc, nil
}

// update mutable resource index map with specified content
func (self *ResourceHandler) updateResourceIndex(rsrc *resource, chunk *Chunk) (*resource, error) {

	// retrieve metadata from chunk data and check that it matches this mutable resource
	signature, period, version, name, data, err := self.parseUpdate(chunk.SData)
	if *rsrc.name != name {
		return nil, NewResourceError(ErrNothingToReturn, fmt.Sprintf("Update belongs to '%s', but have '%s'", name, *rsrc.name))
	}
	log.Trace("update", "name", *rsrc.name, "rootkey", rsrc.nameHash, "updatekey", chunk.Key, "period", period, "version", version)
	// check signature (if signer algorithm is present)
	if signature != nil {
		digest := self.keyDataHash(chunk.Key, data)
		_, err = getAddressFromDataSig(digest, *signature)
		if err != nil {
			return nil, NewResourceError(ErrUnauthorized, fmt.Sprintf("Invalid signature: %v", err))
		}
	}

	// update our rsrcs entry map
	rsrc.lastKey = chunk.Key
	rsrc.lastPeriod = period
	rsrc.version = version
	rsrc.updated = time.Now()
	rsrc.data = make([]byte, len(data))
	copy(rsrc.data, data)
	log.Debug("Resource synced", "name", *rsrc.name, "key", chunk.Key, "period", rsrc.lastPeriod, "version", rsrc.version)
	self.setResource(*rsrc.name, rsrc)
	return rsrc, nil
}

// retrieve update metadata from chunk data
// mirrors newUpdateChunk()
func (self *ResourceHandler) parseUpdate(chunkdata []byte) (*Signature, uint32, uint32, string, []byte, error) {
	// 14 = header + one byte of name + one byte of data
	if len(chunkdata) < 14 {
		return nil, 0, 0, "", nil, NewResourceError(ErrNothingToReturn, "chunk less than 13 bytes cannot be a resource update chunk")
	}
	cursor := 0
	headerlength := binary.LittleEndian.Uint16(chunkdata[cursor : cursor+2])
	cursor += 2
	datalength := binary.LittleEndian.Uint16(chunkdata[cursor : cursor+2])
	exclsignlength := int(headerlength + datalength + 4)
	if exclsignlength > len(chunkdata) || exclsignlength < 14 {
		return nil, 0, 0, "", nil, NewResourceError(ErrNothingToReturn, fmt.Sprintf("Reported headerlength %d + datalength %d longer than actual chunk data length %d", headerlength, datalength, len(chunkdata)))
	}
	var period uint32
	var version uint32
	var name string
	var data []byte
	cursor += 2
	period = binary.LittleEndian.Uint32(chunkdata[cursor : cursor+4])
	cursor += 4
	version = binary.LittleEndian.Uint32(chunkdata[cursor : cursor+4])
	cursor += 4
	namelength := int(headerlength) - cursor + 4
	name = string(chunkdata[cursor : cursor+namelength])
	cursor += namelength
	var intdatalength int
	if datalength == 0 {
		intdatalength = isMultihash(chunkdata[cursor:])
		multihashboundary := cursor + intdatalength
		if len(chunkdata) != multihashboundary && len(chunkdata) < multihashboundary+signatureLength {
			log.Debug("multihash error", "chunkdatalen", len(chunkdata), "multihashboundary", multihashboundary)
			return nil, 0, 0, "", nil, errors.New("Corrupt multihash data")
		}
	} else {
		intdatalength = int(datalength)
	}
	data = make([]byte, intdatalength)
	copy(data, chunkdata[cursor:cursor+intdatalength])

	// omit signatures if we have no validator
	var signature *Signature
	cursor += intdatalength
	if self.signer != nil {
		sigdata := chunkdata[cursor : cursor+signatureLength]
		if len(sigdata) > 0 {
			signature = &Signature{}
			copy(signature[:], sigdata)
		}
	}

	return signature, period, version, name, data, nil
}

// Adds an actual data update
//
// Uses the data currently loaded in the resources map entry.
// It is the caller's responsibility to make sure that this data is not stale.
//
// A resource update cannot span chunks, and thus has max length 4096

func (self *ResourceHandler) UpdateMultihash(ctx context.Context, name string, data []byte) (Key, error) {
	if isMultihash(data) == 0 {
		return nil, NewResourceError(ErrNothingToReturn, "Invalid multihash")
	}
	return self.update(ctx, name, data, true)
}

func (self *ResourceHandler) Update(ctx context.Context, name string, data []byte) (Key, error) {
	return self.update(ctx, name, data, false)
}

func (self *ResourceHandler) update(ctx context.Context, name string, data []byte, multihash bool) (Key, error) {

	if self.chunkStore == nil {
		return nil, NewResourceError(ErrInit, "Call ResourceHandler.SetStore() before updating")
	}
	var signaturelength int
	if self.signer != nil {
		signaturelength = signatureLength
	}

	// get the cached information
	rsrc := self.getResource(name)
	if rsrc == nil {
		return nil, NewResourceError(ErrNotFound, "Resource object not in index")
	}
	if !rsrc.isSynced() {
		return nil, NewResourceError(ErrNotSynced, "Resource object not in sync")
	}

	// an update can be only one chunk long
	datalimit := self.chunkSize() - int64(signaturelength-len(name)-4-4-2-2)
	if int64(len(data)) > datalimit {
		return nil, NewResourceError(ErrDataOverflow, fmt.Sprintf("Data overflow: %d / %d bytes", len(data), datalimit))
	}

	// get our blockheight at this time and the next block of the update period
	currentblock, err := self.getBlock(ctx, name)
	if err != nil {
		return nil, NewResourceError(ErrIO, fmt.Sprintf("Could not get block height: %v", err))
	}
	nextperiod := getNextPeriod(rsrc.startBlock, currentblock, rsrc.frequency)

	// if we already have an update for this block then increment version
	// (resource object MUST be in sync for version to be correct)
	var version uint32
	if self.hasUpdate(name, nextperiod) {
		version = rsrc.version
	}
	version++

	// calculate the chunk key
	key := self.resourceHash(nextperiod, version, rsrc.nameHash)

	var signature *Signature
	if self.signer != nil {
		// sign the data hash with the key
		digest := self.keyDataHash(key, data)
		sig, err := self.signer.Sign(digest)
		if err != nil {
			return nil, NewResourceError(ErrInvalidSignature, fmt.Sprintf("Sign fail: %v", err))
		}
		signature = &sig

		// get the address of the signer (which also checks that it's a valid signature)
		addr, err := getAddressFromDataSig(digest, *signature)
		if err != nil {
			return nil, NewResourceError(ErrInvalidSignature, fmt.Sprintf("Invalid data/signature: %v", err))
		}
		if self.signer != nil {
			// check if the signer has access to update
			ok, err := self.checkAccess(name, addr)
			if err != nil {
				return nil, NewResourceError(ErrIO, fmt.Sprintf("Access check fail: %v", err))
			} else if !ok {
				return nil, NewResourceError(ErrUnauthorized, fmt.Sprintf("Address %x does not have access to update %s", addr, name))
			}
		}
	}

	var datalength int
	if !multihash {
		datalength = len(data)
	}
	chunk := newUpdateChunk(key, signature, nextperiod, version, name, data, datalength)

	// send the chunk
	self.chunkStore.Put(chunk)
	timeout := time.NewTimer(self.storeTimeout)
	select {
	case <-chunk.dbStoredC:
	case <-timeout.C:
		return nil, NewResourceError(ErrIO, "chunk store timeout")
	}
	log.Trace("resource update", "name", name, "key", key, "currentblock", currentblock, "lastperiod", nextperiod, "version", version, "data", chunk.SData)

	// update our resources map entry and return the new key
	rsrc.lastPeriod = nextperiod
	rsrc.version = version
	rsrc.data = make([]byte, len(data))
	copy(rsrc.data, data)
	return key, nil
}

// Closes the datastore.
// Always call this at shutdown to avoid data corruption.
func (self *ResourceHandler) Close() {
	self.chunkStore.Close()
}

func (self *ResourceHandler) getBlock(ctx context.Context, name string) (uint64, error) {
	blockheader, err := self.ethClient.HeaderByNumber(ctx, name, nil)
	if err != nil {
		return 0, err
	}
	return blockheader.Number.Uint64(), nil
}

// Calculate the period index (aka major version number) from a given block number
func (self *ResourceHandler) BlockToPeriod(name string, blocknumber uint64) uint32 {
	return getNextPeriod(self.resources[name].startBlock, blocknumber, self.resources[name].frequency)
}

// Calculate the block number from a given period index (aka major version number)
func (self *ResourceHandler) PeriodToBlock(name string, period uint32) uint64 {
	return self.resources[name].startBlock + (uint64(period) * self.resources[name].frequency)
}

func (self *ResourceHandler) getResource(name string) *resource {
	self.resourceLock.RLock()
	defer self.resourceLock.RUnlock()
	rsrc := self.resources[name]
	return rsrc
}

func (self *ResourceHandler) setResource(name string, rsrc *resource) {
	self.resourceLock.Lock()
	defer self.resourceLock.Unlock()
	self.resources[name] = rsrc
}

// used for chunk keys
func (self *ResourceHandler) resourceHash(period uint32, version uint32, namehash common.Hash) Key {
	// format is: hash(period|version|namehash)
	hasher := self.hashPool.Get().(SwarmHash)
	defer self.hashPool.Put(hasher)
	hasher.Reset()
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, period)
	hasher.Write(b)
	binary.LittleEndian.PutUint32(b, version)
	hasher.Write(b)
	hasher.Write(namehash[:])
	return hasher.Sum(nil)
}

func (self *ResourceHandler) hasUpdate(name string, period uint32) bool {
	return self.resources[name].lastPeriod == period
}

func getAddressFromDataSig(datahash common.Hash, signature Signature) (common.Address, error) {
	pub, err := crypto.SigToPub(datahash.Bytes(), signature[:])
	if err != nil {
		return common.Address{}, err
	}
	return crypto.PubkeyToAddress(*pub), nil
}

// create an update chunk
func newUpdateChunk(key Key, signature *Signature, period uint32, version uint32, name string, data []byte, datalength int) *Chunk {

	// no signatures if no validator
	var signaturelength int
	if signature != nil {
		signaturelength = signatureLength
	}

	// prepend version and period to allow reverse lookups
	headerlength := len(name) + 4 + 4

	actualdatalength := len(data)
	chunk := NewChunk(key, nil)
	chunk.SData = make([]byte, 4+signaturelength+headerlength+actualdatalength) // initial 4 are uint16 length descriptors for headerlength and datalength

	// data header length does NOT include the header length prefix bytes themselves
	cursor := 0
	binary.LittleEndian.PutUint16(chunk.SData[cursor:], uint16(headerlength))
	cursor += 2

	// data length
	binary.LittleEndian.PutUint16(chunk.SData[cursor:], uint16(datalength))
	cursor += 2

	// header = period + version + name
	binary.LittleEndian.PutUint32(chunk.SData[cursor:], period)
	cursor += 4

	binary.LittleEndian.PutUint32(chunk.SData[cursor:], version)
	cursor += 4

	namebytes := []byte(name)
	copy(chunk.SData[cursor:], namebytes)
	cursor += len(namebytes)

	// add the data
	copy(chunk.SData[cursor:], data)

	// if signature is present it's the last item in the chunk data
	if signature != nil {
		cursor += actualdatalength
		copy(chunk.SData[cursor:], signature[:])
	}

	chunk.Size = int64(len(chunk.SData))
	return chunk
}

func getNextPeriod(start uint64, current uint64, frequency uint64) uint32 {
	blockdiff := current - start
	period := blockdiff / frequency
	return uint32(period + 1)
}

func ToSafeName(name string) (string, error) {
	return idna.ToASCII(name)
}

// check that name identifiers contain valid bytes
func isSafeName(name string) bool {
	if name == "" {
		return false
	}
	validname, err := idna.ToASCII(name)
	if err != nil {
		return false
	}
	return validname == name
}

// if first byte is the start of a multihash this function will try to parse it
// if successful it returns the length of multihash data, 0 otherwise
func isMultihash(data []byte) int {
	cursor := 0
	_, c := binary.Uvarint(data)
	if c <= 0 {
		log.Warn("Corrupt multihash data, hashtype is unreadable")
		return 0
	}
	cursor += c
	hashlength, c := binary.Uvarint(data[cursor:])
	if c <= 0 {
		log.Warn("Corrupt multihash data, hashlength is unreadable")
		return 0
	}
	cursor += c
	// we cheekily assume hashlength < maxint
	inthashlength := int(hashlength)
	if len(data[cursor:]) < inthashlength {
		log.Warn("Corrupt multihash data, hash does not align with data boundary")
		return 0
	}
	return cursor + inthashlength
}

// TODO: this should not be exposed, but swarm/testutil/http.go needs it
func NewTestResourceHandler(datadir string, params *ResourceHandlerParams) (*ResourceHandler, error) {
	path := filepath.Join(datadir, DbDirName)
	rh, err := NewResourceHandler(params)
	if err != nil {
		return nil, err
	}
	localstoreparams := NewDefaultLocalStoreParams()
	localstoreparams.Init(path)
	localStore, err := NewLocalStore(localstoreparams, nil)
	if err != nil {
		return nil, err
	}
	localStore.Validator = &ChunkValidator{
		resource: rh.Validate,
	}
	rh.SetStore(localStore)
	return rh, nil
}
