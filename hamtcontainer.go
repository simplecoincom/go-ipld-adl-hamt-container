package hamtcontainer

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"sync"

	"github.com/ipfs/go-cid"
	gocar "github.com/ipld/go-car"
	hamt "github.com/ipld/go-ipld-adl-hamt"
	ipld "github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	sbuilder "github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/simplecoincom/go-ipld-adl-hamt-container/storage"
	"github.com/simplecoincom/go-ipld-adl-hamt-container/utils"
)

const reservedNameKey = "__META_RESERVED_HAMT_KEY__"

// AssemblerFunc function used to set assembler routines
type AssemblerFunc func(hamtSetter HAMTSetter) error

var (
	ErrHAMTNotBuild                  = errors.New("HAMT not ready, build first")
	ErrHAMTValueNotFound             = errors.New("Value not found at HAMT")
	ErrHAMTNoNestedFound             = errors.New("No nested found with the given key")
	ErrHAMTFailedToLoadNested        = errors.New("Failed to load link from nested HAMT")
	ErrHAMTUnsupportedValueType      = errors.New("Unsupported value")
	ErrHAMTUnsupportedCacheValueType = errors.New("Unsupported cache value")
	ErrHAMTFailedToGetAsLink         = errors.New("Value returned should be ipld.Link")
	ErrHAMTFailedToGetAsBytes        = errors.New("Value returned should be Bytes")
	ErrHAMTFailedToGetAsString       = errors.New("Value returned should be String")

	BitWidth   = 8
	BucketSize = 1024
)

type HAMTContainer struct {
	mutex sync.RWMutex
	key   []byte
	// Used to cache key before build the HAMT Container
	kvCache    map[string]interface{}
	storage    storage.Storage
	link       ipld.Link
	linkSystem ipld.LinkSystem
	linkProto  ipld.LinkPrototype
	node       ipld.Node
	limit      int
}

// HAMTSetter is a helper structure for set HAMT key values
type HAMTSetter struct {
	assembler ipld.MapAssembler
}

// Key returns the key that identifies the HAMT
func (hc *HAMTContainer) Key() []byte {
	hc.mutex.RLock()
	defer hc.mutex.RUnlock()
	return hc.key
}

// Storage returns the linking storage used by the HAMT
func (hc *HAMTContainer) Storage() storage.Storage {
	hc.mutex.RLock()
	defer hc.mutex.RUnlock()
	return hc.storage
}

// CID will return the cid.Cid for the ipld.Node
// Or it will return an error if the ipld.Node for the HAMT isn't built
func (hc *HAMTContainer) CID() (cid.Cid, error) {
	hc.mutex.RLock()
	defer hc.mutex.RUnlock()

	if hc.link == nil {
		return cid.Cid{}, ErrHAMTNotBuild
	}

	return cid.Parse(hc.link.String())
}

// GetLink will return the ipld.Link for the ipld.Node
// Or it will return an error if the ipld.Node for the HAMT isn't built
func (hc *HAMTContainer) GetLink() (ipld.Link, error) {
	hc.mutex.RLock()
	defer hc.mutex.RUnlock()

	if hc.link == nil {
		return nil, ErrHAMTNotBuild
	}

	return hc.link, nil
}

// LoadLink will load the storage data from a new HAMTContainer
// Or it illl return and error if the load failed
func (hc *HAMTContainer) LoadLink(link ipld.Link) error {
	hc.mutex.Lock()
	defer hc.mutex.Unlock()

	nodePrototype := basicnode.Prototype.Any

	node, err := hc.linkSystem.Load(
		ipld.LinkContext{}, // The zero value is fine.  Configure it it you want cancellability or other features.
		link,               // The Link we want to load!
		nodePrototype,      // The NodePrototype says what kind of Node we want as a result.
	)
	if err != nil {
		return err
	}

	hc.link = link
	hc.node = node

	return nil
}

// MustBuild is used to build the key maps
// It'll generate the final version of the node with the link
func (hc *HAMTContainer) MustBuild(assemblyFuncs ...AssemblerFunc) error {
	hc.mutex.Lock()
	defer hc.mutex.Unlock()

	// Creates the builder for the HAMT
	builder := hamt.NewBuilder(hamt.Prototype{BitWidth: BitWidth, BucketSize: BucketSize}).
		WithLinking(hc.linkSystem, hc.linkProto)

	// Begin the map build
	assembler, err := builder.BeginMap(0)
	if err != nil {
		return err
	}

	// Set key and value for reserved name
	{
		if err := assembler.AssembleKey().AssignString(hex.EncodeToString([]byte(reservedNameKey))); err != nil {
			return err
		}

		if err := assembler.AssembleValue().AssignBytes(hc.key); err != nil {
			return err
		}
	}

	// Node not nil, then should concat
	if hc.node != nil {
		mapIter := hc.node.MapIterator()

		for !mapIter.Done() {
			key, value, err := mapIter.Next()
			if err != nil {
				return err
			}

			// Keys are always store as string on key value
			ks, err := key.AsString()
			if err != nil {
				return err
			}

			// But should be decoded to bytes
			kb, err := hex.DecodeString(ks)
			if err != nil {
				return err
			}

			// Do not view meta keys
			if string(kb) == reservedNameKey {
				continue
			}

			// Concat the prev values with current cache
			switch kind := value.Kind(); kind {
			case ipld.Kind_String:
				val, _ := value.AsString()
				hc.kvCache[string(ks)] = val
			case ipld.Kind_Bytes:
				val, _ := value.AsBytes()
				hc.kvCache[string(ks)] = val
			case ipld.Kind_Link:
				val, _ := value.AsLink()
				hc.kvCache[string(ks)] = val
			default:
				return ErrHAMTUnsupportedCacheValueType
			}
		}
	}

	// For each key in cache should be added too
	if hc.node != nil {
		mapIter := hc.node.MapIterator()

		for !mapIter.Done() {
			key, value, err := mapIter.Next()
			if err != nil {
				return err
			}

			ks, err := key.AsString()
			if err != nil {
				return err
			}

			bs, err := hex.DecodeString(ks)
			if err != nil {
				return err
			}

			// Do not view meta keys
			if string(bs) == reservedNameKey {
				continue
			}

			switch kind := value.Kind(); kind {
			case ipld.Kind_String:
				val, _ := value.AsString()
				hc.kvCache[string(ks)] = val
			case ipld.Kind_Bytes:
				val, _ := value.AsBytes()
				hc.kvCache[string(ks)] = val
			case ipld.Kind_Link:
				val, _ := value.AsLink()
				hc.kvCache[string(ks)] = val
			default:
				return ErrHAMTUnsupportedCacheValueType
			}
		}
	}

	for k, v := range hc.kvCache {
		if err := assembler.AssembleKey().AssignString(k); err != nil {
			return err
		}

		// Support types for value
		switch v := v.(type) {
		case string:
			if err := assembler.AssembleValue().AssignString(v); err != nil {
				return err
			}
		case []byte:
			if err := assembler.AssembleValue().AssignBytes(v); err != nil {
				return err
			}
		case ipld.Link:
			if err := assembler.AssembleValue().AssignLink(v); err != nil {
				return err
			}
		case *HAMTContainer:
			link, err := v.GetLink()
			if err != nil {
				return err
			}

			if err := assembler.AssembleValue().AssignLink(link); err != nil {
				return err
			}
		case HAMTContainer:
			link, err := v.GetLink()
			if err != nil {
				return err
			}

			if err := assembler.AssembleValue().AssignLink(link); err != nil {
				return err
			}
		default:
			return ErrHAMTUnsupportedValueType
		}
	}

	// Run the assembly funcs
	for _, assemblyFunc := range assemblyFuncs {
		if err := assemblyFunc(HAMTSetter{assembler}); err != nil {
			return err
		}
	}

	// Finish the assembler process
	if err := assembler.Finish(); err != nil {
		return err
	}

	// Build the hamt
	hc.node = hamt.Build(builder)

	// Store the values into link system
	link, err := hc.linkSystem.Store(
		ipld.LinkContext{},
		hc.linkProto,
		hc.node,
	)

	if err != nil {
		return err
	}

	// Our current link
	hc.link = link

	return nil
}

// Set adds k/v to the hamt but not imediately and only when build
func (hc *HAMTContainer) Set(key []byte, value interface{}) {
	hc.mutex.Lock()
	defer hc.mutex.Unlock()
	hc.kvCache[hex.EncodeToString(key)] = value
}

// Set adds a new k/v content for the HAMT
// For string values it will add k/v pair of strings
// For ipld.Link values it will add string key and a link for another HAMT structure as value
func (hs *HAMTSetter) Set(key []byte, value interface{}) error {
	if err := hs.assembler.AssembleKey().AssignString(hex.EncodeToString(key)); err != nil {
		return err
	}

	// Support types for value
	switch v := value.(type) {
	case string:
		if err := hs.assembler.AssembleValue().AssignString(v); err != nil {
			return err
		}
	case []byte:
		if err := hs.assembler.AssembleValue().AssignBytes(v); err != nil {
			return err
		}
	case ipld.Link:
		if err := hs.assembler.AssembleValue().AssignLink(v); err != nil {
			return err
		}
	case *HAMTContainer:
		link, err := v.GetLink()
		if err != nil {
			return err
		}

		if err := hs.assembler.AssembleValue().AssignLink(link); err != nil {
			return err
		}
	case HAMTContainer:
		link, err := v.GetLink()
		if err != nil {
			return err
		}

		if err := hs.assembler.AssembleValue().AssignLink(link); err != nil {
			return err
		}
	default:
		return ErrHAMTUnsupportedValueType
	}

	return nil
}

// Get will return the value by the key
// It will return error if the hamt not build or if the value not found
func (hc *HAMTContainer) Get(key []byte) (interface{}, error) {
	hc.mutex.Lock()
	defer hc.mutex.Unlock()

	// Node should be build to retrieve values
	if hc.node == nil {
		return nil, ErrHAMTNotBuild
	}

	// Lookup by string, first translate the byte to hex string
	valNode, err := hc.node.LookupByString(hex.EncodeToString(key))
	if err != nil {
		if errors.Is(err, err.(ipld.ErrNotExists)) {
			return nil, ErrHAMTValueNotFound
		}
		return nil, err
	}

	if valNode == nil {
		return nil, ErrHAMTValueNotFound
	}

	return utils.NodeValue(valNode)
}

// GetAsLink returns a ipld.Link type by key
// The method will fail if the returned type isn't of type ipld.Link
func (hc *HAMTContainer) GetAsLink(key []byte) (ipld.Link, error) {
	result, err := hc.Get(key)
	if err != nil {
		return nil, err
	}

	switch result.(type) {
	case ipld.Link:
		return result.(ipld.Link), nil
	default:
		return nil, ErrHAMTFailedToGetAsLink
	}
}

// GetAsBytes returns a byte slice type by key
// The method will fail if the returned type isn't of type byte slice
func (hc *HAMTContainer) GetAsBytes(key []byte) ([]byte, error) {
	result, err := hc.Get(key)
	if err != nil {
		return nil, err
	}

	switch result.(type) {
	case []byte:
		return result.([]byte), nil
	default:
		return nil, ErrHAMTFailedToGetAsBytes
	}
}

// GetAsString returns a string type by key
// The method will fail if the returned type isn't of type string or failed to convert to string
func (hc *HAMTContainer) GetAsString(key []byte) (string, error) {
	result, err := hc.Get(key)
	if err != nil {
		return "", err
	}

	switch r := result.(type) {
	case []byte:
		return string(r), nil
	case string:
		return r, nil
	default:
		return "", ErrHAMTFailedToGetAsString
	}
}

// View will iterate over each item key map
func (hc *HAMTContainer) View(iterFunc func(key []byte, value interface{}) error) error {
	hc.mutex.RLock()
	defer hc.mutex.RUnlock()

	// Should build the node before
	if hc.node == nil {
		return ErrHAMTNotBuild
	}

	mapIter := hc.node.MapIterator()

	for !mapIter.Done() {
		key, value, err := mapIter.Next()
		if err != nil {
			return err
		}

		// Keys are always store as string on key value
		ks, err := key.AsString()
		if err != nil {
			return err
		}

		// Decode to bytes before return
		kb, err := hex.DecodeString(ks)
		if err != nil {
			return err
		}

		// Do not expose meta keys
		if string(kb) == reservedNameKey {
			continue
		}

		// Call the iter function with the key and value
		if err := iterFunc(kb, value); err != nil {
			return err
		}
	}

	return nil
}

// WriteCar creates the car file
func (hc *HAMTContainer) WriteCar(writer io.Writer) error {
	hc.mutex.RLock()
	defer hc.mutex.RUnlock()

	if hc.node == nil {
		return ErrHAMTNotBuild
	}

	lnk, err := hc.GetLink()
	if err != nil {
		return err
	}

	cid, err := cid.Parse(lnk.String())
	if err != nil {
		return err
	}

	ssb := sbuilder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selector := ssb.ExploreFields(func(efsb sbuilder.ExploreFieldsSpecBuilder) {
		efsb.Insert("Links",
			ssb.ExploreIndex(1, ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))))
	}).Node()

	lsysStore := utils.ToReadStore(hc.linkSystem.StorageReadOpener)
	sc := gocar.NewSelectiveCar(context.Background(), lsysStore, []gocar.Dag{{Root: cid, Selector: selector}})

	blockCount := 0
	var oneStepBlocks []gocar.Block
	return sc.Write(writer, func(block gocar.Block) error {
		oneStepBlocks = append(oneStepBlocks, block)
		blockCount++
		return nil
	})
}
