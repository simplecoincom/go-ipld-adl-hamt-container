package hamtcontainer

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
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

var ErrHAMTNotBuild = errors.New("HAMT not ready, build first")
var ErrHAMTValueNotFound = errors.New("Value not found at HAMT")
var ErrHAMTNoNestedFound = errors.New("No nested found with the given key")
var ErrHAMTFailedToLoadNested = errors.New("Failed to load link from nested HAMT")
var ErrHAMTAlreadyBuild = errors.New("HAMT already build")
var ErrHAMTUnsupportedValueType = errors.New("Unsupported value")
var ErrHAMTUnsupportedKeyType = errors.New("Key type not supported")
var ErrHAMTFailedToGetAsLink = errors.New("Value returned should be ipld.Link")
var ErrHAMTFailedToGetAsBytes = errors.New("Value returned should be Bytes")
var ErrHAMTFailedToGetAsString = errors.New("Value returned should be String")

type HAMTContainerParams struct {
	key                 []byte
	storage             storage.Storage
	link                ipld.Link
	parentHAMTContainer *HAMTContainer
}

type HAMTContainer struct {
	mutex      sync.RWMutex
	key        []byte
	storage    storage.Storage
	link       ipld.Link
	linkSystem ipld.LinkSystem
	linkProto  ipld.LinkPrototype
	node       ipld.Node
	cid        cid.Cid
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
	return cid.Parse(hc.link.String())
}

// GetLink will return the ipld.Link for the ipld.Node
// Or it will return an error if the ipld.Node for the HAMT isn't built
func (hc *HAMTContainer) GetLink() (ipld.Link, error) {
	hc.mutex.RLock()
	defer hc.mutex.RUnlock()
	return hc.link, nil
}

// LoadLink will load the storage data from a new HAMTContainer
// Or it illl return and error if the load failed
func (hc *HAMTContainer) LoadLink(link ipld.Link) error {
	hc.mutex.Lock()
	defer hc.mutex.Unlock()

	nodePrototye := basicnode.Prototype.Any

	node, err := hc.linkSystem.Load(
		ipld.LinkContext{}, // The zero value is fine.  Configure it it you want cancellability or other features.
		link,               // The Link we want to load!
		nodePrototye,       // The NodePrototype says what kind of Node we want as a result.
	)
	if err != nil {
		return err
	}

	hc.link = link
	hc.node = node

	return nil
}

// Must is used to build the key maps
func (hc *HAMTContainer) Must(assemblyFunc func(assembler ipld.MapAssembler) error) error {
	hc.mutex.Lock()
	defer hc.mutex.Unlock()

	// Creates the builder for the HAMT
	builder := hamt.NewBuilder(hamt.Prototype{BitWidth: 3, BucketSize: 64}).
		WithLinking(hc.linkSystem, hc.linkProto)

	assembler, err := builder.BeginMap(0)
	if err != nil {
		return err
	}

	if err := assembler.AssembleKey().AssignString(hex.EncodeToString([]byte(reservedNameKey))); err != nil {
		return err
	}

	if err := assembler.AssembleValue().AssignBytes(hc.key); err != nil {
		return err
	}

	if err := assemblyFunc(assembler); err != nil {
		return err
	}

	if err := assembler.Finish(); err != nil {
		return err
	}

	hc.node = hamt.Build(builder)

	link, err := hc.linkSystem.Store(
		ipld.LinkContext{},
		hc.linkProto,
		hc.node,
	)
	if err != nil {
		return err
	}

	hc.link = link

	return nil
}

// Set adds a new k/v content for the HAMT
// For string values it will add k/v pair of strings
// For ipld.Link values it will add string key and a link for another HAMT structure as value
func (hc *HAMTContainer) Set(assembler ipld.MapAssembler, key []byte, value interface{}) error {
	if err := assembler.AssembleKey().AssignString(hex.EncodeToString([]byte(key))); err != nil {
		return err
	}

	// Support types for value
	switch v := value.(type) {
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

	return nil
}

// Get will return the value by the key
// It will return error if the hamt not build or if the value not found
func (hc *HAMTContainer) Get(key []byte) (interface{}, error) {
	hc.mutex.Lock()
	defer hc.mutex.Unlock()

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
	hc.mutex.Lock()
	defer hc.mutex.Unlock()

	iter := hc.node.MapIterator()
	for !iter.Done() {
		k, v, err := iter.Next()
		if err != nil {
			return err
		}

		kk, err := k.AsString()
		if err != nil {
			return err
		}

		bs, err := hex.DecodeString(kk)
		if err != nil {
			return err
		}

		// Do not view meta keys
		if string(bs) == reservedNameKey {
			continue
		}

		err = iterFunc(bs, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (hc *HAMTContainer) GetCar() ([]byte, error) {
	hc.mutex.RLock()
	defer hc.mutex.RUnlock()

	ssb := sbuilder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selector := ssb.ExploreFields(func(efsb sbuilder.ExploreFieldsSpecBuilder) {
		efsb.Insert("Links",
			ssb.ExploreIndex(1, ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreAll(ssb.ExploreRecursiveEdge()))))
	}).Node()

	lnk, err := hc.GetLink()
	if err != nil {
		return nil, err
	}

	cid, err := cid.Parse(lnk.String())
	if err != nil {
		return nil, err
	}

	lsysStore := utils.ToReadStore(hc.linkSystem.StorageReadOpener)
	sc := gocar.NewSelectiveCar(context.Background(), lsysStore, []gocar.Dag{{Root: cid, Selector: selector}})

	buf := new(bytes.Buffer)
	blockCount := 0
	var oneStepBlocks []gocar.Block
	err = sc.Write(buf, func(block gocar.Block) error {
		oneStepBlocks = append(oneStepBlocks, block)
		blockCount++
		return nil
	})

	return buf.Bytes(), nil
}
