package hamtcontainer

import (
	"encoding/hex"
	"errors"

	"github.com/ipfs/go-cid"
	hamt "github.com/ipld/go-ipld-adl-hamt"
	ipld "github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/simplecoincom/go-ipld-adl-hamt-container/storage"
	"github.com/simplecoincom/go-ipld-adl-hamt-container/utils"

	"github.com/multiformats/go-multicodec"
)

var ErrHAMTNotBuild = errors.New("HAMT not ready, build first")
var ErrHAMTValueNotFound = errors.New("Value not found at HAMT")
var ErrHAMTAlreadyBuild = errors.New("HAMT already build")
var ErrHAMTUnsupportedValueType = errors.New("Value type not supported")
var ErrHAMTUnsupportedKeyType = errors.New("Key type not supported")
var ErrHAMTFailedToGetAsLink = errors.New("Value returned should ipld.Link")

// HAMTContainer is just a way to put together all HAMT needed structures in order to create a ipld.Node
// which will represent the ipld-adl-hamt after the builder runs
type HAMTContainer interface {
	// Isbuild returns true if the under ipld-adl-hamt is build, it means that there's pld.Node
	// representing the ipld-adl-hamt ready to be used with CID and Link
	IsBuild() bool
	// Key returns the identification of the ipld-adl-hamt, it's like id, but there's no guarantee to have a
	// nested ipld-adl-hamt with the same key
	Key() []byte
	// Storage returns the underline storage used to store ipld-adl-hamt structure
	Storage() storage.Storage
	// CID will return the unique content id, only available after Build
	CID() (cid.Cid, error)
	// GetLink returns the ipld.Link representation for the node ipld-adl-hamt
	GetLink() (ipld.Link, error)
	// LoadLink will receive a ipld.Link from another ipld-adl-hamt and try to load it
	// It will return and error if the process of load failed
	LoadLink(link ipld.Link) error
	// Set will receive a key value params and store the values into the ipld-adl-hamt structure
	// It can return an error if the params types aren't supported
	Set(key interface{}, value interface{}) error
	// Get will return the value for the given key
	// It can return an error if the param types aren't supported or key doesn't exists
	Get(key interface{}) (interface{}, error)
	// GetAsLink is helper for return a typed ipld.Link value from a given key
	// It can return an error if the value isn't a link or if the given key doesn't exists
	GetAsLink(key interface{}) (ipld.Link, error)
	// GetAsBytes is helper for return a typed []byte value from a given key
	// It can return an error if the value isn't compatible with []byte or if the given key doesn't exists
	GetAsBytes(key interface{}) ([]byte, error)
	// GetAsString is helper for return a typed string value from a given key
	// It can return an error if the value isn't a compatible with string or if the given key doesn't exists
	GetAsString(key interface{}) (string, error)
	// Iterator is a helper method to return the ipld.MapIterator (ipld-adl-hamt implementation for more info)
	Iterator() ipld.MapIterator
	// View helps to iterate on the keys and values available
	// It can return an error if something goes wrong internally
	// It also can return an error if the iterator function returns an error too
	// If something happpens it should
	View(iterFunc func(key interface{}, value interface{}) error) error
	// Build will create the ipld.Node representing the ipld-adl-hamt structure
	// It important to build before create link and cid
	// Becasue the ipld-adl-hamt is a immutable structure, the Build method should be called
	// Everytime that some key/value is added using Set method
	Build() error
}

type hamtContainer struct {
	key        []byte
	storage    storage.Storage
	linkSystem ipld.LinkSystem
	linkProto  ipld.LinkPrototype
	assembler  ipld.MapAssembler
	node       ipld.Node
	link       ipld.Link
	cid        cid.Cid
	builder    *hamt.Builder
	isBuild    bool
}

// NewHAMTContainer creates a new HAMTContainer
func NewHAMTContainer(key interface{}, storage storage.Storage) (HAMTContainer, error) {
	var err error
	var typedKey []byte

	// Supported key types for HAMTContainer key
	switch k := key.(type) {
	case string:
		typedKey, err = hex.DecodeString(k)
		if err != nil {
			// Let's try pure string to byte
			typedKey = []byte(k)
		}
	case []byte:
		typedKey = k
	default:
		return nil, ErrHAMTUnsupportedKeyType
	}

	newHAMTContainer := hamtContainer{
		key:     typedKey,
		storage: storage,
	}

	// Sets the link system
	newHAMTContainer.linkSystem = cidlink.DefaultLinkSystem()
	newHAMTContainer.linkProto = cidlink.LinkPrototype{Prefix: cid.Prefix{
		Version:  1, // Usually '1'.
		Codec:    uint64(multicodec.DagCbor),
		MhType:   uint64(multicodec.Sha2_512),
		MhLength: 64, // sha2-512 hash has a 64-byte sum.
	}}

	// Sets the writer and reader interfaces for the link system
	newHAMTContainer.linkSystem.StorageWriteOpener = newHAMTContainer.storage.OpenWrite
	newHAMTContainer.linkSystem.StorageReadOpener = newHAMTContainer.storage.OpenRead

	// Creates the builder for the HAMT
	newHAMTContainer.builder = hamt.NewBuilder(hamt.Prototype{BitWidth: 3, BucketSize: 64}).
		WithLinking(newHAMTContainer.linkSystem, newHAMTContainer.linkProto)

	// Sets the assembler to build the k/v for the map structure
	newHAMTContainer.assembler, err = newHAMTContainer.builder.BeginMap(0)
	if err != nil {
		return nil, err
	}

	return &newHAMTContainer, nil
}

// NewHAMTContainerFromLink creates a new HAMTContainer from a link
func NewHAMTContainerFromLink(key interface{}, storage storage.Storage, link ipld.Link) (HAMTContainer, error) {
	var err error
	var typedKey []byte

	// Supported key types for HAMTContainer key
	switch k := key.(type) {
	case string:
		typedKey, err = hex.DecodeString(k)
		if err != nil {
			// Let's try pure string to byte
			typedKey = []byte(k)
		}
	case []byte:
		typedKey = k
	default:
		return nil, ErrHAMTUnsupportedKeyType
	}

	newHAMTContainer := hamtContainer{
		key:     typedKey,
		storage: storage,
	}

	// Sets the link system
	newHAMTContainer.linkSystem = cidlink.DefaultLinkSystem()
	newHAMTContainer.linkProto = cidlink.LinkPrototype{Prefix: cid.Prefix{
		Version:  1, // Usually '1'.
		Codec:    uint64(multicodec.DagCbor),
		MhType:   uint64(multicodec.Sha2_512),
		MhLength: 64, // sha2-512 hash has a 64-byte sum.
	}}

	// Sets the writer and reader interfaces for the link system
	newHAMTContainer.linkSystem.StorageWriteOpener = newHAMTContainer.storage.OpenWrite
	newHAMTContainer.linkSystem.StorageReadOpener = newHAMTContainer.storage.OpenRead

	// Creates the builder for the HAMT
	newHAMTContainer.builder = hamt.NewBuilder(hamt.Prototype{BitWidth: 3, BucketSize: 64}).
		WithLinking(newHAMTContainer.linkSystem, newHAMTContainer.linkProto)

	// Sets the assembler to build the k/v for the map structure
	newHAMTContainer.assembler, err = newHAMTContainer.builder.BeginMap(0)
	if err != nil {
		return nil, err
	}

	if err := newHAMTContainer.LoadLink(link); err != nil {
		return nil, err
	}

	return &newHAMTContainer, nil
}

// NewHAMTContainerFromNested creates a new HAMTContainer from parent HAMTContainer
func NewHAMTContainerFromNested(key interface{}, parentHAMTContainer HAMTContainer) (HAMTContainer, error) {
	var err error

	link, err := parentHAMTContainer.GetAsLink(key)
	if err != nil {
		return nil, err
	}

	newHAMTContainer, err := NewHAMTContainerFromLink(key, parentHAMTContainer.Storage(), link)
	if err != nil {
		return nil, err
	}

	return newHAMTContainer, nil
}

// IsBuild returns if the HAMT is build
func (hc hamtContainer) IsBuild() bool {
	return hc.isBuild
}

// Key returns the key that identifies the HAMT
func (hc hamtContainer) Key() []byte {
	return hc.key
}

// Storage returns the linking storage used by the HAMT
func (hc hamtContainer) Storage() storage.Storage {
	return hc.storage
}

// CID will return the cid.Cid for the ipld.Node
// Or it will return an error if the ipld.Node for the HAMT isn't built
func (hc hamtContainer) CID() (cid.Cid, error) {
	if !hc.isBuild {
		return cid.Cid{}, ErrHAMTNotBuild
	}

	return hc.cid, nil
}

// GetLink will return the ipld.Link for the ipld.Node
// Or it will return an error if the ipld.Node for the HAMT isn't built
func (hc hamtContainer) GetLink() (ipld.Link, error) {
	if !hc.isBuild {
		return nil, ErrHAMTNotBuild
	}

	return hc.link, nil
}

// LoadLink will load the storage data from a new HAMTContainer
// Or it illl return and error if the load failed
func (hc *hamtContainer) LoadLink(link ipld.Link) error {
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

	hc.cid, err = cid.Parse(link.String())
	if err != nil {
		return err
	}

	hc.isBuild = true

	return nil
}

// Set adds a new k/v content for the HAMT
// For string values it will add k/v pair of strings
// For ipld.Link values it will add string key and a link for another HAMT structure as value
func (hc *hamtContainer) Set(key interface{}, value interface{}) error {
	var err error

	// Support types for key
	// The final result should be always a string
	switch k := key.(type) {
	case string:
		err := hc.assembler.AssembleKey().AssignString(k)
		if err != nil {
			return err
		}
	case []byte:
		err = hc.assembler.AssembleKey().AssignString(hex.EncodeToString([]byte(k)))
		if err != nil {
			return err
		}
	default:
		return ErrHAMTUnsupportedKeyType
	}

	// Support types for value
	switch v := value.(type) {
	case string:
		err = hc.assembler.AssembleValue().AssignString(v)
		if err != nil {
			return err
		}
	case []byte:
		err = hc.assembler.AssembleValue().AssignBytes(v)
		if err != nil {
			return err
		}
	case ipld.Link:
		err = hc.assembler.AssembleValue().AssignLink(v)
		if err != nil {
			return err
		}
	case hamtContainer:
		link, err := v.GetLink()
		if err != nil {
			return err
		}

		err = hc.assembler.AssembleValue().AssignLink(link)
		if err != nil {
			return err
		}
	case HAMTContainer:
		link, err := v.GetLink()
		if err != nil {
			return err
		}

		err = hc.assembler.AssembleValue().AssignLink(link)
		if err != nil {
			return err
		}
	default:
		return ErrHAMTUnsupportedValueType
	}

	// Something is added the ipld-adl-hamt should be build again
	// Consider this value a dirty state flag
	hc.isBuild = false

	return nil
}

// Get will return the value by the key
// It will return error if the hamt not build or if the value not found
func (hc hamtContainer) Get(key interface{}) (interface{}, error) {
	if !hc.isBuild {
		return nil, ErrHAMTNotBuild
	}

	var err error
	var typedKey string

	// Support types for key
	// The final value will be always a string
	switch k := key.(type) {
	case string:
		typedKey = k
	case []byte:
		typedKey = hex.EncodeToString(k)
	default:
		return nil, ErrHAMTUnsupportedKeyType
	}

	valNode, err := hc.node.LookupByString(typedKey)
	if err != nil {
		return nil, err
	}

	if valNode == nil {
		return nil, ErrHAMTValueNotFound
	}

	return utils.NodeValue(valNode)
}

// GetAsLink returns a ipld.Link type by key
// The method will fail if the returned type isn't of type ipld.Link
func (hc hamtContainer) GetAsLink(key interface{}) (ipld.Link, error) {
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
func (hc hamtContainer) GetAsBytes(key interface{}) ([]byte, error) {
	result, err := hc.Get(key)
	if err != nil {
		return nil, err
	}

	switch result.(type) {
	case []byte:
		return result.([]byte), nil
	default:
		return nil, ErrHAMTFailedToGetAsLink
	}
}

// GetAsString returns a string type by key
// The method will fail if the returned type isn't of type string or failed to convert to string
func (hc hamtContainer) GetAsString(key interface{}) (string, error) {
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
		return "", ErrHAMTFailedToGetAsLink
	}
}

// Iterator will create a map iterator to iterate over keys of the hamt
func (hc hamtContainer) Iterator() ipld.MapIterator {
	return hc.node.MapIterator()
}

// View will iterate over each item key map
func (hc hamtContainer) View(iterFunc func(key interface{}, value interface{}) error) error {
	iter := hc.Iterator()
	for !iter.Done() {
		k, v, err := iter.Next()
		if err != nil {
			return err
		}

		err = iterFunc(k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

// Build will build the HAMT internal representation inside the container
// It will have Link, and ipld.Node representing the HAMT also CID for the root content
func (hc *hamtContainer) Build() error {
	if hc.isBuild {
		return ErrHAMTAlreadyBuild
	}

	err := hc.assembler.Finish()
	if err != nil {
		return err
	}

	hc.node = hamt.Build(hc.builder)

	link, err := hc.linkSystem.Store(
		ipld.LinkContext{},
		hc.linkProto,
		hc.node,
	)
	if err != nil {
		return err
	}

	hc.link = link

	hc.cid, err = cid.Parse(link.String())
	if err != nil {
		return err
	}

	hc.isBuild = true

	return err
}
