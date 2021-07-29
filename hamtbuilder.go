package hamtcontainer

import (
	"github.com/ipfs/go-cid"
	hamt "github.com/ipld/go-ipld-adl-hamt"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"github.com/simplecoincom/go-ipld-adl-hamt-container/storage"
)

type HAMTBuilder interface {
	// KeyAsByte sets the identification of the ipld-adl-hamt, it's like id, but there's no guarantee to have a
	// nested ipld-adl-hamt with the same key
	Key(key []byte) HAMTBuilder
	// AutoBuild if true the auto build is active, if active it's not required to call Build manually
	// It will build the ipld-adl-hamt automatically when needed
	AutoBuild(autoBuild bool) HAMTBuilder
	// Storage sets the underline storage used to store ipld-adl-hamt structure
	Storage(storage storage.Storage) HAMTBuilder
	// FromNested creates a new HAMTContainer from parent HAMTContainer
	FromNested(parent HAMTContainer) HAMTBuilder
	// Build creates the HAMTContainer based on the parameters passed on builder functions
	Build() (HAMTContainer, error)
}

type hamtBuilder struct {
	HAMTContainerParams
}

func NewHAMTBuilder() HAMTBuilder {
	return &hamtBuilder{}
}

func (hb *hamtBuilder) Key(key []byte) HAMTBuilder {
	hb.key = key
	return hb
}

func (hb *hamtBuilder) AutoBuild(autoBuild bool) HAMTBuilder {
	hb.isAutoBuild = autoBuild
	return hb
}

func (hb *hamtBuilder) Storage(storage storage.Storage) HAMTBuilder {
	hb.storage = storage
	return hb
}

func (hb *hamtBuilder) FromNested(parent HAMTContainer) HAMTBuilder {
	hb.parentHAMTContainer = parent
	return hb
}

func (hb *hamtBuilder) parseParamRules() error {
	if len(hb.key) == 0 {
		hb.key = []byte("hamt")
	}

	if hb.storage == nil {
		hb.storage = storage.NewMemoryStorage()
	}

	return nil
}

func (hb hamtBuilder) Build() (HAMTContainer, error) {
	if err := hb.parseParamRules(); err != nil {
		return nil, err
	}

	newHAMTContainer := hamtContainer{
		HAMTContainerParams: HAMTContainerParams{
			key:         hb.key,
			isAutoBuild: hb.isAutoBuild,
			storage:     hb.storage,
		},
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

	var err error

	// Sets the assembler to build the k/v for the map structure
	newHAMTContainer.assembler, err = newHAMTContainer.builder.BeginMap(0)
	if err != nil {
		return nil, err
	}

	if hb.parentHAMTContainer != nil {
		link, err := hb.parentHAMTContainer.GetAsLink(hb.key)
		if err != nil {
			return nil, err
		}

		if err := newHAMTContainer.LoadLink(link); err != nil {
			return nil, err
		}

		newHAMTContainer.isBuild = true
		newHAMTContainer.isLoaded = true
	}

	return &newHAMTContainer, nil
}
