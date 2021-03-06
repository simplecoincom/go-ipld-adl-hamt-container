package hamtcontainer

import (
	"github.com/pkg/errors"

	"github.com/ipfs/go-cid"
	ipld "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"github.com/simplecoincom/go-ipld-adl-hamt-container/storage"
)

var ErrCantUseStorageAndNested = errors.New("Cannot use Storage and FromNested in the same build")
var ErrCantUseParentAndLink = errors.New("Cannot use Parant and Link in the same build")

type Option func(*HAMTBuilder)

type HAMTBuilder struct {
	key                 []byte
	storage             storage.Storage
	link                ipld.Link
	parentHAMTContainer *HAMTContainer
}

// NewHAMTBuilder create a new HAMTBuilder helper
func NewHAMTBuilder(options ...Option) *HAMTBuilder {
	hamtBuilder := &HAMTBuilder{}
	for _, opt := range options {
		opt(hamtBuilder)
	}
	return hamtBuilder
}

// WithKey sets the key for the future HAMTContainer
func WithKey(key []byte) Option {
	return func(h *HAMTBuilder) {
		h.key = key
	}
}

// WithStorage sets the storage for the future HAMTContainer
func WithStorage(storage storage.Storage) Option {
	return func(h *HAMTBuilder) {
		h.storage = storage
	}
}

// WithLink sets the link for the future HAMTContainer
func WithLink(link ipld.Link) Option {
	return func(h *HAMTBuilder) {
		h.link = link
	}
}

// WithHAMTContainer sets the parent container to load from the future HAMTContainer
func WithHAMTContainer(hamtContainer *HAMTContainer) Option {
	return func(h *HAMTBuilder) {
		h.parentHAMTContainer = hamtContainer
	}
}

func (hb *HAMTBuilder) parseParamRules() error {
	// Should parse params and helps with some rules

	// Storage and parent Storage should not be use in the same time
	// Because it should get the same storage from the parent
	if hb.storage != nil && hb.parentHAMTContainer != nil {
		return ErrCantUseStorageAndNested
	}

	// Not key provided, it cab be just "hamt"
	if len(hb.key) == 0 {
		hb.key = []byte("hamt")
	}

	// If storage and parent are nil, we can use the default memory storage
	if hb.storage == nil && hb.parentHAMTContainer == nil {
		hb.storage = storage.NewMemoryStorage()
	}

	// If link and parent container
	if hb.parentHAMTContainer != nil && hb.link != nil {
		return ErrCantUseParentAndLink
	}

	// If parent isn't nil then we should use it storage
	if hb.parentHAMTContainer != nil {
		hb.storage = hb.parentHAMTContainer.Storage()
	}

	return nil
}

// Build creates the HAMT Container based on the params from HAMTBuilder
func (hb HAMTBuilder) Build() (*HAMTContainer, error) {
	if err := hb.parseParamRules(); err != nil {
		return nil, err
	}

	newHAMTContainer := &HAMTContainer{
		key:     hb.key,
		kvCache: make(map[string]interface{}),
		storage: hb.storage,
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

	// If has a parent we should load from it
	if hb.parentHAMTContainer != nil {

		// If the key doesn't exists we should warn
		link, err := hb.parentHAMTContainer.GetAsLink(hb.key)
		if err != nil {
			return nil, ErrHAMTNoNestedFound
		}

		// Should load link from parent
		if err := newHAMTContainer.LoadLink(link); err != nil {
			return nil, ErrHAMTFailedToLoadNested
		}
	}

	// Has a link, try to load
	if hb.link != nil {
		if err := newHAMTContainer.LoadLink(hb.link); err != nil {
			return nil, err
		}
	}

	// If has the parent container and the link we should load the key from it
	if hb.parentHAMTContainer != nil || hb.link != nil {
		key, err := newHAMTContainer.GetAsBytes([]byte(reservedNameKey))
		if err != nil {
			return nil, err
		}
		newHAMTContainer.key = key
	}

	return newHAMTContainer, nil
}
