package hamtcontainer

import (
	"errors"
	"fmt"
	"os"
	"testing"

	ipfsApi "github.com/ipfs/go-ipfs-api"
	ipld "github.com/ipld/go-ipld-prime"
	"github.com/simplecoincom/go-ipld-adl-hamt-container/storage"
	"github.com/stretchr/testify/assert"
)

func TestHAMTContainerWithString(t *testing.T) {
	assert := assert.New(t)
	rootHAMT, err := NewHAMTBuilder().Key([]byte("root")).Build()
	assert.Nil(err)

	// Set some k/v
	assert.Nil(rootHAMT.MustBuild(func(hamtSetter HAMTSetter) error {
		return hamtSetter.Set([]byte("foo"), "bar")
	}))

	// Get node value as string
	val, err := rootHAMT.GetAsString([]byte("foo"))
	assert.Nil(err)
	assert.Equal(val, "bar")

	// Get a non existing nod value
	val, err = rootHAMT.GetAsString([]byte("non-existing-key"))
	assert.NotNil(err)
	assert.True(errors.Is(err, ErrHAMTValueNotFound))
	assert.Empty(val)
}

func TestHAMTContainerViewIterator(t *testing.T) {
	assert := assert.New(t)

	store := storage.NewMemoryStorage()

	// Create the first HAMT
	rootHAMT, err := NewHAMTBuilder().Key([]byte("root")).Storage(store).Build()
	assert.Nil(err)

	// Set some k/v
	assert.Nil(rootHAMT.MustBuild(func(hamtSetter HAMTSetter) error {
		if err := hamtSetter.Set([]byte("foo"), "bar"); err != nil {
			return err
		}

		return hamtSetter.Set([]byte("zoo"), "zar")
	}))

	// Get node value as string
	val, err := rootHAMT.GetAsString([]byte("foo"))
	assert.Nil(err)
	assert.Equal(val, "bar")

	// View iterator
	err = rootHAMT.View(func(key []byte, value interface{}) error {
		assert.Nil(err)
		assert.Contains([]string{"foo", "zoo"}, string(key))

		v, err := value.(ipld.Node).AsString()
		assert.Nil(err)
		assert.Contains([]string{"bar", "zar"}, v)
		return nil
	})
	assert.Nil(err)
}

func TestHAMTContainerWithBytes(t *testing.T) {
	assert := assert.New(t)
	store := storage.NewMemoryStorage()

	// Create the first HAMT
	rootHAMT, err := NewHAMTBuilder().Key([]byte("root")).Storage(store).Build()
	assert.Nil(err)

	// Set some k/v
	assert.Nil(rootHAMT.MustBuild(func(hamtSetter HAMTSetter) error {
		return hamtSetter.Set([]byte("foo"), []byte("bar"))
	}))

	// Get node value as string
	val, err := rootHAMT.GetAsBytes([]byte("foo"))
	assert.Nil(err)
	assert.Equal(string(val), "bar")

	val, err = rootHAMT.GetAsBytes([]byte("non-existing-key"))
	assert.NotNil(err)
	assert.Nil(val)
}

func TestHAMTContainerWithIPFS(t *testing.T) {
	assert := assert.New(t)
	ipfsURL, ok := os.LookupEnv("IPFS_URL")
	if !ok {
		// export IPFS_URL="http://localhost:5001"
		fmt.Println("Pass IPFS_URL env in order to test IPFS connection")
		return
	}

	store := storage.NewIPFSStorage(ipfsApi.NewShell(ipfsURL))

	// Create the first HAMT
	rootHAMT, err := NewHAMTBuilder().Key([]byte("root")).Storage(store).Build()
	assert.Nil(err)

	// Set some k/v
	assert.Nil(rootHAMT.MustBuild(func(hamtSetter HAMTSetter) error {
		return hamtSetter.Set([]byte("foo"), []byte("bar"))
	}))

	// Get node value as string
	val, err := rootHAMT.GetAsBytes([]byte("foo"))
	assert.Nil(err)
	assert.Equal(string(val), "bar")

	// Get HAMT link
	lnk, err := rootHAMT.GetLink()
	assert.Nil(err)

	// Load HAMT from link
	newHC, err := NewHAMTBuilder().Key([]byte("root")).Storage(store).FromLink(lnk).Build()
	assert.Nil(err)

	// Get node value as string
	val2, err := newHC.GetAsBytes([]byte("foo"))
	assert.Nil(err)
	assert.Equal(string(val2), "bar")
}

func TestNestedHAMTContainer(t *testing.T) {
	assert := assert.New(t)
	store := storage.NewMemoryStorage()

	// Create the first HAMT
	childHAMT, err := NewHAMTBuilder().Key([]byte("child")).Storage(store).Build()
	assert.NotNil(childHAMT)
	assert.Nil(err)

	// Set some k/v
	assert.Nil(childHAMT.MustBuild(func(hamtSetter HAMTSetter) error {
		return hamtSetter.Set([]byte("foo"), "bar")
	}))

	// Creates the parent HAMT
	parentHAMT, err := NewHAMTBuilder().Key([]byte("parent")).Storage(store).Build()
	assert.NotNil(parentHAMT)
	assert.Nil(err)

	// Put the child HAMT as values of the parent HAMT
	assert.Nil(parentHAMT.MustBuild(func(hamtSetter HAMTSetter) error {
		return hamtSetter.Set([]byte("child"), childHAMT)
	}))

	// Load nested HAMT from parent HAMT
	newHC, err := NewHAMTBuilder().Key([]byte("child")).FromNested(parentHAMT).Build()
	assert.Nil(err)
	assert.NotNil(newHC)

	// Get value as string
	val, err := newHC.GetAsString([]byte("foo"))
	assert.Nil(err)
	assert.Equal(val, "bar")
}
