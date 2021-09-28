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
	rootHAMT, err := NewHAMTBuilder(WithKey([]byte("root"))).Build()
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
	rootHAMT, err := NewHAMTBuilder(
		WithKey([]byte("root")),
		WithStorage(store),
	).Build()
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
	rootHAMT, err := NewHAMTBuilder(
		WithKey([]byte("root")),
		WithStorage(store)).Build()
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

func TestHAMTContainerWithCachedKV(t *testing.T) {
	assert := assert.New(t)
	store := storage.NewMemoryStorage()

	// Create the first HAMT
	rootHAMT, err := NewHAMTBuilder(
		WithKey([]byte("root")),
		WithStorage(store),
	).Build()
	assert.Nil(err)

	// Added to cached k/v to be build later
	rootHAMT.Set([]byte("zoo"), []byte("zar"))

	// Set some k/v
	assert.Nil(rootHAMT.MustBuild(func(hamtSetter HAMTSetter) error {
		return hamtSetter.Set([]byte("foo"), []byte("bar"))
	}))

	// Get node value as string
	val, err := rootHAMT.GetAsBytes([]byte("foo"))
	assert.Nil(err)
	assert.Equal(string(val), "bar")

	// Get node cached value as string
	val, err = rootHAMT.GetAsBytes([]byte("zoo"))
	assert.Nil(err)
	assert.Equal(string(val), "zar")

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
	rootHAMT, err := NewHAMTBuilder(
		WithKey([]byte("root")),
		WithStorage(store),
	).Build()
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
	newHC, err := NewHAMTBuilder(
		WithKey([]byte("root")),
		WithStorage(store),
		WithLink(lnk),
	).Build()
	assert.Nil(err)

	// Shoud rebuild prev values too
	assert.Nil(newHC.MustBuild())

	// Get node value as string
	val2, err := newHC.GetAsBytes([]byte("foo"))
	assert.Nil(err)
	assert.Equal(string(val2), "bar")

	assert.Equal("root", string(newHC.Key()))
}

func TestUpdateHAMTContainer(t *testing.T) {
	assert := assert.New(t)
	store := storage.NewMemoryStorage()

	// Create the first HAMT
	hamt, err := NewHAMTBuilder(
		WithKey([]byte("first")),
		WithStorage(store),
	).Build()
	assert.NotNil(hamt)
	assert.Nil(err)

	// Set some k/v
	assert.Nil(hamt.MustBuild(func(hamtSetter HAMTSetter) error {
		return hamtSetter.Set([]byte("foo"), "bar")
	}))

	l1, err := hamt.GetLink()
	assert.Nil(err)
	assert.Equal("bafyrgqb5ccumyuwhlsulrr5yphx3t2fmd3dobftetwf4wk3f4twnkwj7kwhhwcs54b4tpfysgl6sefp4x2habf3oqnbtfqcshfkeod2s3ct3k", l1.String())

	// Set some k/v
	assert.Nil(hamt.MustBuild(func(hamtSetter HAMTSetter) error {
		return hamtSetter.Set([]byte("zoo"), "zar")
	}))

	l2, err := hamt.GetLink()
	assert.Nil(err)
	assert.NotEqual("bafyrgqb5ccumyuwhlsulrr5yphx3t2fmd3dobftetwf4wk3f4twnkwj7kwhhwcs54b4tpfysgl6sefp4x2habf3oqnbtfqcshfkeod2s3ct3k", l2.String())

	s1, err := hamt.GetAsString([]byte("foo"))
	assert.Nil(err)
	assert.Equal(s1, "bar")

	s2, err := hamt.GetAsString([]byte("zoo"))
	assert.Nil(err)
	assert.Equal(s2, "zar")

	l3, err := hamt.GetLink()
	assert.Nil(err)
	assert.Equal("bafyrgqbhi5gpyypniliixeboianpfu7wqfp2w7mhstbzl2k72vya7kuj7nvwtdbaplkvadv5w5c4ywjnkofxpyuav7jeb6sewuww7b4k5qi64", l3.String())

	// Set some k/v
	assert.Nil(hamt.MustBuild(func(hamtSetter HAMTSetter) error {
		return hamtSetter.Set([]byte("zoo"), "zoor")
	}))

	s3, err := hamt.GetAsString([]byte("zoo"))
	assert.Nil(err)
	assert.Equal(s3, "zoor")

	l4, err := hamt.GetLink()
	assert.Nil(err)
	assert.Equal("bafyrgqg3c2hkug64cdlotx2yaxdekjx2s7rxjn734a2ohwfwzwwt5me3aqsv6skoyeksgi7iuecdzkrx6z37l7m73zwrurz2z644cyl35a4qe", l4.String())
}

func TestNestedHAMTContainer(t *testing.T) {
	assert := assert.New(t)
	store := storage.NewMemoryStorage()

	// Create the first HAMT
	childHAMT, err := NewHAMTBuilder(
		WithKey([]byte("child")),
		WithStorage(store),
	).Build()
	assert.NotNil(childHAMT)
	assert.Nil(err)

	// Set some k/v
	assert.Nil(childHAMT.MustBuild(func(hamtSetter HAMTSetter) error {
		return hamtSetter.Set([]byte("foo"), "bar")
	}))

	// Creates the parent HAMT
	parentHAMT, err := NewHAMTBuilder(
		WithKey([]byte("parent")),
		WithStorage(store),
	).Build()
	assert.NotNil(parentHAMT)
	assert.Nil(err)

	// Put the child HAMT as values of the parent HAMT
	assert.Nil(parentHAMT.MustBuild(func(hamtSetter HAMTSetter) error {
		return hamtSetter.Set([]byte("child"), childHAMT)
	}))

	// Load nested HAMT from parent HAMT
	newHC, err := NewHAMTBuilder(
		WithKey([]byte("child")),
		WithHAMTContainer(parentHAMT),
	).Build()
	assert.Nil(err)
	assert.NotNil(newHC)

	// Get value as string
	val, err := newHC.GetAsString([]byte("foo"))
	assert.Nil(err)
	assert.Equal(val, "bar")
}
