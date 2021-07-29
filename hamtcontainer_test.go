package hamtcontainer

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	ipld "github.com/ipld/go-ipld-prime"
	"github.com/simplecoincom/go-ipld-adl-hamt-container/storage"
)

func TestHAMTContainerWithString(t *testing.T) {
	rootHAMT, err := NewHAMTBuilder().Key([]byte("root")).AutoBuild(true).Build()
	qt.Assert(t, err, qt.IsNil)

	// Set some k/v
	qt.Assert(t, rootHAMT.Set([]byte("foo"), "bar"), qt.IsNil)

	// Get node value as string
	val, err := rootHAMT.GetAsString([]byte("foo"))
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, val, qt.Equals, "bar")

	// Get a non existing nod value
	val, err = rootHAMT.GetAsString([]byte("non-existing-key"))
	qt.Assert(t, err, qt.Not(qt.IsNil))
	qt.Assert(t, errors.Is(err, ErrHAMTValueNotFound), qt.IsTrue)
	qt.Assert(t, val, qt.Equals, "")
}

func TestHAMTContainerViewIterator(t *testing.T) {
	store := storage.NewMemoryStorage()

	// Create the first HAMT
	rootHAMT, err := NewHAMTBuilder().Key([]byte("root")).Storage(store).Build()
	qt.Assert(t, err, qt.IsNil)

	// Set some k/v
	qt.Assert(t, rootHAMT.Set([]byte("foo"), "bar"), qt.IsNil)
	qt.Assert(t, rootHAMT.Set([]byte("zoo"), "zar"), qt.IsNil)

	// Build the HAMT Node
	qt.Assert(t, rootHAMT.Build(), qt.IsNil)

	// Get node value as string
	val, err := rootHAMT.GetAsString([]byte("foo"))
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, val, qt.Equals, "bar")

	// View iterator
	err = rootHAMT.View(func(key []byte, value interface{}) error {
		qt.Assert(t, err, qt.IsNil)
		qt.Assert(t, []string{"foo", "zoo"}, qt.Any(qt.Equals), string(key))

		v, err := value.(ipld.Node).AsString()
		qt.Assert(t, err, qt.IsNil)
		qt.Assert(t, []string{"bar", "zar"}, qt.Any(qt.Equals), v)
		return nil
	})
	qt.Assert(t, err, qt.IsNil)
}

func TestHAMTContainerWithBytes(t *testing.T) {
	store := storage.NewMemoryStorage()

	// Create the first HAMT
	rootHAMT, err := NewHAMTBuilder().Key([]byte("root")).Storage(store).Build()
	qt.Assert(t, err, qt.IsNil)

	// Set some k/v
	qt.Assert(t, rootHAMT.Set([]byte([]byte("foo")), []byte("bar")), qt.IsNil)

	// Build the HAMT Node
	qt.Assert(t, rootHAMT.Build(), qt.IsNil)

	val, err := rootHAMT.GetAsBytes([]byte([]byte("foo")))
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, string(val), qt.Equals, "bar")

	val, err = rootHAMT.GetAsBytes([]byte([]byte("zoo")))
	qt.Assert(t, err, qt.Not(qt.IsNil))
	qt.Assert(t, val, qt.IsNil)
}

func TestNestedHAMTContainer(t *testing.T) {
	store := storage.NewMemoryStorage()

	// Create the first HAMT
	childHAMT, err := NewHAMTBuilder().Key([]byte("child")).Storage(store).AutoBuild(true).Build()
	qt.Assert(t, err, qt.IsNil)

	// Set some k/v
	err = childHAMT.Set([]byte("foo"), "bar")
	qt.Assert(t, err, qt.IsNil)

	// Creates the parent HAMT
	parentHAMT, err := NewHAMTBuilder().Key([]byte("parent")).Storage(store).AutoBuild(true).Build()
	qt.Assert(t, err, qt.IsNil)

	// Put the child HAMT as values of the parent HAMT
	err = parentHAMT.Set([]byte("child"), childHAMT)
	qt.Assert(t, err, qt.IsNil)

	// Load nested HAMT from parent HAMT
	newHC, err := NewHAMTBuilder().Key([]byte("child")).Storage(store).AutoBuild(true).FromNested(parentHAMT).Build()
	qt.Assert(t, err, qt.IsNil)

	// Get value as string
	val, err := newHC.GetAsString([]byte("foo"))
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, val, qt.Equals, "bar")
}
