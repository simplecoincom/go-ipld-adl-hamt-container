package hamtcontainer

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	ipld "github.com/ipld/go-ipld-prime"
	"github.com/simplecoincom/go-ipld-hamt-container/storage"
)

func TestHAMTContainerWithString(t *testing.T) {
	store := storage.NewMemoryStorage()

	// Create the first HAMT
	rootHAMT, err := NewHAMTContainer("root", store)
	qt.Assert(t, err, qt.IsNil)

	// Set some k/v
	qt.Assert(t, rootHAMT.Set("foo", "bar"), qt.IsNil)

	// Build the HAMT Node
	qt.Assert(t, rootHAMT.Build(), qt.IsNil)

	// Get node value as string
	val, err := rootHAMT.GetAsString("foo")
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, val, qt.Equals, "bar")

	// Get a non existing nod value
	val, err = rootHAMT.GetAsString("non-existing-key")
	qt.Assert(t, err, qt.Not(qt.IsNil))
	qt.Assert(t, errors.Is(err, ErrHAMTValueNotFound), qt.IsTrue)
	qt.Assert(t, val, qt.Equals, "")
}

func TestHAMTContainerViewIterator(t *testing.T) {
	store := storage.NewMemoryStorage()

	// Create the first HAMT
	rootHAMT, err := NewHAMTContainer("root", store)
	qt.Assert(t, err, qt.IsNil)

	// Set some k/v
	qt.Assert(t, rootHAMT.Set("foo", "bar"), qt.IsNil)
	qt.Assert(t, rootHAMT.Set("zoo", "zar"), qt.IsNil)

	// Build the HAMT Node
	qt.Assert(t, rootHAMT.Build(), qt.IsNil)

	// Get node value as string
	val, err := rootHAMT.GetAsString("foo")
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, val, qt.Equals, "bar")

	// View iterator
	err = rootHAMT.View(func(key, value interface{}) error {
		k, err := key.(ipld.Node).AsString()
		qt.Assert(t, err, qt.IsNil)
		qt.Assert(t, []string{"foo", "zoo"}, qt.Any(qt.Equals), k)

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
	rootHAMT, err := NewHAMTContainer([]byte("root"), store)
	qt.Assert(t, err, qt.IsNil)

	// Set some k/v
	qt.Assert(t, rootHAMT.Set([]byte("foo"), []byte("bar")), qt.IsNil)

	// Build the HAMT Node
	qt.Assert(t, rootHAMT.Build(), qt.IsNil)

	val, err := rootHAMT.GetAsBytes([]byte("foo"))
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, string(val), qt.Equals, "bar")

	val, err = rootHAMT.GetAsBytes([]byte("zoo"))
	qt.Assert(t, err, qt.Not(qt.IsNil))
	qt.Assert(t, val, qt.IsNil)
}

func TestNestedHAMTContainer(t *testing.T) {
	store := storage.NewMemoryStorage()

	// Create the first HAMT
	childHAMT, err := NewHAMTContainer("child", store)
	qt.Assert(t, err, qt.IsNil)

	// Set some k/v
	err = childHAMT.Set("foo", "bar")
	qt.Assert(t, err, qt.IsNil)

	// Build the HAMT Node
	err = childHAMT.Build()
	qt.Assert(t, err, qt.IsNil)

	// Creates the parent HAMT
	parentHAMT, err := NewHAMTContainer("parent", store)
	qt.Assert(t, err, qt.IsNil)

	// Put the child HAMT as values of the parent HAMT
	err = parentHAMT.Set("parent", childHAMT)
	qt.Assert(t, err, qt.IsNil)

	// Should fail to because parent isn't build
	_, err = NewHAMTContainerFromNested("parent", parentHAMT)
	qt.Assert(t, err, qt.IsNotNil)
	qt.Assert(t, errors.Is(err, ErrHAMTNotBuild), qt.IsTrue)

	// Build the parent node
	err = parentHAMT.Build()
	qt.Assert(t, err, qt.IsNil)

	// Load nested HAMT from parent HAMT
	newHC, err := NewHAMTContainerFromNested("parent", parentHAMT)
	qt.Assert(t, err, qt.IsNil)

	// Get value as string
	val, err := newHC.GetAsString("foo")
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, val, qt.Equals, "bar")
}
