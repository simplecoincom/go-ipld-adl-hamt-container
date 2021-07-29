package hamtcontainer

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/simplecoincom/go-ipld-adl-hamt-container/storage"
)

func TestBuilderWithNoParams(t *testing.T) {
	builder := NewHAMTBuilder()
	hamtContainer, err := builder.Build()
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, string(hamtContainer.Key()), qt.Equals, "hamt")
}

func TestBuilderWithKeyParam(t *testing.T) {
	builder := NewHAMTBuilder().Key([]byte("root"))
	hamtContainer, err := builder.Build()
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, string(hamtContainer.Key()), qt.Equals, "root")
}

func TestBuilderWithStorageParam(t *testing.T) {
	store := storage.NewMemoryStorage()
	builder := NewHAMTBuilder().Storage(store)
	hamtContainer, err := builder.Build()
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, hamtContainer.Storage(), qt.Equals, store)
}

func TestBuilderWithParentParam(t *testing.T) {
	store := storage.NewMemoryStorage()

	childContainer, err := NewHAMTBuilder().Storage(store).Key([]byte("child")).Build()
	qt.Assert(t, err, qt.IsNil)

	parentContainer, err := NewHAMTBuilder().Storage(store).Key([]byte("parent")).Build()
	qt.Assert(t, err, qt.IsNil)

	err = parentContainer.Set([]byte("child"), childContainer)
	qt.Assert(t, err, qt.IsNil)

	newContainer, err := NewHAMTBuilder().Key([]byte("child")).FromNested(parentContainer).Build()
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, newContainer, qt.IsNotNil)
	qt.Assert(t, newContainer.Parent(), qt.Equals, parentContainer)
}
