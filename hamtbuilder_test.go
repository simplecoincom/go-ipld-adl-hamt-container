package hamtcontainer

import (
	"testing"

	ipld "github.com/ipld/go-ipld-prime"
	"github.com/simplecoincom/go-ipld-adl-hamt-container/storage"
	"github.com/stretchr/testify/assert"
)

func TestBuilderWithNoParams(t *testing.T) {
	assert := assert.New(t)
	builder := NewHAMTBuilder()

	hamtContainer, err := builder.Build()
	assert.Nil(err)
	assert.NotNil(hamtContainer)
	assert.Equal(string(hamtContainer.Key()), "hamt")
}

func TestBuilderWithKeyParam(t *testing.T) {
	assert := assert.New(t)
	builder := NewHAMTBuilder().Key([]byte("root"))
	hamtContainer, err := builder.Build()
	assert.Nil(err)
	assert.Equal(string(hamtContainer.Key()), "root")
}

func TestBuilderWithStorageParam(t *testing.T) {
	assert := assert.New(t)
	store := storage.NewMemoryStorage()
	builder := NewHAMTBuilder().Storage(store)
	hamtContainer, err := builder.Build()
	assert.Nil(err)
	assert.Equal(hamtContainer.Storage(), store)
}

func TestBuilderWithParentParam(t *testing.T) {
	assert := assert.New(t)
	store := storage.NewMemoryStorage()

	childContainer, err := NewHAMTBuilder().Storage(store).Key([]byte("child")).Build()
	assert.Nil(err)
	assert.NotNil(childContainer)

	assert.Nil(childContainer.Must(func(assembler ipld.MapAssembler) error {
		return childContainer.Set(assembler, []byte("foo"), "bar")
	}))

	parentContainer, err := NewHAMTBuilder().Storage(store).Key([]byte("parent")).Build()
	assert.Nil(err)
	assert.NotNil(parentContainer)

	assert.Nil(parentContainer.Must(func(assembler ipld.MapAssembler) error {
		return parentContainer.Set(assembler, []byte("child"), childContainer)
	}))

	newContainer, err := NewHAMTBuilder().Key([]byte("child")).FromNested(parentContainer).Build()
	assert.Nil(err)
	assert.NotNil(newContainer)
}
