package hamtcontainer

import (
	"testing"

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

	assert.Nil(childContainer.MustBuild(func(hamtSetter HAMTSetter) error {
		return hamtSetter.Set([]byte("foo"), "bar")
	}))

	parentContainer, err := NewHAMTBuilder().Storage(store).Key([]byte("parent")).Build()
	assert.Nil(err)
	assert.NotNil(parentContainer)

	assert.Nil(parentContainer.MustBuild(func(hamtSetter HAMTSetter) error {
		return hamtSetter.Set([]byte("child"), childContainer)
	}))

	newContainer, err := NewHAMTBuilder().Key([]byte("child")).FromNested(parentContainer).Build()
	assert.Nil(err)
	assert.NotNil(newContainer)
}
