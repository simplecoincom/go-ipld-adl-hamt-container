# go-ipld-adl-hamt-container

> go-ipld-adl-hamt-container is a wrapper around `go-ipld-adl-hamt` project

## Creating HAMT container

```go
package main

import (
	"fmt"

	hamtcontainer "github.com/simplecoincom/go-ipld-adl-hamt-container"
)

func main() {
	// Create the root HAMT container
	rootHAMT, err := hamtcontainer.NewHAMTBuilder().Key([]byte("root")).Build()
	if err != nil {
		panic(err)
	}

	// Let's add a key
	err = rootHAMT.Set([]byte("foo"), "bar")
	if err != nil {
		panic(err)
	}

	// Retrieve the value with key
	val, err := rootHAMT.GetAsString([]byte("foo"))
	if err != nil {
		panic(err)
	}

	fmt.Println(val) // bar
}
```

## Linking container with Redis

```go
func main() {
	// Linking the data into a Redis storage
	store := storage.NewRedisStorage("localhost:6379", "")

	// Create the root HAMT container
	rootHAMT, err := hamtcontainer.NewHAMTBuilder().Key([]byte("root")).Storage(store).Build()
	if err != nil {
		panic(err)
	}

	// Let's add a key
	err = rootHAMT.Set([]byte("foo"), "bar")
	if err != nil {
		panic(err)
	}

	// Retrieve the value with key
	val, err := rootHAMT.GetAsString([]byte("foo"))
	if err != nil {
		panic(err)
	}

	fmt.Println(val) // bar
}
```

## Nested HAMT containers

```go
func main() {
 	store := storage.NewMemoryStorage()

	// Create the child HAMT container
	childHAMT, err := hamtcontainer.NewHAMTBuilder().Key([]byte("child")).Storage(store).Build()
	if err != nil {
		panic(err)
	}

	// Let's add a key
	err = childHAMT.Set([]byte("foo"), "zar")
	if err != nil {
		panic(err)
	}

	// Create the root HAMT container
	parentHAMT, err := hamtcontainer.NewHAMTBuilder().Key([]byte("parent")).Storage(store).Build()
	if err != nil {
		panic(err)
	}

	// Adds the child container to root
	err = parentHAMT.Set([]byte("child"), childHAMT)
	if err != nil {
		panic(err)
	}

	// Load child HAMT container
	newChildHAMT, err := hamtcontainer.NewHAMTBuilder().Key([]byte("child")).FromNested(parentHAMT).Build()
	if err != nil {
		panic(err)
	}

	// Retrieve the value with key
	val, err := newChildHAMT.GetAsString([]byte("foo"))
	if err != nil {
		panic(err)
	}

	fmt.Println(val) // zar
}
```
