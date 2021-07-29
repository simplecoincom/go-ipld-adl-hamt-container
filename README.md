# go-ipld-adl-hamt-container

> go-ipld-adl-hamt-container is a wrapper around `go-ipld-adl-hamt` project

## Creating HAMT container

```go
package main

import (
    "fmt"

    hamtcontainer "github.com/simplecoincom/go-ipld-adl-hamt-container"
    "github.com/simplecoincom/go-ipld-adl-hamt-container/storage"
)

func main() {
    // Create the linking store
    // Every HAMT requires the link storage
    store := storage.NewMemoryStorage()

    // Create the root HAMT container
    rootHAMT, err := hamtcontainer.NewHAMTContainer("root", store)
    if err != nil {
        panic(err)
    }

    // Let's add a key
    err = rootHAMT.Set("foo", "bar")
    if err != nil {
        panic(err)
    }

    // Before use we should build the structure
    err = rootHAMT.Build()
    if err != nil {
        panic(err)
    }

    // Retrieve the value with key
    val, err := rootHAMT.GetAsString("foo")
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
    rootHAMT, err := hamtcontainer.NewHAMTContainerWithLinking("root", store)
    if err != nil {
        panic(err)
    }

    // Let's add a key
    err = rootHAMT.Set("foo", "bar")
    if err != nil {
        panic(err)
    }

    // Before use we should build the structure
    err = rootHAMT.Build()
    if err != nil {
        panic(err)
    }

    // Retrieve the value with key
    val, err := rootHAMT.GetAsString("foo")
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
    childHAMT, err := hamtcontainer.NewHAMTContainerWithLinking("child", store)
    if err != nil {
        panic(err)
    }

    // Let's add a key
    err = childHAMT.Set("foo", "zar")
    if err != nil {
        panic(err)
    }

    // Before use we should build the structure
    err = childHAMT.Build()
    if err != nil {
        panic(err)
    }

    // Create the root HAMT container
    rootHAMT, err := hamtcontainer.NewHAMTContainerWithLinking("root", store)
    if err != nil {
        panic(err)
    }

    // Adds the child container to root
    err = rootHAMT.Set("child", childHAMT)
    if err != nil {
        panic(err)
    }

    // Before use we should build the structure
    err = rootHAMT.Build()
    if err != nil {
        panic(err)
    }

    // Load child HAMT container
    newChildHAMT, err := hamtcontainer.NewHAMTContainerFromNested("child", rootHAMT)
    if err != nil {
        panic(err)
    }

    // Retrieve the value with key
    val, err := newChildHAMT.GetAsString("foo")
    if err != nil {
        panic(err)
    }

    fmt.Println(val) // zar
}
```
