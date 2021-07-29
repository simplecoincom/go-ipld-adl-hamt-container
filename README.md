# go-ipld-hamt-container

> go-ipld-hamt-container is a wrapper around `go-ipld-adl-hamt` project

## How to use

```go
package main

import (
    "github.com/simplecoincom/go-ipld-hamt-container/storage"
)

func main() {
    // Create the linking store
    store := storage.NewMemoryStorage()

    // Create the root HAMT container
    rootHAMT, err := NewHAMTContainer("root", store)
    if err != nil {
        panic(err)
    }

    // Let's add a key
    err = rootHAMT.Set("foo", "bar")
    if err != nil {
        panic(err)
    }
}
```