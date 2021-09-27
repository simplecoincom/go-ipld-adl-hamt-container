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
	// Create the root HAMT container
	rootHAMT, err := hamtcontainer.NewHAMTBuilder(
		hamtcontainer.WithKey([]byte("root"),
	).Build()
	if err != nil {
		panic(err)
	}

	// Let's add a key
	err = rootHAMT.MustBuild(func(hamtSetter hamtcontainer.HAMTSetter) error {
		return hamtSetter.Set([]byte("foo"), "bar")
	})
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
import (
	"fmt"

	hamtcontainer "github.com/simplecoincom/go-ipld-adl-hamt-container"
	"github.com/simplecoincom/go-ipld-adl-hamt-container/storage"
)

func main() {
	// Linking the data into a Redis storage
	store := storage.NewRedisStorage("localhost:6379", "")

	// Create the root HAMT container
	rootHAMT, err := hamtcontainer.NewHAMTBuilder(
		hamtcontainer.WithKey([]byte("root")),
		hamtcontainer.WithStorage(store),
	).Build()
	if err != nil {
		panic(err)
	}

	// Let's add a key
	err = rootHAMT.MustBuild(func(hamtSetter hamtcontainer.HAMTSetter) error {
		return hamtSetter.Set([]byte("foo"), "bar")
	})
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
import (
	"fmt"

	hamtcontainer "github.com/simplecoincom/go-ipld-adl-hamt-container"
	"github.com/simplecoincom/go-ipld-adl-hamt-container/storage"
)


func main() {
	store := storage.NewMemoryStorage()

	// Create the child HAMT container
	childHAMT, err := hamtcontainer.NewHAMTBuilder(
		hamtcontainer.WithKey([]byte("child")),
		hamtcontainer.WithStorage(store),
	).Build()
	if err != nil {
		panic(err)
	}

	// Let's add a key
	err = childHAMT.MustBuild(func(hamtSetter hamtcontainer.HAMTSetter) error {
		return hamtSetter.Set([]byte("foo"), "zar")
	})
	if err != nil {
		panic(err)
	}

	// Create the root HAMT container
	parentHAMT, err := hamtcontainer.NewHAMTBuilder(
		hamtcontainer.WithKey([]byte("parent")),
		hamtcontainer.WithStorage(store),
	).Build()
	if err != nil {
		panic(err)
	}

	// Adds the child container to root
	err = parentHAMT.MustBuild(func(hamtSetter hamtcontainer.HAMTSetter) error {
		return hamtSetter.Set([]byte("child"), childHAMT)
	})
	if err != nil {
		panic(err)
	}

	// Load child HAMT container
	newChildHAMT, err := hamtcontainer.NewHAMTBuilder(
		hamtcontainer.WithKey([]byte("child")),
		hamtcontainer.WithHAMTContainer(parentHAMT),
	).Build()
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

## Store and Load from IPFS

```go
import (
	"fmt"

	ipfsApi "github.com/ipfs/go-ipfs-api"
	hamtcontainer "github.com/simplecoincom/go-ipld-adl-hamt-container"
	"github.com/simplecoincom/go-ipld-adl-hamt-container/storage"
)

func main() {
	store := storage.NewIPFSStorage(ipfsApi.NewShell("http://localhost:5001"))

	// Create the first HAMT
	rootHAMT, err := hamtcontainer.NewHAMTBuilder(
		hamtcontainer.WithKey([]byte("root")),
		hamtcontainer.WithStorage(store),
	).Build()
	if err != nil {
		panic(err)
	}

	// Set some k/v
	err = rootHAMT.MustBuild(func(hamtSetter hamtcontainer.HAMTSetter) error {
		return hamtSetter.Set([]byte("foo"), []byte("bar"))
	})
	if err != nil {
		panic(err)
	}

	// Get HAMT link
	lnk, err := rootHAMT.GetLink()
	if err != nil {
		panic(err)
	}

	// Load HAMT from link
	newHC, err := hamtcontainer.NewHAMTBuilder(
		hamtcontainer.WithKey([]byte("root")),
		hamtcontainer.WithStorage(store),
		hamtcontainer.WithLink(lnk)
	).Build()
	if err != nil {
		panic(err)
	}

	// Get node value as string
	val2, err := newHC.GetAsString([]byte("foo"))
	if err != nil {
		panic(err)
	}

	fmt.Println(val2) // bar
}
```

## Generate a `.car` file

```go
import (
	"fmt"

	hamtcontainer "github.com/simplecoincom/go-ipld-adl-hamt-container"
	"github.com/simplecoincom/go-ipld-adl-hamt-container/storage"
)

func main() {
		// Linking the data into a Redis storage
	store := storage.NewRedisStorage("localhost:6379", "")

	// Create the root HAMT container
	rootHAMT, err := hamtcontainer.NewHAMTBuilder(
		hamtcontainer.WithKey([]byte("root")),
		hamtcontainer.WithStorage(store),
	).Build()
	if err != nil {
		panic(err)
	}

	// Let's add a key
	err = rootHAMT.MustBuild(func(hamtSetter hamtcontainer.HAMTSetter) error {
		return hamtSetter.Set([]byte("foo"), "bar")
	})
	if err != nil {
		panic(err)
	}

	f, err := os.Create("/tmp/files.car")

	err = rootHAMT.WriteCar(f)
	if err != nil {
		panic(err)
	}
}
```

> Then you can run `ipfs dag import /tmp/file.car` to import the dag to the IPFS Node