package storage

import (
	"os"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/fluent"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/multiformats/go-multicodec"
)

func TestStorageRedisWrite(t *testing.T) {
	_, ok := os.LookupEnv("SHOULD_TEST_REDIS")
	if !ok {
		return
	}

	redisHost, ok := os.LookupEnv("REDIS_HOST")
	if !ok {
		t.Error("Should test redis, requires env var REDIS_HOST")
		return
	}

	// Let's get a LinkSystem.  We're going to be working with CID links,
	//  so let's get the default LinkSystem that's ready to work with those.
	lsys := cidlink.DefaultLinkSystem()

	// Ok this is a real program, so we're going to use Redis as link storage
	store := NewRedisStorage(redisHost, "")

	// We want to store the serialized data somewhere.
	//  We'll use an in-memory store for this.  (It's a package scoped variable.)
	//  You can use any kind of storage system here;
	//   you just need a function that conforms to the ipld.BlockWriteOpener interface.
	lsys.StorageWriteOpener = store.OpenWrite

	// To create any links, first we need a LinkPrototype.
	// This gathers together any parameters that might be needed when making a link.
	// (For CIDs, the version, the codec, and the multihash type are all parameters we'll need.)
	// Often, you can probably make this a constant for your whole application.
	lp := cidlink.LinkPrototype{Prefix: cid.Prefix{
		Version:  1, // Usually '1'.
		Codec:    uint64(multicodec.DagCbor),
		MhType:   uint64(multicodec.Sha2_512),
		MhLength: 64, // sha2-512 hash has a 64-byte sum.
	}}

	// And we need some data to link to!  Here's a quick piece of example data:
	n := fluent.MustBuildMap(basicnode.Prototype.Map, 1, func(na fluent.MapAssembler) {
		na.AssembleEntry("hello").AssignString("world")
	})

	// Now: time to apply the LinkSystem, and do the actual store operation!
	lnk, err := lsys.Store(
		ipld.LinkContext{}, // The zero value is fine.  Configure it it you want cancellability or other features.
		lp,                 // The LinkPrototype says what codec and hashing to use.
		n,                  // And here's our data.
	)
	qt.Assert(t, err, qt.IsNil)

	// That's it!  We got a link.
	_, err = cid.Parse(lnk.String())
	qt.Assert(t, err, qt.IsNil)
}

func TestStorageRedisLoad(t *testing.T) {
	_, ok := os.LookupEnv("SHOULD_TEST_REDIS")
	if !ok {
		return
	}

	redisHost, ok := os.LookupEnv("REDIS_HOST")
	if !ok {
		t.Error("Should test redis, requires env var REDIS_HOST")
		return
	}

	// Let's get a LinkSystem.  We're going to be working with CID links,
	//  so let's get the default LinkSystem that's ready to work with those.
	lsys := cidlink.DefaultLinkSystem()

	// In a real program, you'll probably make functions to load and store from disk,
	// or some network storage, or... whatever you want, really :)
	store := NewRedisStorage(redisHost, "")

	// We want to store the serialized data somewhere.
	//  We'll use an redis store for this.  (It's a package scoped variable.)
	lsys.StorageWriteOpener = store.OpenWrite

	// To create any links, first we need a LinkPrototype.
	// This gathers together any parameters that might be needed when making a link.
	// (For CIDs, the version, the codec, and the multihash type are all parameters we'll need.)
	// Often, you can probably make this a constant for your whole application.
	lp := cidlink.LinkPrototype{Prefix: cid.Prefix{
		Version:  1, // Usually '1'.
		Codec:    uint64(multicodec.DagCbor),
		MhType:   uint64(multicodec.Sha2_512),
		MhLength: 64, // sha2-512 hash has a 64-byte sum.
	}}

	// And we need some data to link to!  Here's a quick piece of example data:
	n := fluent.MustBuildMap(basicnode.Prototype.Map, 1, func(na fluent.MapAssembler) {
		na.AssembleEntry("hello").AssignString("world")
	})

	// Now: time to apply the LinkSystem, and do the actual store operation!
	lnk, err := lsys.Store(
		ipld.LinkContext{}, // The zero value is fine.  Configure it it you want cancellability or other features.
		lp,                 // The LinkPrototype says what codec and hashing to use.
		n,                  // And here's our data.
	)
	qt.Assert(t, err, qt.IsNil)

	// Let's say we want to load this link (it's the same one we just created in the example above).
	cid, _ := cid.Decode("bafyrgqhai26anf3i7pips7q22coa4sz2fr4gk4q4sqdtymvvjyginfzaqewveaeqdh524nsktaq43j65v22xxrybrtertmcfxufdam3da3hbk")
	lnk = cidlink.Link{Cid: cid}

	// We need somewhere to go looking for any of the data we might want to load!
	//  We'll use an in-memory store for this.  (It's a package scoped variable.)
	//   (This particular memory store was filled with the data we'll load earlier, during ExampleStoringLink.)
	//  You can use any kind of storage system here;
	//   you just need a function that conforms to the ipld.BlockReadOpener interface.
	lsys.StorageReadOpener = store.OpenRead

	// We'll need to decide what in-memory implementation of ipld.Node we want to use.
	//  Here, we'll use the "basicnode" implementation.  This is a good getting-started choice.
	//   But you could also use other implementations, or even a code-generated type with special features!
	np := basicnode.Prototype.Any

	// Before we use the LinkService, NOTE:
	//  There's a side-effecting import at the top of the file.  It's for the dag-cbor codec.
	//  See the comments in ExampleStoringLink for more discussion of this and why it's important.

	// Apply the LinkSystem, and ask it to load our link!
	n, err = lsys.Load(
		ipld.LinkContext{}, // The zero value is fine.  Configure it it you want cancellability or other features.
		lnk,                // The Link we want to load!
		np,                 // The NodePrototype says what kind of Node we want as a result.
	)
	qt.Assert(t, err, qt.IsNil)
}
