package storage

import (
	"bytes"
	"fmt"
	"io"

	ipfsApi "github.com/ipfs/go-ipfs-api"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

// IPFS structu contains the IPFS shell connection
//
// The OpenRead method conforms to ipld.BlockReadOpener,
// and the OpenWrite method conforms to ipld.BlockWriteOpener.
// Therefore it's easy to use in a LinkSystem like this:
//
//		store := storage.Redis{}
//		lsys.StorageReadOpener = (&store).OpenRead
//		lsys.StorageWriteOpener = (&store).OpenWrite
type IPFS struct {
	shell *ipfsApi.Shell
}

func NewIPFSStorage(shell *ipfsApi.Shell) Storage {
	return &IPFS{shell: shell}
}

func (store *IPFS) beInitialized() {

}

func (store *IPFS) OpenRead(_ ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
	store.beInitialized()

	theCid, ok := lnk.(cidlink.Link)
	if !ok {
		return nil, fmt.Errorf("Attempted to load a non CID link: %v", lnk)
	}

	block, err := store.shell.BlockGet(theCid.String())
	if err != nil {
		return nil, fmt.Errorf("error loading %v: %v", theCid.String(), err)
	}

	return bytes.NewBuffer(block), nil
}

func (store *IPFS) OpenWrite(_ ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
	store.beInitialized()

	buf := bytes.Buffer{}
	return &buf, func(lnk ipld.Link) error {
		_, err := store.shell.BlockPut(
			buf.Bytes(),
			// TODO: How to pass those params?
			"cbor",
			"sha2-512",
			64,
		)
		return err
	}, nil
}
