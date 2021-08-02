package utils

import (
	"io/ioutil"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
	ipld "github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

type readStore ipld.BlockReadOpener

func (rs readStore) Get(c cid.Cid) (blocks.Block, error) {
	link := cidlink.Link{Cid: c}
	r, err := rs(ipld.LinkContext{}, link)
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return blocks.NewBlockWithCid(data, c)
}

func ToReadStore(opener ipld.BlockReadOpener) car.ReadStore {
	return readStore(opener)
}
