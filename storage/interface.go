package storage

import (
	"errors"
	"io"

	"github.com/ipld/go-ipld-prime"
)

var ErrDataNotFound = errors.New("Data not found on the storage")

// Storage represents the default interface for linkable data
type Storage interface {
	OpenRead(lnkCtx ipld.LinkContext, lnk ipld.Link) (io.Reader, error)
	OpenWrite(lnkCtx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error)
}
