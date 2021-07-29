package storage

import (
	"bytes"
	"encoding/base64"
	"io"

	"github.com/go-redis/redis/v8"
	"github.com/ipld/go-ipld-prime"
)

// Redis is a key value storage for data indexed by ipld.Link.
//
// The OpenRead method conforms to ipld.BlockReadOpener,
// and the OpenWrite method conforms to ipld.BlockWriteOpener.
// Therefore it's easy to use in a LinkSystem like this:
//
//		store := storage.Redis{}
//		lsys.StorageReadOpener = (&store).OpenRead
//		lsys.StorageWriteOpener = (&store).OpenWrite
type Redis struct {
	addr   string
	passwd string
	rdb    *redis.Client
}

func NewRedisStorage(addr, passwd string) Storage {
	return &Redis{addr, passwd, nil}
}

func (store *Redis) beInitialized() {
	if store.rdb != nil {
		return
	}

	store.rdb = redis.NewClient(&redis.Options{
		Addr:     store.addr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
}

func (store *Redis) OpenRead(lnkContext ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
	store.beInitialized()

	result, err := store.rdb.Get(lnkContext.Ctx, lnk.String()).Result()
	if err == redis.Nil {
		return nil, ErrDataNotFound
	} else if err != nil {
		return nil, err
	}

	data, err := base64.StdEncoding.DecodeString(result)
	if err != nil {
		return nil, err
	}

	return bytes.NewReader(data), nil
}

func (store *Redis) OpenWrite(lnkContext ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
	store.beInitialized()

	buf := bytes.Buffer{}
	return &buf, func(lnk ipld.Link) error {
		result := base64.StdEncoding.EncodeToString(buf.Bytes())
		err := store.rdb.Set(lnkContext.Ctx, lnk.String(), result, 0).Err()
		if err != nil {
			return err
		}

		return nil
	}, nil
}
