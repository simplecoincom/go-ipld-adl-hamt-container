package utils

import (
	"errors"

	"github.com/ipld/go-ipld-prime"
)

var ErrNilIPLDNode = errors.New("Unexpected nil ipld.Node")
var ErrMissingBasicKind = errors.New("ipld.Node does not have a basic kind")

func NodeValue(node ipld.Node) (interface{}, error) {
	if node == nil {
		return nil, ErrNilIPLDNode
	}

	var val interface{}
	var err error
	switch kind := node.Kind(); kind {
	case ipld.Kind_Null:
		return nil, nil
	case ipld.Kind_Bool:
		val, err = node.AsBool()
	case ipld.Kind_Int:
		val, err = node.AsInt()
	case ipld.Kind_Float:
		val, err = node.AsFloat()
	case ipld.Kind_String:
		val, err = node.AsString()
	case ipld.Kind_Bytes:
		val, err = node.AsBytes()
	case ipld.Kind_Link:
		val, err = node.AsLink()
	default:
		err = ErrMissingBasicKind
	}

	if err != nil {
		return nil, err
	}

	return val, nil
}
