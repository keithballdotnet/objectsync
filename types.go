package objectsync

import (
	"bytes"
	"time"
)

// GenericObjectCollection is a collection of GenericObjects
type GenericObjectCollection []*GenericObject

// GenericObject ...
type GenericObject struct {
	ID       string
	Modified time.Time
	Value    string
}

// SerializedObjectCollection is a collection of serialized objects
type SerializedObjectCollection []*SerializedObject

// SerializedObject is a pairing of data and a hash
type SerializedObject struct {
	Hash []byte
	Data []byte
}

// GetData will return the data slice of the SerializedObjectCollection
func (c SerializedObjectCollection) GetData() [][]byte {
	data := make([][]byte, len(c))
	for i := 0; i < len(c); i++ {
		data[i] = c[i].Data
	}
	return data
}

// GetHashes will return the hash slice of the SerializedObjectCollection
func (c SerializedObjectCollection) GetHashes() [][]byte {
	hashes := make([][]byte, len(c))
	for i := 0; i < len(c); i++ {
		hashes[i] = c[i].Hash
	}
	return hashes
}

type serializedObjectSorter []*SerializedObject

func (b serializedObjectSorter) Len() int { return len(b) }
func (b serializedObjectSorter) Less(i, j int) bool {
	switch bytes.Compare(b[i].Hash, b[j].Hash) {
	case -1:
		return true
	case 0, 1:
		return false
	default:
		return false
	}
}
func (b serializedObjectSorter) Swap(i, j int) { b[j], b[i] = b[i], b[j] }
