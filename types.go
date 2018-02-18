package objectsync

import (
	"bytes"
	"time"
)

// Hash is the hash of an object
type Hash []byte

// GenericObjectCollection is a collection of GenericObjects
type GenericObjectCollection []*GenericObject

// GenericObject ...
type GenericObject struct {
	ID       string
	Modified time.Time
	Value    string
}

// TreeObjectCollection is a collection of tree objects
type TreeObjectCollection []*TreeObject

// TreeObject is a pairing of data and a hash
type TreeObject struct {
	ID   string
	Hash Hash
}

// GetHashes will return the hash slice of the TreeObjectCollection
func (c TreeObjectCollection) GetHashes() [][]byte {
	hashes := make([][]byte, len(c))
	for i := 0; i < len(c); i++ {
		hashes[i] = c[i].Hash
	}
	return hashes
}

// GetIDs will return the IDs of the TreeObjectCollection
func (c TreeObjectCollection) GetIDs() [][]byte {
	ids := make([][]byte, len(c))
	for i := 0; i < len(c); i++ {
		ids[i] = []byte(c[i].ID)
	}
	return ids
}

// treeObjectSorted will sort objects based on their Hash
type treeObjectSorter []*TreeObject

func (b treeObjectSorter) Len() int { return len(b) }
func (b treeObjectSorter) Less(i, j int) bool {
	switch bytes.Compare(b[i].Hash, b[j].Hash) {
	case -1:
		return true
	case 0, 1:
		return false
	default:
		return false
	}
}
func (b treeObjectSorter) Swap(i, j int) { b[j], b[i] = b[i], b[j] }
