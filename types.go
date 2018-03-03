package objectsync

import (
	"errors"
	"time"
)

// Hash is the hash of an object
type Hash []byte

// GenericObjectCollection is a collection of GenericObjects
type GenericObjectCollection []*GenericObject

// GenericObject ...
type GenericObject struct {
	ID       string
	Hash     Hash
	Modified time.Time
	Value    string
}

// SyncStatus is a status of the last sync for items
type SyncStatus struct {
	ID         string
	LocalHash  Hash
	RemoteHash Hash
}

// ChangeType represents what change should be performed
type ChangeType string

// Types of changes that can be done during a sync
const (
	ChangeTypeAdd          ChangeType = "add"
	ChangeTypeDelete       ChangeType = "delete"
	ChangeTypeUpdate       ChangeType = "update"
	ChangeTypeDeleteStatus ChangeType = "delete_status"
	ChangeTypeAddStatus    ChangeType = "add_status"
)

// Change is a change
type Change struct {
	Type       ChangeType
	ID         string
	Object     *GenericObject
	Store      Storage
	SyncStatus *SyncStatus
}

// ErrorNotFound ...
var ErrorNotFound = errors.New("not found")

// IsNotFoundError ...
func IsNotFoundError(err error) bool {
	return err.Error() == ErrorNotFound.Error()
}
