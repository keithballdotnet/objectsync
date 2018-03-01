package objectsync

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
)

// Sync will sync together two Storages
// Based off - https://unterwaditzer.net/2016/sync-algorithm.html
// Last Write Wins (LWW) conflict resolution
func Sync(ctx context.Context, local, remote Storage, status StatusStorage) error {

	localSet, err := local.GetAll(ctx)
	if err != nil {
		return err
	}

	fmt.Printf("local len: %v\n", len(localSet))

	remoteSet, err := remote.GetAll(ctx)
	if err != nil {
		return err
	}

	fmt.Printf("remote len: %v\n", len(remoteSet))

	foundIDs := []string{}

	changes := []*Change{}

	/* Phase 1 - Discover changes */

	// Iterate local
	for _, localObject := range localSet {
		// Keep a note of this foundIDs to check against the status set
		foundIDs = append(foundIDs, localObject.ID)

		remoteObject, err := remote.Get(ctx, localObject.ID)
		foundRemote, err := wasFound(err)
		if err != nil {
			return err
		}

		syncStatus, err := status.Get(ctx, localObject.ID)
		foundStatus, err := wasFound(err)
		if err != nil {
			return err
		}

		// A - B - status
		if !foundRemote && !foundStatus {
			fmt.Printf("We should add local [%s] to remote\n", localObject.ID)
			// Add local -> Remote
			// Store Status
			changes = append(changes, &Change{Type: ChangeTypeAdd, Object: localObject, Store: remote})
		}

		// A + status - B
		if !foundRemote && foundStatus {
			fmt.Printf("We should delete local [%s]\n", localObject.ID)
			// Delete local
			// Delete status
			changes = append(changes, &Change{Type: ChangeTypeDelete, Object: localObject, Store: local})
		}

		// A + B - status
		if foundRemote && !foundStatus {
			fmt.Printf("Found in both sets, but missing status.  Invoke conflict resolution\n")

			// We should invoke conflict resolution as we dont know what to do with the object

			// store status
			changes = append(changes, &Change{Type: ChangeTypeAddStatus, ID: localObject.ID})

		}

		// A + B + Status
		if foundRemote && foundStatus {
			fmt.Printf("Found in both sets and status.  Check content...\n")
			fmt.Printf("localhash: %s\n", base64.StdEncoding.EncodeToString(localObject.Hash))
			fmt.Printf("localsynchash: %s\n", base64.StdEncoding.EncodeToString(syncStatus.Local))
			fmt.Printf("remotehash: %s\n", base64.StdEncoding.EncodeToString(remoteObject.Hash))
			fmt.Printf("remotesynchash: %s\n", base64.StdEncoding.EncodeToString(syncStatus.Remote))

			// A-Hash != Status-Hash && B-Hash == Status-Hash
			if !bytes.Equal(localObject.Hash, syncStatus.Local) && bytes.Equal(remoteObject.Hash, syncStatus.Remote) {
				fmt.Printf("Hash has changed local but not on remote.  Update remote.\n")
			}

			// A-Hash == Status-Hash && B-Hash != Status-Hash
			if bytes.Equal(localObject.Hash, syncStatus.Local) && !bytes.Equal(remoteObject.Hash, syncStatus.Remote) {
				fmt.Printf("Hash has changed remote but not on local.  Update local.\n")
			}

			// A-Hash != Status-Hash && B-Hash != Status-Hash
			if !bytes.Equal(localObject.Hash, syncStatus.Local) && !bytes.Equal(remoteObject.Hash, syncStatus.Remote) {
				fmt.Printf("Hash has changed local, also changed remote.  Invoke conflict resolution.\n")

			}
		}
	}

	// Iterate remote
	for _, remoteObject := range remoteSet {
		// Keep a note of this foundIDs to check against the status set
		foundIDs = append(foundIDs, remoteObject.ID)

		_, err = local.Get(ctx, remoteObject.ID)
		foundLocal, err := wasFound(err)
		if err != nil {
			return err
		}

		_, err = status.Get(ctx, remoteObject.ID)
		foundStatus, err := wasFound(err)
		if err != nil {
			return err
		}

		// B - A - status
		if !foundLocal && !foundStatus {
			fmt.Printf("We should add remote [%s] to local\n", remoteObject.ID)
			// Add remote -> local
			// store status
			changes = append(changes, &Change{Type: ChangeTypeAdd, Object: remoteObject, Store: local})
		}

		// B + status - A
		if !foundLocal && foundStatus {
			fmt.Printf("We should remove remote [%s]\n", remoteObject.ID)
			// Delete remote
			// Delete status
			changes = append(changes, &Change{Type: ChangeTypeDelete, Object: remoteObject, Store: remote})
		}
	}

	// Find dead status
	allStati, err := status.GetAll(ctx)
	if err != nil {
		return err
	}
	for _, statusEntry := range allStati {
		statusFound := false
		for _, id := range foundIDs {
			if statusEntry.ID == id {
				// Found this status in the list of relevant IDs.
				// Jump to next
				statusFound = true
				continue
			}
		}
		if statusFound {
			continue
		}

		// status - A - B
		fmt.Printf("We should remove status [%s]\n", statusEntry.ID)
		changes = append(changes, &Change{Type: ChangeTypeDeleteStatus, ID: statusEntry.ID})
	}

	/* Phase 2 - Reconcile changes */
	for _, change := range changes {
		fmt.Printf("Got change: %v\n", change.Type)

		switch change.Type {
		case ChangeTypeAdd:
			// Add object to store
			newobj := *change.Object // As we are dealing with go specific pointers, we will copy the value out
			err = change.Store.Set(ctx, &newobj)
			if err != nil {
				return err
			}
			// Set status
			err = status.Set(ctx, &SyncStatus{ID: change.Object.ID, Local: change.Object.Hash, Remote: change.Object.Hash})
			if err != nil {
				return err
			}

			fmt.Printf("Added: %v To: %s\n", change.Object.ID, change.Store.GetName())
		case ChangeTypeDelete:
			err = change.Store.Delete(ctx, change.Object.ID)
			if err != nil {
				return err
			}
			// Delete status
			err = status.Delete(ctx, change.Object.ID)
			if err != nil {
				return err
			}

			fmt.Printf("Deleted: %v From: %s\n", change.Object.ID, change.Store.GetName())
		case ChangeTypeDeleteStatus:
			err = status.Delete(ctx, change.ID)
			if err != nil {
				return err
			}
		case ChangeTypeAddStatus:
			err = status.Set(ctx, &SyncStatus{ID: change.Object.ID, Local: change.Object.Hash, Remote: change.Object.Hash})
			if err != nil {
				return err
			}
		default:
			fmt.Println("Currently unsupported change type")
		}

	}

	return nil
}

func wasFound(err error) (bool, error) {
	if err != nil && !IsNotFoundError(err) {
		return false, err
	}

	return err == nil || !IsNotFoundError(err), nil
}
