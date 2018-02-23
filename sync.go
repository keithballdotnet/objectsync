package objectsync

import (
	"context"
	"fmt"
)

// Sync will sync together two Storages
// Based off - https://unterwaditzer.net/2016/sync-algorithm.html
// Last Write Wins (LWW) conflict resolution
func Sync(ctx context.Context, local, remote Storage, status StatusStorage) error {

	localSet, _, err := local.GetAll(ctx)
	if err != nil {
		return err
	}

	remoteSet, _, err := remote.GetAll(ctx)
	if err != nil {
		return err
	}

	foundIDs := []string{}

	changes := []*Change{}

	/* Phase 1 - Discover changes */

	for _, localObject := range localSet {
		// Keep a note of this foundIDs to check against the status set
		foundIDs = append(foundIDs, localObject.ID)

		_, _, err = remote.Get(ctx, localObject.ID)
		foundRemote := wasFound(err)

		_, err = status.Get(ctx, localObject.ID)
		foundStatus := wasFound(err)

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
			fmt.Printf("Found in both sets, but missing status.  Add status\n")
			// store status
		}

	}

	for _, remoteObject := range remoteSet {
		// Keep a note of this foundIDs to check against the status set
		foundIDs = append(foundIDs, remoteObject.ID)

		_, _, err = local.Get(ctx, remoteObject.ID)
		foundLocal := wasFound(err)
		_, err = status.Get(ctx, remoteObject.ID)
		foundStatus := wasFound(err)

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
	for _, status := range allStati {
		statusFound := false
		for _, id := range foundIDs {
			if status.ID == id {
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
		fmt.Printf("We should remove status [%s]\n", status.ID)
	}

	/* Phase 2 - Reconcile changes */
	for _, change := range changes {
		fmt.Printf("Got change: %v\n", change.Type)

		switch change.Type {
		case ChangeTypeAdd:
			// Add object to store
			err = change.Store.Set(ctx, change.Object)
			if err != nil {
				return err
			}
			// Set status
			err = status.Set(ctx, &SyncStatus{ID: change.Object.ID})
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
		default:
			fmt.Println("Currently unsupported change type")
		}

	}

	return nil
}

func wasFound(err error) bool {
	return err == nil || err.Error() != "not found"
}

/* Old experiments

func getTree(ctx context.Context, store Storage) (*merkle.Tree, error) {
	hashes, err := store.GetHashes(ctx)
	if err != nil {
		return nil, err
	}

	objects, err := getTreeObjects(ctx, hashes)
	if err != nil {
		return nil, err
	}

	tree, err := createMerkleTree(ctx, objects)
	if err != nil {
		return nil, err
	}

	//fmt.Printf("tree: %v", tree.ToString(ctx))

	return tree, nil
}


// getTreeObjects will return a sorted serialized object collection
func getTreeObjects(ctx context.Context, hashes map[string]Hash) (TreeObjectCollection, error) {

	objects := make([]*TreeObject, len(hashes))
	i := 0
	for id, hash := range hashes {
		// Create tree object
		objects[i] = &TreeObject{ID: id, Hash: hash}
		i++
	}

	// Sort collection
	sort.Sort(treeObjectSorter(objects))

	return TreeObjectCollection(objects), nil
}

// createMerkleTree will return a merkle tree for a collection of serialized objects
func createMerkleTree(ctx context.Context, objects TreeObjectCollection) (*merkle.Tree, error) {
	// create a new Sha256 powered MerkleTree
	tree := merkle.NewTree(&merkle.Sha256Hasher{})
	// We will add the hashes to the tree, not the actual data.
	// Add the IDs as extra data so we have an ID in the tree
	// we can use to identify the leaves from the tree
	tree.AddContent(ctx, objects.GetHashes(), objects.GetIDs())
	// Build the tree
	tree.Build(ctx)

	return tree, nil
}



localTree, err := getTree(ctx, local)
	if err != nil {
		return err
	}

	remoteTree, err := getTree(ctx, remote)
	if err != nil {
		return err
	}

	// Nothing to sync
	if bytes.Equal(localTree.GetRootHash(), remoteTree.GetRootHash()) {
		fmt.Printf("Tree Root matches: %v\n", base64.StdEncoding.EncodeToString(localTree.GetRootHash()))
		return nil
	}

	// Old school merge via differential

	fmt.Printf("Tree Depth: local: %v remote: %v\n", localTree.Depth, remoteTree.Depth)

	localLeaves := localTree.Layers[localTree.Depth-1]
	remoteLeaves := remoteTree.Layers[remoteTree.Depth-1]

	fmt.Printf("Tree Leaves: local: %v remote: %v\n", len(localLeaves), len(remoteLeaves))

	// First merge local -> remote
	for _, localLeaf := range localLeaves {
		foundLeaf := false
		for _, remoteLeaf := range remoteLeaves {
			if bytes.Equal(remoteLeaf.Hash, localLeaf.Hash) {
				foundLeaf = true
			}
		}
		// Add leaf to remote leaves
		if !foundLeaf {
			data, _, err := local.Get(ctx, string(localLeaf.ExtraData))
			if err != nil {
				return err
			}
			err = remote.Set(ctx, data)
			if err != nil {
				return err
			}
			// TODO:  Add leaf to localLeaves
		}
	}

	// Second pass remote -> local
	for _, remoteLeaf := range remoteLeaves {
		foundLeaf := false
		for _, localLeaf := range localLeaves {
			if bytes.Equal(remoteLeaf.Hash, localLeaf.Hash) {
				foundLeaf = true
			}
		}
		// Add leaf to local data
		if !foundLeaf {
			data, _, err := remote.Get(ctx, string(remoteLeaf.ExtraData))
			if err != nil {
				return err
			}
			err = local.Set(ctx, data)
			if err != nil {
				return err
			}
		}
	}

	// Experiments in walking the tree.
	// Maybe I need to walk the tree at the same time.

	// fmt.Printf("Tree Depth: local: %v remote: %v\n", localTree.Depth, remoteTree.Depth)
	// if localTree.Depth != remoteTree.Depth {
	// 	// Fall back to hash list?
	// } else {
	// 	checkIndexes := []int{0}
	// 	// Under the root check layers
	// 	for l := 1; l < localTree.Depth; l++ {
	// 		//fmt.Printf("Check Layer %v from %v\n", l, len(localTree.Layers))
	// 		localLayer := localTree.Layers[l]
	// 		remoteLayer := remoteTree.Layers[l]

	// 		layerLen := len(localLayer)

	// 		// We check the left nodes
	// 		// And any ODD node for differences
	// 		childrenIndexes := []int{}
	// 		for _, b := range checkIndexes {
	// 			// Check me.  Plus my sibling.
	// 			localNode := localLayer[b]
	// 			remoteNode := remoteLayer[b]

	// 			// fmt.Printf("Check Left Node %v-%v from %v - ", l, b, len(localLayer))

	// 			// fmt.Printf("local: %s %v - remote: %s %v\n",
	// 			// 	base64.StdEncoding.EncodeToString(localNode.Hash),
	// 			// 	localNode.IsLeft,
	// 			// 	base64.StdEncoding.EncodeToString(remoteNode.Hash),
	// 			// 	remoteNode.IsLeft)

	// 			if !bytes.Equal(localNode.Hash, remoteNode.Hash) {
	// 				if localNode.Type == merkle.NodeTypeLeaf {
	// 					fmt.Printf("FOUND A LEAF!!!: %s", string(localNode.ExtraData))
	// 				}

	// 				childrenIndexes = append(childrenIndexes, b*2)
	// 			}

	// 			// Have we looked at a promoted node?
	// 			if b+1 == layerLen {
	// 				continue
	// 			}

	// 			localSiblingNode := localLayer[b+1]
	// 			remoteSiblingNode := remoteLayer[b+1]

	// 			// fmt.Printf("localsib: %s %v - remotesib: %s %v\n",
	// 			// 	base64.StdEncoding.EncodeToString(localSiblingNode.Hash),
	// 			// 	localSiblingNode.IsLeft,
	// 			// 	base64.StdEncoding.EncodeToString(remoteSiblingNode.Hash),
	// 			// 	remoteSiblingNode.IsLeft)

	// 			if !bytes.Equal(localSiblingNode.Hash, remoteSiblingNode.Hash) {
	// 				if localNode.Type == merkle.NodeTypeLeaf {
	// 					fmt.Printf("FOUND A LEAF!!!: %s", string(localNode.ExtraData))
	// 				}
	// 				childrenIndexes = append(childrenIndexes, (b+1)*2)
	// 			}
	// 		}

	// 		// Do we need to search anymore?
	// 		if len(childrenIndexes) == 0 {
	// 			break
	// 		}
	// 		// On next layer check the following indexes
	// 		checkIndexes = childrenIndexes

	// 	}
	// }

	// // Depth of tree is different.
	// if localTree.Depth == remoteTree.Depth {
	// 	fmt.Printf("Tree Depth is same: local: %v remote: %v\n", localTree.Depth, remoteTree.Depth)
	// 	// Descend layers of the tree looking for differences
	// 	// Check children of root
	// 	checkIndexes := []int{0, 1}
	// 	// Skip root layer
	// 	for i := 1; i < localTree.Depth; i++ {
	// 		fmt.Printf("Check Layer %v from %v\n", i, len(localTree.Layers[i]))
	// 		childrenIndexes := []int{}
	// 		for _, b := range checkIndexes {
	// 			fmt.Printf("Check Node %v-%v\n", i, b)
	// 			localNode := localTree.Layers[i][b]
	// 			remoteNode := remoteTree.Layers[i][b]
	// 			// Do the nodes match?  If not follow the branch down
	// 			if !bytes.Equal(localNode.Hash, remoteNode.Hash) {
	// 				fmt.Printf("Node difference %v-%v: local: %s remote: %s\n", i, b, base64.StdEncoding.EncodeToString(localNode.Hash), base64.StdEncoding.EncodeToString(remoteNode.Hash))
	// 				// On next pass drop down to child and check sibling
	// 				childrenIndexes = append(childrenIndexes, b*2)
	// 				childrenIndexes = append(childrenIndexes, (b*2)+1)
	// 			}
	// 		}
	// 		// On next layer check the following indexes
	// 		checkIndexes = childrenIndexes
	// 	}
	//}

*/
