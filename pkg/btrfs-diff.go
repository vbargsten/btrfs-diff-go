//+build linux

// to build it run :
//   GOOS=linux GOARCH=amd64 go build -a -v btrfs-diff.go
// it requires: golang and libbtrfs-dev, so on GNU Linux/Debian do :
//   sudo apt install golang libbtrfs-dev

package btrfsdiff

// We get the constants from this header.

// #include <btrfs/send.h>
// #include <btrfs/send-utils.h>
// #include <btrfs/ioctl.h>
// #cgo LDFLAGS: -lbtrfs
import "C"

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
	"unsafe"
)

var debug bool = false

// operation is the result of one or more instructions/commands
type operation int

// list of available operations
const (
	opUnspec operation = iota
	opIgnore
	opCreate
	opModify
	opTimes
	opPermissions
	opOwnership
	opAttributes
	opDelete
	opRename
	opEnd
)

// operation's names
var names []string = []string{"!!!", "ignored", "added", "changed", "times", "perms", "own", "attr", "deleted", "renamed", "END"}

// convert an operation to a string
func (op operation) String() string {
	return names[op]
}

// commandMapOp is the mapping between a command and a resulting operation
type commandMapOp struct {
	Name string
	Op   operation
}

// commandInst is the instanciation of a command and its data
type commandInst struct {
	Type *commandMapOp
	data []byte
}

// initCommandsDefinitions initialize the commands mapping with operations
func initCommandsDefinitions() *[C.__BTRFS_SEND_C_MAX]commandMapOp {

	var commandsDefs [C.__BTRFS_SEND_C_MAX]commandMapOp
	commandsDefs[C.BTRFS_SEND_C_UNSPEC] = commandMapOp{Name: "BTRFS_SEND_C_UNSPEC", Op: opUnspec}

	commandsDefs[C.BTRFS_SEND_C_SUBVOL] = commandMapOp{Name: "BTRFS_SEND_C_SUBVOL", Op: opIgnore}
	commandsDefs[C.BTRFS_SEND_C_SNAPSHOT] = commandMapOp{Name: "BTRFS_SEND_C_SNAPSHOT", Op: opIgnore}

	commandsDefs[C.BTRFS_SEND_C_MKFILE] = commandMapOp{Name: "BTRFS_SEND_C_MKFILE", Op: opCreate}
	commandsDefs[C.BTRFS_SEND_C_MKDIR] = commandMapOp{Name: "BTRFS_SEND_C_MKDIR", Op: opCreate}
	commandsDefs[C.BTRFS_SEND_C_MKNOD] = commandMapOp{Name: "BTRFS_SEND_C_MKNOD", Op: opCreate}
	commandsDefs[C.BTRFS_SEND_C_MKFIFO] = commandMapOp{Name: "BTRFS_SEND_C_MKFIFO", Op: opCreate}
	commandsDefs[C.BTRFS_SEND_C_MKSOCK] = commandMapOp{Name: "BTRFS_SEND_C_MKSOCK", Op: opCreate}
	commandsDefs[C.BTRFS_SEND_C_SYMLINK] = commandMapOp{Name: "BTRFS_SEND_C_SYMLINK", Op: opCreate}

	commandsDefs[C.BTRFS_SEND_C_LINK] = commandMapOp{Name: "BTRFS_SEND_C_LINK", Op: opCreate}

	commandsDefs[C.BTRFS_SEND_C_RENAME] = commandMapOp{Name: "BTRFS_SEND_C_RENAME", Op: opRename}

	commandsDefs[C.BTRFS_SEND_C_UNLINK] = commandMapOp{Name: "BTRFS_SEND_C_UNLINK", Op: opDelete}
	commandsDefs[C.BTRFS_SEND_C_RMDIR] = commandMapOp{Name: "BTRFS_SEND_C_RMDIR", Op: opDelete}

	commandsDefs[C.BTRFS_SEND_C_WRITE] = commandMapOp{Name: "BTRFS_SEND_C_WRITE", Op: opModify}
	commandsDefs[C.BTRFS_SEND_C_CLONE] = commandMapOp{Name: "BTRFS_SEND_C_CLONE", Op: opModify}
	commandsDefs[C.BTRFS_SEND_C_TRUNCATE] = commandMapOp{Name: "BTRFS_SEND_C_TRUNCATE", Op: opModify}

	commandsDefs[C.BTRFS_SEND_C_CHMOD] = commandMapOp{Name: "BTRFS_SEND_C_CHMOD", Op: opIgnore}
	commandsDefs[C.BTRFS_SEND_C_CHOWN] = commandMapOp{Name: "BTRFS_SEND_C_CHOWN", Op: opIgnore}
	commandsDefs[C.BTRFS_SEND_C_UTIMES] = commandMapOp{Name: "BTRFS_SEND_C_UTIMES", Op: opIgnore}
	commandsDefs[C.BTRFS_SEND_C_SET_XATTR] = commandMapOp{Name: "BTRFS_SEND_C_SET_XATTR", Op: opIgnore}
	commandsDefs[C.BTRFS_SEND_C_REMOVE_XATTR] = commandMapOp{Name: "BTRFS_SEND_C_REMOVE_XATTR", Op: opIgnore}

	commandsDefs[C.BTRFS_SEND_C_END] = commandMapOp{Name: "BTRFS_SEND_C_END", Op: opEnd}
	commandsDefs[C.BTRFS_SEND_C_UPDATE_EXTENT] = commandMapOp{Name: "BTRFS_SEND_C_UPDATE_EXTENT", Op: opModify}
	// Sanity check (hopefully no holes).
	for i, command := range commandsDefs {
		if i != C.BTRFS_SEND_C_UNSPEC && command.Op == opUnspec {
			return nil
		}
	}
	return &commandsDefs
}

// do the initialization of the commands mapping
var commandsDefs *[C.__BTRFS_SEND_C_MAX]commandMapOp = initCommandsDefinitions()

// nodeInst is the representation of a file in a tree, with metadata attached
//   Children   is the files in the directory (if the file is a directory)
//   Op is the name of the operation done on that node
//   Parent     is the parent directory of the file
//   Original   is the file in its previous version (new file may have an older version)
type nodeInst struct {
	Children   map[string]*nodeInst
	Name       string
	State      operation
	Parent     *nodeInst
	Original   *nodeInst
}

// diffInst is the structure that hold two trees, one new, one old
type diffInst struct {
	Original nodeInst
	New      nodeInst
}

// processSingleParamOp is the processing of single param commands (update the Diff double tree)
// Note: that code only allow to register for one operation per file. For example, a file can't
//       have its time modified and its ownership at the same time, even if this is actually the
//       case in reality. That simplified design looses information. The last operation will
//       not override the previous one.
func (diff *diffInst) processSingleParamOp(Op operation, path string) (error) {
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG]            searching for matching node\n")
	}
	isNew := Op == opCreate
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG]                is new? %t (only when '%v')\n", isNew, opCreate)
	}
	fileNode := diff.updateBothTreesAndReturnNode(path, isNew)
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG]            found: %v\n", fileNode)
	}

	// in case of deletion
	if Op == opDelete {
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG]            that's a deletion\n")
		}
		if fileNode.Original == nil {
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]            no Original tree existing\n")
			}
			fmt.Fprintf(os.Stderr, "[DEBUG]            BUG? deleting path %v which was created in same diff?\n", path)
		}
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG]            deleting the node in the Parent tree (may be New tree)\n")
		}
		delete(fileNode.Parent.Children, fileNode.Name)

		// If we deleted /this/ node, it sure as hell needs no children.
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG]            deleting the node children\n")
		}
		fileNode.Children = nil

		// old version exist
		if fileNode.Original != nil {
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]                    node had a previous version\n")
				fmt.Fprintf(os.Stderr, "[DEBUG]                    setting its State to '%v'\n", opDelete)
				fmt.Fprintf(os.Stderr, "[DEBUG]                    deleting the old node children\n")
			}
			// Leave behind a sentinel in the Original structure.
			fileNode.Original.State = opDelete
			err := fileNode.Original.verifyDelete(path)
			if err != nil {
				return err
			}
			fileNode.Original.Children = nil
		}

	// not a deletion
	} else {

		// not a creation nor the current operation is a modification
		if (fileNode.State != opCreate || (Op != opModify && Op != opTimes && Op != opPermissions && Op != opOwnership && Op != opAttributes)) {
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]            current operation is not a modification, or the current node State is not '%v' (%v)\n", opCreate, fileNode.State)
				fmt.Fprintf(os.Stderr, "[DEBUG]            replacing it with current operation '%v'\n", Op)
			}
			fileNode.State = Op
		}
	}
	return nil
}

// verifyDelete checks that every children of the node are either deleted or renamed
func (node *nodeInst) verifyDelete(path string) (error) {
	for _, child := range node.Children {
		if child.State != opDelete && child.State != opRename {
			return fmt.Errorf("BUG? deleting parent of node %v in %v which is not gone", node, path)
		}
	}
	return nil
}

// processTwoParamsOp is the processing of double params commands (update the Diff double tree)
//   It is used only by the 'rename' operation, although it could be used for 'link' op, but this
//   one is treated as a single param, for simplicity's sake.
func (diff *diffInst) processTwoParamsOp(Op operation, from string, to string) {

	// from node
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG]            searching for 'from' node\n")
	}
	fromNode := diff.updateBothTreesAndReturnNode(from, false)
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG]            found: %v\n", fromNode)
	}

	// renaming specifics
	if Op == opRename {
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG]            it's a renaming\n")
		}

		// deleting the node from the new files tree, because it should only exist in the old tree
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG]                removing it from its parent node '%v' (New tree)\n", fromNode.Parent.Name)
		}
		delete(fromNode.Parent.Children, fromNode.Name)
		// Note: now the fromNode is orphan

		if fromNode.Original != nil {

			// if fromNode had an original, we must mark that path destroyed.
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]                set old node St to '%v' (Original tree)\n", opRename)
			}
			fromNode.Original.State = opRename
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]                old node is now: %v\n", fromNode.Original)
			}
		}
	}

	// to node
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG]            searching for 'to' node\n")
	}
	toNode := diff.updateBothTreesAndReturnNode(to, true)
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG]            found: %v\n", toNode)
	}

	// renaming specifics
	if Op == opRename {
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG]            it's a renaming (bis)\n")
		}

		// attaching the old node (which was orphan) to the new files tree
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG]                adding the 'from' node to the 'to' node parent tree at name '%v' (New tree)\n", toNode.Name)
		}
		toNode.Parent.Children[toNode.Name] = fromNode

		// updating its name
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG]                'from' node Name is replaced by the 'to' node Name '%v'\n", toNode.Name)
		}
		fromNode.Name = toNode.Name

		// ensuring its status is 'added'
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG]                'from' node Op is set to '%v'\n", opCreate)
		}
		fromNode.State = opCreate

		// updating the parent node
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG]                'from' node Parent is assigned the 'to' node Parent '%v'\n", toNode.Parent.Name)
		}
		fromNode.Parent = toNode.Parent

		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG]                now node is: %v\n", toNode.Parent.Children[toNode.Name])
		}
	}
}

// updateBothTreesAndReturnNode return the searched node, after it has updated both Diff trees (old and new)
func (diff *diffInst) updateBothTreesAndReturnNode(path string, isNew bool) *nodeInst {
	if diff.New.Original == nil {
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG]                the New tree is not referencing the Original one, fixing that\n")
		}
		diff.New.Original = &diff.Original
	}
	if path == "" {
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG]                empty path, returning the node from the top level of the New tree '%v'\n", diff.New.Name)
		}
		return &diff.New
	}

	// foreach part of the path (in the 'New' tree)
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG]                splitted path in parts, processing each one ...\n")
	}
	parts := strings.Split(path, "/")

	parent := &diff.New
	for i, part := range parts {
		nodeName := strings.Trim(part, "\x00")
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG]                    - %v\n", nodeName)
		}
		if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]                        parent node is %v\n", parent)
		}
		if parent.Children == nil {
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]                            no children: set a new empty children list/map\n")
			}
			parent.Children = make(map[string]*nodeInst)
		}

		// get the node in the parent tree (New tree)
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG]                        getting the node in the children (New tree)\n")
		}
		newNode := parent.Children[nodeName]
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG]                            found: %v\n", newNode)
		}

		// the parent part/node doesn't exist
		if newNode == nil {
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]                        creating the node\n")
			}

			// creating it
			parent.Children[nodeName] = &nodeInst{}
			newNode = parent.Children[nodeName]
			newNode.Name = nodeName
			newNode.Parent = parent
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]                            created: %v\n", newNode)
				fmt.Fprintf(os.Stderr, "[DEBUG]                            added to its parent node (New tree)\n")
			}

			// no previous version of the parent node
			oldParent := parent.Original
			if oldParent == nil {
				if debug {
					fmt.Fprintf(os.Stderr, "[DEBUG]                            no previous version of the parent node (Original tree)\n")
				}
				// was !(isNew && i == len(parts)-1) which is the same, but replaced for consistency reason
				if !isNew || i < len(parts)-1 {
					if debug {
						fmt.Fprintf(os.Stderr, "[DEBUG]                            isNew is 'false' or the parent part isn't the last one\n")
					}
					// Either a path has a route in the oldParent, or it's been
					// explicitly created. Once we traverse into a path without
					// an oldParent, we know the full tree, so getting here is a
					// sign we did it wrong.
					fmt.Fprintf(os.Stderr, "BUG? referenced path %v cannot exist\n", path)
					os.Exit(1)
				}

			// the parent node have a previous version
			} else {
				if debug {
					fmt.Fprintf(os.Stderr, "[DEBUG]                            the parent node have an old version (Original tree)\n")
					fmt.Fprintf(os.Stderr, "[DEBUG]                                old parent: %v\n", oldParent)
				}
				if oldParent.Children == nil {
					if debug {
						fmt.Fprintf(os.Stderr, "[DEBUG]                                    no children: set a new empty children list/map\n")
					}
					oldParent.Children = make(map[string]*nodeInst)
				}

				// get the node in the previous tree (Original tree)
				if debug {
					fmt.Fprintf(os.Stderr, "[DEBUG]                        getting the node in the previous tree (Original tree)\n")
				}
				oldNode := oldParent.Children[nodeName]

				// the node didn't exist before
				if oldNode == nil {
					if debug {
						fmt.Fprintf(os.Stderr, "[DEBUG]                        node '%v' didn't exist before (Original tree)\n", nodeName)
					}
					if !isNew || i < len(parts)-1 {
						if debug {
							fmt.Fprintf(os.Stderr, "[DEBUG]                        isNew is 'false' or the parent part isn't the last one\n")
						}

						// Was meant to already exist, so make sure it did!
						if debug {
							fmt.Fprintf(os.Stderr, "[DEBUG]                        that path part is supposed to exist, so creating it\n")
						}
						oldParent.Children[nodeName] = &nodeInst{}
						oldNode = oldParent.Children[nodeName]
						oldNode.Name = nodeName
						oldNode.Parent = oldParent
						newNode.Original = oldNode
						if debug {
							fmt.Fprintf(os.Stderr, "[DEBUG]                        added to its parent node (Original tree)\n")
						}
					}
				}
			}

		// the parent part/node exists
		} else {
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]                        node exists (New tree)\n")
			}
			if isNew && i == len(parts)-1 {
				if debug {
					fmt.Fprintf(os.Stderr, "[DEBUG]                            isNew is 'true' and the parent part is the last one\n")
				}

				// As this is the target of a create, we should expect to see
				// nothing here.
				fmt.Fprintf(os.Stderr, "BUG? overwritten path %v already existed\n", path)
				os.Exit(1)
			}
		}
		parent = newNode
	}
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG]                returning node '%v'\n", parent)
	}
	return parent
}

// convert a node to a string
func (node *nodeInst) String() string {
	var oriNodeName string = "<nil>"
	var oriNodeSt operation
	var childrenCount int = len(node.Children)
	if node.Original != nil {
		oriNodeName = node.Original.Name
		if oriNodeName == node.Name {
			oriNodeName = "<same>"
		}
		oriNodeSt = node.Original.State
		return fmt.Sprintf("_n('%v', St:%v, ori:%v, oriSt:%v, child:%d)", node.Name, node.State, oriNodeName, oriNodeSt, childrenCount)
	}
	return fmt.Sprintf("_n('%v', St:%v, child:%d)", node.Name, node.State, childrenCount)
}

// convert a tree to a string
func (diff *diffInst) String() string {
	return "\n\t" + strings.Join((diff.Changes())[:], "\n\t") + "\n"
}

// Changes return the list of changes for a diff double tree
func (diff *diffInst) Changes() []string {
	newFiles := make(map[string]*nodeInst)
	oldFiles := make(map[string]*nodeInst)
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG] --- new ---\n")
	}
	resolvePathsAndFlatten(&diff.New, "", newFiles)
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG] ------\n")
		fmt.Fprintf(os.Stderr, "[DEBUG] --- old ---\n")
	}
	resolvePathsAndFlatten(&diff.Original, "", oldFiles)
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG] ------\n")
		fmt.Fprintf(os.Stderr, "[DEBUG] processing %d old files ...\n", len(oldFiles))
	}
	var ret []string

	// old files
	for path, node := range oldFiles {
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG]    - old node: %v # %v\n", node, path)
		}

		// getting the same path in the new files
		var newFileNode *nodeInst = newFiles[path]

		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG]        new file node: %v\n", newFileNode)
		}

		// all the modified files (found in new files and old node Op is opUnspec)
		if newFileNode != nil && node.State == opUnspec {
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]        found in new files (%v) and old node St is '%v'\n", newFileNode, opUnspec)
				fmt.Fprintf(os.Stderr, "[DEBUG]        that's a changed file\n")
			}

			ret = append(ret, fmt.Sprintf("%10v: %v", newFileNode.State, path))
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]                appended (node.St:%v): %10v: %v (%v) (%v)\n", opUnspec, newFileNode.State, path, newFileNode, node)
			}

			// deleting the node from the new ones, to avoid being processed twice for the same info
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]            deleting node in new files\n")
			}
			delete(newFiles, path)

		// not a modified file (not in new files, or node Op is not opUnspec)
		} else {
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]        not found in new files (%v) or old node St is not '%v'\n", newFileNode, opUnspec)
				fmt.Fprintf(os.Stderr, "[DEBUG]        not a changed file (%v)\n", node.State)
			}

			if node.State != opDelete && node.State != opRename {
				fmt.Fprintf(os.Stderr, "unexpected State on oldParent %v: %v", path, node.State)
			}
			if (node.State == opDelete || node.State == opRename) && newFileNode != nil && newFileNode.State == opCreate {
				ret = append(ret, fmt.Sprintf("%10v: %v", opModify, path))
				if debug {
					fmt.Fprintf(os.Stderr, "[DEBUG] appended (opDelete||opRename): %10v: %v\n", opModify, path)
				}
				delete(newFiles, path)
			} else {
				//fmt.Fprintf(os.Stderr, "DEBUG %v %v %v\n ", node.State, newFileNode, path)
				ret = append(ret, fmt.Sprintf("%10v: %v", node.State, path))
				if debug {
					fmt.Fprintf(os.Stderr, "[DEBUG] appended (rest): %10v: %v\n", node.State, path)
				}
			}
		}
	}

	// new files
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG] processing %d new files ...\n", len(newFiles))
	}
	for path, node := range newFiles {
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG]    - new node: %v # %v\n", node, path)
		}

		ret = append(ret, fmt.Sprintf("%10v: %v", opCreate, path))
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]        appended (new): %10v: %v\n", node.State, path)
			}
	}
	return ret
}

// resolvePathsAndFlatten generate a flat slice with full path mapped to nodes
func resolvePathsAndFlatten(node *nodeInst, prefix string, ret map[string]*nodeInst) {
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG] resolvePathsAndFlatten() %v%v (%v)\n", prefix, node.Name, node)
	}
	newPrefix := prefix + node.Name
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG]    newPrefix: '%v'\n", newPrefix)
	}
	if newPrefix != "" {
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG]    replacing node %v by %v\n", ret[newPrefix], node)
		}
		ret[newPrefix] = node
	} else {
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG]    replacing node %v by %v (empty prefix)\n", ret["/"], node)
		}
		ret["/"] = node
	}
	if node.State == opCreate {
		// TODO diff equality only
		return
	}
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG]    iterating over node %d children\n", len(node.Children))
	}
	var trailingSlash string = "/"
	if node.Name == "/" {
		trailingSlash = ""
	}
	for _, child := range node.Children {
		resolvePathsAndFlatten(child, newPrefix+trailingSlash, ret)
	}
}

// peekAndDiscard return n bytes from the stream buffer, if required increase its size
func peekAndDiscard(input *bufio.Reader, n int) ([]byte, error) {
	buffered := input.Buffered()
	if n > buffered {
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG] peekAndDiscard() need to read more bytes '%v' than there are buffered '%v'\n", n, buffered)
			fmt.Fprintf(os.Stderr, "[DEBUG] peekAndDiscard() increasing the buffer size to match the need\n")
		}
		input = bufio.NewReaderSize(input, n)
	}
	data, err := input.Peek(n)
	if err != nil {
		return nil, err
	}
	if _, err := input.Discard(n); err != nil {
		return nil, err
	}
	return data, nil
}

// readCommand return a command from reading and parsing the stream input
func readCommand(input *bufio.Reader) (*commandInst, error) {
	cmdSizeB, err := peekAndDiscard(input, 4)
	if err != nil {
		return nil, fmt.Errorf("short read on command size: %v", err)
	}
	cmdSize := binary.LittleEndian.Uint32(cmdSizeB)
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG] command size: '%v' (%v)\n", cmdSize, cmdSizeB)
	}
	cmdTypeB, err := peekAndDiscard(input, 2)
	if err != nil {
		return nil, fmt.Errorf("short read on command type: %v", err)
	}
	cmdType := binary.LittleEndian.Uint16(cmdTypeB)
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG] command type: '%v' (%v)\n", cmdType, cmdTypeB)
	}
	if cmdType > C.BTRFS_SEND_C_MAX {
		return nil, fmt.Errorf("stream contains invalid command type %v", cmdType)
	}
	_, err = peekAndDiscard(input, 4)
	if err != nil {
		return nil, fmt.Errorf("short read on command checksum: %v", err)
	}
	cmdData, err := peekAndDiscard(input, int(cmdSize))
	if err != nil {
		return nil, fmt.Errorf("short read on command data: %v", err)
	}
	return &commandInst{
		Type: &commandsDefs[cmdType],
		data: cmdData,
	}, nil
}

// ReadParam return a parameter of a command, if it matches the one expected
func (command *commandInst) ReadParam(expectedType int) (string, error) {
	if len(command.data) < 4 {
		return "", fmt.Errorf("no more parameters")
	}
	paramType := binary.LittleEndian.Uint16(command.data[0:2])
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG]            param type: '%v' (expected: %v, raw: %v)\n", expectedType, paramType, command.data[0:2])
	}
	if int(paramType) != expectedType {
		return "", fmt.Errorf("expect type %v; got %v", expectedType, paramType)
	}
	paramLength := binary.LittleEndian.Uint16(command.data[2:4])
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG]            param length: '%v' (raw: %v)\n", paramLength, command.data[2:4])
	}
	if int(paramLength)+4 > len(command.data) {
		return "", fmt.Errorf("short command param; length was %v but only %v left", paramLength, len(command.data)-4)
	}
	ret := strings.Trim(string(command.data[4 : 4+paramLength]), "\x00")
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG]            param: '%v' (str:'%v', raw: %v)\n", ret, string(command.data[4 : 4+paramLength]), command.data[4 : 4+paramLength])
	}
	command.data = command.data[4+paramLength:]
	return ret, nil
}

// readStream reads the stream and produce a diff, using the channel specified
func readStream(stream *os.File, diff *diffInst, channel chan error) {
	channel <- doReadStream(stream, diff)
}

// doReadStream reads the stream and produce a diff
func doReadStream(stream *os.File, diff *diffInst) error {
	// ensure that we catch the error from stream.Close()
	var err error
	defer func() {
		cerr := stream.Close()
		if err == nil {
			err = cerr
		}
	}()
	input := bufio.NewReader(stream)
	btrfsStreamHeader, err := input.ReadString('\x00')
	if err != nil {
		return err
	}
	if btrfsStreamHeader[:len(btrfsStreamHeader)-1] != C.BTRFS_SEND_STREAM_MAGIC {
		return fmt.Errorf("magic is %v, not %v", btrfsStreamHeader, C.BTRFS_SEND_STREAM_MAGIC)
	}
	verB, err := peekAndDiscard(input, 4)
	if err != nil {
		return err
	}
	ver := binary.LittleEndian.Uint32(verB)
	if ver != 1 {
		return fmt.Errorf("unexpected stream version %v", ver)
	}
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG] reading each command until EOF ...\n")
	}
	var ret error = nil
	var stop bool = false
	for {
		// read input and get the command type and data
		var command *commandInst
		command, err = readCommand(input)
		if err != nil {
			ret = err
			break
		}
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG] %v -> %v\n", command.Type.Name, command.Type.Op)
		}

		// analyze the command ...
		switch command.Type.Op {

		// unspecified: bug
		case opUnspec:
			ret = fmt.Errorf("unexpected command %v", command)

		// ignored operation
		case opIgnore:
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]    ignoring (as specified in command definitions)\n")
			}

		// two path ops
		case opRename:
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]    '%v' operation\n", command.Type.Op)
				fmt.Fprintf(os.Stderr, "[DEBUG]        reading param (C.BTRFS_SEND_A_PATH) ...\n")
			}

			// reading 'from' and 'to' params
			var fromPath string
			var toPath string
			fromPath, err = command.ReadParam(C.BTRFS_SEND_A_PATH)
			if err != nil {
				ret = err
				break
			}
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]        from: '%v'\n", fromPath)
				fmt.Fprintf(os.Stderr, "[DEBUG]        reading param (C.BTRFS_SEND_A_PATH_TO) ...\n")
			}
			toPath, err = command.ReadParam(C.BTRFS_SEND_A_PATH_TO)
			if err != nil {
				ret = err
				break
			}
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]        to: '%v'\n", toPath)
			}

			// process to represent the renaming in the diff double tree
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]        updating the diff double tree\n")
			}
			diff.processTwoParamsOp(command.Type.Op, fromPath, toPath)
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]        '%v' operation processed\n", command.Type.Op)
			}

		// end operation
		case opEnd:
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]    END operation\n")
			}
			stop = true
			break

		// other operations
		default:
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]    other operation (%v)\n", command.Type.Op)
			}

			// read the 'path' param
			var path string
			if (command.Type.Name == "BTRFS_SEND_C_CLONE" && command.Type.Op == opModify) {
				if debug {
					fmt.Fprintf(os.Stderr, "[DEBUG]        reading param (C.BTRFS_SEND_A_FILE_OFFSET) ...\n")
				}
				_, err = command.ReadParam(C.BTRFS_SEND_A_FILE_OFFSET)
				if err != nil {
					ret = err
					break
				}
				if debug {
					fmt.Fprintf(os.Stderr, "[DEBUG]        reading param (C.BTRFS_SEND_A_CLONE_LEN) ...\n")
				}
				_, err = command.ReadParam(C.BTRFS_SEND_A_CLONE_LEN)
				if err != nil {
					ret = err
					break
				}
				if debug {
					fmt.Fprintf(os.Stderr, "[DEBUG]        reading param (C.BTRFS_SEND_A_CLONE_PATH) ...\n")
				}
				path, err = command.ReadParam(C.BTRFS_SEND_A_CLONE_PATH)
			} else {
				if debug {
					fmt.Fprintf(os.Stderr, "[DEBUG]        reading param (C.BTRFS_SEND_A_PATH) ...\n")
				}
				path, err = command.ReadParam(C.BTRFS_SEND_A_PATH)
			}
			if err != nil {
				ret = err
				break
			}
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]        path: '%v'\n", path)
			}

			// adding the operation to the list of changes
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]        processing operation '%v'\n", command.Type.Op)
			}
			err = diff.processSingleParamOp(command.Type.Op, path)
			if err != nil {
				ret = err
				break
			}
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]        operation '%v' processed\n", command.Type.Op)
			}
		}
		if stop || err != nil {
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]    breaking out of the loop\n")
			}
			break
		}
	}
	return ret
}

// getSubVolUID return the subvolume UID
func getSubVolUID(path string) (C.__u64, error) {
	var sus C.struct_subvol_uuid_search
	var subvolInfo *C.struct_subvol_info
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG] opening path '%s'\n", path)
	}
	subvolDir, err := os.OpenFile(path, os.O_RDONLY, 0777)
	if err != nil {
		return 0, fmt.Errorf("open returned %v", err)
	}
	r := C.subvol_uuid_search_init(C.int(subvolDir.Fd()), &sus)
	if r < 0 {
		return 0, fmt.Errorf("subvol_uuid_search_init returned %v", r)
	}
	subvolInfo, err = C.subvol_uuid_search(&sus, 0, nil, 0, C.CString(path), C.subvol_search_by_path)
	if subvolInfo == nil {
		return 0, fmt.Errorf("subvol_uuid_search returned %v", err)
	}
	return C.__u64(subvolInfo.root_id), nil
}

// btrfsSendSyscall write the file stream with the system call 'btrfs send'
func btrfsSendSyscall(stream *os.File, source string, subvolume string) error {
	// ensure that we catch the error from stream.Close()
	var err error
	defer func() {
		cerr := stream.Close()
		if err == nil {
			err = cerr
		}
	}()
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG] opening subvolume '%s'\n", subvolume)
	}
	subvolDir, err := os.OpenFile(subvolume, os.O_RDONLY, 0777)
	if err != nil {
		return fmt.Errorf("open returned %v", err)
	}
	sourceUID, err := getSubVolUID(source)
	if err != nil {
		fmt.Fprintf(os.Stderr, "getSubVolUID returns %v\n", err)
		os.Exit(1)
	}
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG] sourceUID %v\n", sourceUID)
	}
	var subvolFd C.uint = C.uint(subvolDir.Fd())
	var opts C.struct_btrfs_ioctl_send_args
	opts.send_fd = C.__s64(stream.Fd())
	opts.clone_sources = &sourceUID
	opts.clone_sources_count = 1
	opts.parent_root = sourceUID
	opts.flags = C.BTRFS_SEND_FLAG_NO_FILE_DATA
	ret, _, err := syscall.Syscall(syscall.SYS_IOCTL, uintptr(subvolFd), C.BTRFS_IOC_SEND, uintptr(unsafe.Pointer(&opts)))
	if ret != 0 {
		return err
	}
	return nil
}

// btrfsSendDiff returns the differences between two subvolumes
func btrfsSendDiff(source, subvolume string) (*diffInst, error) {
	read, write, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("pipe returned %v", err)
	}

	var diff diffInst = diffInst{}
	channel := make(chan error)
	go readStream(read, &diff, channel)
	err = btrfsSendSyscall(write, source, subvolume)
	if err != nil {
		return nil, fmt.Errorf("btrfsSendSyscall returns %v", err)
	}
	err = <-channel
	if err != nil {
		return nil, fmt.Errorf("readStream returns %v", err)
	}
	return &diff, nil
}

// btrfsSendDiff returns the differences from a file stream
func btrfsStreamFileDiff(streamfile string) (*diffInst, error) {
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG] opening file '%v'\n", streamfile)
	}
	f, err := os.Open(streamfile)
	if err != nil {
		return nil, fmt.Errorf("open returned %v", err)
	}

	// ensure that we catch the error from f.Close()
	defer func() {
		cerr := f.Close()
		if err == nil {
			err = cerr
		}
	}()

	var diff diffInst = diffInst{}
	channel := make(chan error)
	go readStream(f, &diff, channel)
	if err != nil {
		return nil, fmt.Errorf("btrfsGetSyscall returns %v", err)
	}
	err = <-channel
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("readStream returns %v", err)
	}
	return &diff, nil
}

// GetChangesFromTwoSubvolumes return a list of changes (a diff) between two BTRFS subvolumes
func GetChangesFromTwoSubvolumes(child string, parent string) ([]string, error) {
	parentStat, err := os.Stat(parent)
	if err != nil {
		return nil, err
	}
	if !parentStat.IsDir() {
		return nil, fmt.Errorf("'%s' is not a directory", parent)
	}
	childStat, err := os.Stat(child)
	if err != nil {
		return nil, err
	}
	if !childStat.IsDir() {
		return nil, fmt.Errorf("'%s' is not a directory", child)
	}
	diff, err := btrfsSendDiff(parent, child)
	if err != nil {
		return nil, err
	}
	return diff.Changes(), nil
}

// GetChangesFromStreamFile return a list of changes (a diff) from a BTRFS send stream file
func GetChangesFromStreamFile(streamfile string) ([]string, error) {
	fileStat, err := os.Lstat(streamfile)
	if err != nil {
		return nil, err
	}
	if !fileStat.Mode().IsRegular() {
		return nil, fmt.Errorf("'%s' is not a file", streamfile)
	}
	diff, err := btrfsStreamFileDiff(streamfile)
	if err != nil {
		return nil, err
	}
	return diff.Changes(), nil
}

// SetDebug set the debug mode flag
func SetDebug(status bool) {
	debug = status
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG] DEBUG mode enabled\n")
	}
}

// ConsiderUtimeOp consider the Utime instruction (eventually turned into a 'changed' operation)
func ConsiderUtimeOp(asOpModify bool) {
	var opRef operation = opTimes
	if asOpModify {
		opRef = opModify
	}
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG] Utimes will be considered as '%v'\n", opRef)
	}
	commandsDefs[C.BTRFS_SEND_C_UTIMES] = commandMapOp{Name: "BTRFS_SEND_C_UTIMES", Op: opRef}
}

// ConsiderChmodOp consider the Chmod instruction (eventually turned into a 'changed' operation)
func ConsiderChmodOp(asOpModify bool) {
	var opRef operation = opPermissions
	if asOpModify {
		opRef = opModify
	}
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG] Chmod will be considered as '%v'\n", opRef)
	}
	commandsDefs[C.BTRFS_SEND_C_CHMOD] = commandMapOp{Name: "BTRFS_SEND_C_CHMOD", Op: opRef}
}

// ConsiderChownOp consider the Chown instruction (eventually turned into a 'changed' operation)
func ConsiderChownOp(asOpModify bool) {
	var opRef operation = opOwnership
	if asOpModify {
		opRef = opModify
	}
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG] Chown will be considered as '%v'\n", opRef)
	}
	commandsDefs[C.BTRFS_SEND_C_CHOWN] = commandMapOp{Name: "BTRFS_SEND_C_CHOWN", Op: opRef}
}

// ConsiderXattrOp consider the Xattr instruction (eventually turned into a 'changed' operation)
func ConsiderXattrOp(asOpModify bool) {
	var opRef operation = opAttributes
	if asOpModify {
		opRef = opModify
	}
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG] Xattr will be considered as '%v'\n", opRef)
	}
	commandsDefs[C.BTRFS_SEND_C_SET_XATTR] = commandMapOp{Name: "BTRFS_SEND_C_SET_XATTR", Op: opRef}
	commandsDefs[C.BTRFS_SEND_C_REMOVE_XATTR] = commandMapOp{Name: "BTRFS_SEND_C_REMOVE_XATTR", Op: opRef}
}
