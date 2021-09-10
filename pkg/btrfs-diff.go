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

// NAUGHTYNESS:
// For a recursive delete, we get a rename, then a delete on the renamed copy.
// * We need understand that if we rm a renamed path, we should unrename anything inside it for the diff.
// For a create, we get a garbage name, then a rename.
// * We need to understand that if we get a rename of a file that was new, we must rename all the stuff we did to it.

var debug bool = false

type operation int

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
	opRename // Special cased -- we need two paths
	opEnd
)

var names []string = []string{"!!!", "ignored", "added", "changed", "times", "perms", "own", "attr", "deleted", "renamed", "END"}

func (op operation) String() string {
	return names[op]
}

type commandMapOp struct {
	Name string
	Op   operation
}

type commandInst struct {
	Type *commandMapOp
	data []byte
}

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

	commandsDefs[C.BTRFS_SEND_C_RENAME] = commandMapOp{Name: "BTRFS_SEND_C_RENAME", Op: opRename}
	commandsDefs[C.BTRFS_SEND_C_LINK] = commandMapOp{Name: "BTRFS_SEND_C_LINK", Op: opCreate}
	commandsDefs[C.BTRFS_SEND_C_UNLINK] = commandMapOp{Name: "BTRFS_SEND_C_UNLINK", Op: opDelete}
	commandsDefs[C.BTRFS_SEND_C_RMDIR] = commandMapOp{Name: "BTRFS_SEND_C_RMDIR", Op: opDelete}

	commandsDefs[C.BTRFS_SEND_C_SET_XATTR] = commandMapOp{Name: "BTRFS_SEND_C_SET_XATTR", Op: opIgnore}
	commandsDefs[C.BTRFS_SEND_C_REMOVE_XATTR] = commandMapOp{Name: "BTRFS_SEND_C_REMOVE_XATTR", Op: opIgnore}

	commandsDefs[C.BTRFS_SEND_C_WRITE] = commandMapOp{Name: "BTRFS_SEND_C_WRITE", Op: opModify}
	commandsDefs[C.BTRFS_SEND_C_CLONE] = commandMapOp{Name: "BTRFS_SEND_C_CLONE", Op: opModify}

	commandsDefs[C.BTRFS_SEND_C_TRUNCATE] = commandMapOp{Name: "BTRFS_SEND_C_TRUNCATE", Op: opModify}
	commandsDefs[C.BTRFS_SEND_C_CHMOD] = commandMapOp{Name: "BTRFS_SEND_C_CHMOD", Op: opIgnore}
	commandsDefs[C.BTRFS_SEND_C_CHOWN] = commandMapOp{Name: "BTRFS_SEND_C_CHOWN", Op: opIgnore}
	commandsDefs[C.BTRFS_SEND_C_UTIMES] = commandMapOp{Name: "BTRFS_SEND_C_UTIMES", Op: opIgnore}

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

var commandsDefs *[C.__BTRFS_SEND_C_MAX]commandMapOp = initCommandsDefinitions()

type nodeInst struct {
	Children   map[string]*nodeInst
	Name       string
	State      operation
	Parent     *nodeInst
	Original   *nodeInst
}

type diffInst struct {
	Original nodeInst
	New      nodeInst
}

func (diff *diffInst) processSingleParamOp(path string, Op operation) {
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG]            searching for matching node\n")
	}
	isNew := Op == opCreate
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG]                is new? %t (only when '%v')\n", isNew, opCreate)
	}
	fileNode := diff.updateBothTreesAndReturnNode(path, isNew)


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
		if fileNode.Original != nil {
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]            node had an Original tree\n")
				fmt.Fprintf(os.Stderr, "[DEBUG]            setting its State to '%v'\n", opDelete)
				fmt.Fprintf(os.Stderr, "[DEBUG]            deleting the node children\n")
			}
			// Leave behind a sentinel in the Original structure.
			fileNode.Original.State = opDelete
			fileNode.Original.verifyDelete(path)
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
}

func (node *nodeInst) verifyDelete(path string) {
	for _, child := range node.Children {
		if child.State != opDelete && child.State != opRename {
			fmt.Fprintf(os.Stderr, "[DEBUG]            BUG? deleting parent of node %v in %v which is not gone", node, path)
		}
	}
}

func (diff *diffInst) processTwoParamsOp(from string, to string) {
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG]            searching for 'from' node\n")
	}
	fromNode := diff.updateBothTreesAndReturnNode(from, false)
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG]            removing it from its parent node\n")
	}
	delete(fromNode.Parent.Children, fromNode.Name)
	if fromNode.Original != nil {
		// if fromNode had an original, we must mark that path destroyed.
		fromNode.Original.State = opRename
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG]            the original node have its State set to '%v'\n", opRename)
		}
	}
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG]            searching for 'to' node\n")
	}
	toNode := diff.updateBothTreesAndReturnNode(to, true)
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG]            adding it to its parent node\n")
	}
	toNode.Parent.Children[toNode.Name] = fromNode
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG]            'from' node Name is replaced by the 'to' node Name\n")
	}
	fromNode.Name = toNode.Name
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG]            'from' node State is set to '%v'\n", opCreate)
	}
	fromNode.State = opCreate
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG]            'from' node Parent is assigned the 'to' node Parent\n")
	}
	fromNode.Parent = toNode.Parent
	//fmt.Fprintf(os.Stderr, "intermediate=%v\n", diff)
}

func (diff *diffInst) updateBothTreesAndReturnNode(path string, isNew bool) *nodeInst {
	if diff.New.Original == nil {
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG]                the New tree is not referencing the Original one, fixing that\n")
		}
		diff.New.Original = &diff.Original
	}
	if path == "" {
		if debug {
			newNodeName := diff.New.Name
			if len(newNodeName) > 0 {
				newNodeName = "/"
			}
			fmt.Fprintf(os.Stderr, "[DEBUG]                empty path, returning the node from the top level of the New tree '%v'\n", newNodeName)
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
				fmt.Fprintf(os.Stderr, "[DEBUG]                        parent node is '/%v'\n", parent.Name)
		}
		if parent.Children == nil {
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]                            no children: set a new empty children list/map\n")
			}
			parent.Children = make(map[string]*nodeInst)
		}

		// get the node in the parent tree (New tree)
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG]                        getting the node in the parent tree (New tree)\n")
		}
		newNode := parent.Children[nodeName]

		// the parent part/node doesn't exist
		if newNode == nil {
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]                        node '%v' doesn't exist: creating it\n", nodeName)
			}

			// creating it
			parent.Children[nodeName] = &nodeInst{}
			newNode = parent.Children[nodeName]
			newNode.Name = nodeName
			newNode.Parent = parent
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]                        added to its parent node (New tree)\n")
			}

			// no previous tree
			oldParent := parent.Original
			if oldParent == nil {
				if debug {
					fmt.Fprintf(os.Stderr, "[DEBUG]                        no previous tree (Original tree)\n")
				}
				// was !(isNew && i == len(parts)-1) which is the same, but replaced for consistency reason
				if !isNew || i < len(parts)-1 {
					if debug {
						fmt.Fprintf(os.Stderr, "[DEBUG]                        isNew is 'false' or the parent part isn't the last one\n")
					}
					// Either a path has a route in the oldParent, or it's been
					// explicitly created. Once we traverse into a path without
					// an oldParent, we know the full tree, so getting here is a
					// sign we did it wrong.
					fmt.Fprintf(os.Stderr, "[DEBUG]                        BUG? referenced path %v cannot exist\n", path)
					os.Exit(1)
				}

			// had a previous tree
			} else {
				if debug {
					fmt.Fprintf(os.Stderr, "[DEBUG]                        a previous tree exists (Original tree)\n")
				}
				if oldParent.Children == nil {
					if debug {
						fmt.Fprintf(os.Stderr, "[DEBUG]                            no children: set a new empty children list/map\n")
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
				fmt.Fprintf(os.Stderr, "[DEBUG]                        node '%v' exists (New tree)\n", newNode.Name)
			}
			if isNew && i == len(parts)-1 {
				if debug {
					fmt.Fprintf(os.Stderr, "[DEBUG]                        isNew is 'true' and the parent part is the last one\n")
				}

				// As this is the target of a create, we should expect to see
				// nothing here.
				fmt.Fprintf(os.Stderr, "[DEBUG]                        BUG? overwritten path %v already existed\n", path)
			}
		}
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG]                        'parent' node is now '%v'\n", newNode.Name)
		}
		parent = newNode
	}
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG]                returning 'parent' node '%v'\n", parent.Name)
	}
	return parent
}

func (node *nodeInst) String() string {
	return fmt.Sprintf("(%v, %v, %v)", node.Children, node.State, node.Name)
}

func (diff *diffInst) String() string {
	return "\n\t" + strings.Join((diff.Changes())[:], "\n\t") + "\n"
}

func (diff *diffInst) Changes() []string {
	newFiles := make(map[string]*nodeInst)
	oldFiles := make(map[string]*nodeInst)
	resolvePathsAndFlatten(&diff.New, "", newFiles)
	resolvePathsAndFlatten(&diff.Original, "", oldFiles)
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG] new: %v\n[DEBUG] %v\n", newFiles, &diff.New)
		fmt.Fprintf(os.Stderr, "[DEBUG] old: %v\n[DEBUG] %v\n", oldFiles, &diff.Original)
	}
	var ret []string
	for name, node := range oldFiles {
		if newFiles[name] != nil && node.State == opUnspec {
			if node.Children == nil {
				newNodeState := newFiles[name].State
				// specific case when there might be an empty change detected on the root of the subvolume
				if name == "/" && newFiles[name].Name == "" {
					if debug {
						fmt.Fprintf(os.Stderr, "[DEBUG] not appending %v (node.Children: nil, node.State:%v, new_node:%v)\n", name, opUnspec, newFiles[name])
					}
				// time modification or permissions
				} else if (newNodeState == opTimes || newNodeState == opPermissions || newNodeState == opOwnership || newNodeState == opAttributes) {
					ret = append(ret, fmt.Sprintf("%10v: %v", newNodeState, name))
					if debug {
						fmt.Fprintf(os.Stderr, "[DEBUG] appended (node.Children == nil): %10v: %v (%v) (%v)\n", newNodeState, name, newFiles[name], node)
					}
				} else {
					// TODO diff equality only
					ret = append(ret, fmt.Sprintf("%10v: %v", opModify, name))
					if debug {
						fmt.Fprintf(os.Stderr, "[DEBUG] appended (node.Children == nil): %10v: %v (%v) (%v)\n", opModify, name, newFiles[name], node)
					}
				}
			}
			delete(newFiles, name)
		} else {
			if node.State != opDelete && node.State != opRename {
				fmt.Fprintf(os.Stderr, "unexpected State on oldParent %v: %v", name, node.State)
			}
			if (node.State == opDelete || node.State == opRename) && newFiles[name] != nil && newFiles[name].State == opCreate {
				ret = append(ret, fmt.Sprintf("%10v: %v", opModify, name))
				if debug {
					fmt.Fprintf(os.Stderr, "[DEBUG] appended (opDelete||opRename): %10v: %v\n", opModify, name)
				}
				delete(newFiles, name)
			} else {
				//fmt.Fprintf(os.Stderr, "DEBUG DEBUG %v %v %v\n ", node.State, newFiles[name], name)
				ret = append(ret, fmt.Sprintf("%10v: %v", node.State, name))
				if debug {
					fmt.Fprintf(os.Stderr, "[DEBUG] appended (rest): %10v: %v\n", node.State, name)
				}
			}
		}
	}
	for name := range newFiles {
		ret = append(ret, fmt.Sprintf("%10v: %v", opCreate, name))
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG] appended (new): %10v: %v\n", opCreate, name)
		}
	}
	return ret
}

func resolvePathsAndFlatten(node *nodeInst, prefix string, ret map[string]*nodeInst) {
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG] resolvePathsAndFlatten(%v, %v)\n", node.Name, prefix)
	}
	newPrefix := prefix + node.Name
	if newPrefix != "" {
		ret[newPrefix] = node
	}
	if node.State == opCreate {
		// TODO diff equality only
		return
	}
	for _, child := range node.Children {
		resolvePathsAndFlatten(child, newPrefix+"/", ret)
	}
}

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

func (command *commandInst) ReadParam(expectedType int) (string, error) {
	if len(command.data) < 4 {
		return "", fmt.Errorf("no more parameters")
	}
	paramType := binary.LittleEndian.Uint16(command.data[0:2])
	if int(paramType) != expectedType {
		return "", fmt.Errorf("expect type %v; got %v", expectedType, paramType)
	}
	paramLength := binary.LittleEndian.Uint16(command.data[2:4])
	if int(paramLength)+4 > len(command.data) {
		return "", fmt.Errorf("short command param; length was %v but only %v left", paramLength, len(command.data)-4)
	}
	ret := string(command.data[4 : 4+paramLength])
	command.data = command.data[4+paramLength:]
	return ret, nil
}

func readStream(stream *os.File, diff *diffInst, channel chan error) {
	channel <- doReadStream(stream, diff)
}

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
	for {
		// read input and get the command type and data
		var command *commandInst
		command, err = readCommand(input)
		if err != nil {
			return err
		}
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG] %v -> %v\n", command.Type.Name, command.Type.Op)
		}

		// analyze the command ...

		// unspecified: bug
		if command.Type.Op == opUnspec {
			return fmt.Errorf("unexpected command %v", command)

		// ignored operation
		} else if command.Type.Op == opIgnore {
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]    ignoring (as specified in command definitions)\n")
			}
			continue

		// rename operation
		} else if command.Type.Op == opRename {
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]    rename operation\n")
				fmt.Fprintf(os.Stderr, "[DEBUG]        reading param (C.BTRFS_SEND_A_PATH) ...\n")
			}

			// reading 'from' and 'to' params
			var fromPath string
			var toPath string
			fromPath, err = command.ReadParam(C.BTRFS_SEND_A_PATH)
			if err != nil {
				return err
			}
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]        from: '%v'\n", fromPath)
				fmt.Fprintf(os.Stderr, "[DEBUG]        reading param (C.BTRFS_SEND_A_PATH_TO) ...\n")
			}
			toPath, err = command.ReadParam(C.BTRFS_SEND_A_PATH_TO)
			if err != nil {
				return err
			}
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]        to: '%v'\n", toPath)
			}

			// add the renaming operation to the list of changes
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]        processing that operation\n")
			}
			diff.processTwoParamsOp(fromPath, toPath)
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]        operation processed\n")
			}

		// end operation
		} else if command.Type.Op == opEnd {
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]    END operation\n")
			}
			break

		// other operations
		} else {
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]    other operation (%v)\n", command.Type.Op)
				fmt.Fprintf(os.Stderr, "[DEBUG]        reading param (C.BTRFS_SEND_A_PATH) ...\n")
			}

			// read the 'path' param
			var path string
			path, err = command.ReadParam(C.BTRFS_SEND_A_PATH)
			if err != nil {
				if err.Error() != "expect type 15; got 18" {
					return err
				}

				// the usual way to read param have failed, trying one for the specific case of 'write' operation
				if debug {
					fmt.Fprintf(os.Stderr, "[DEBUG]        re-reading param (C.BTRFS_SEND_A_FILE_OFFSET) ...\n")
				}
				path, err = command.ReadParam(C.BTRFS_SEND_A_FILE_OFFSET)
				if err != nil {
					return err
				}
			}
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]        path: '/%v'\n", path)
			}

			// adding the operation to the list of changes
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]        processing that operation\n")
			}
			diff.processSingleParamOp(path, command.Type.Op)
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG]        operation processed\n")
			}
		}
	}
	return nil
}

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
