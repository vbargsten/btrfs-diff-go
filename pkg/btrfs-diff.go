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

var debugMode bool = false
var debugPrefix string = "[DEBUG] "
var debugIndTab string = "    "

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

// debug print a message (to STDERR) only if debug mode is enabled
func debug(msg string, params ...interface{}) {
	if debugMode {
		fmt.Fprintf(os.Stderr, "%s%s\n", debugPrefix, fmt.Sprintf(msg, params...))
	}
}

// debugInd is like 'debug()' but can handle indentation as well
func debugInd(ind int, msg string, params ...interface{}) {
	if debugMode {
		indentation := ""
		for i:=0; i<ind; i++ {
			indentation += debugIndTab
		}
		fmt.Fprintf(os.Stderr, "%s%s%s\n", debugPrefix, indentation, fmt.Sprintf(msg, params...))
	}
}

// processSingleParamOp is the processing of single param commands (update the Diff double tree)
// Note: that code only allow to register for one operation per file. For example, a file can't
//       have its time modified and its ownership at the same time, even if this is actually the
//       case in reality. That simplified design looses information. The last operation will
//       not override the previous one.
func (diff *diffInst) processSingleParamOp(Op operation, path string) (error) {
	debugInd(3, "searching for matching node")
	isNew := Op == opCreate
	debugInd(4, "is new? %t (only when '%v')", isNew, opCreate)
	fileNode := diff.updateBothTreesAndReturnNode(path, isNew)
	debugInd(3, "found: %v", fileNode)

	// in case of deletion
	if Op == opDelete {
		debugInd(3, "that's a deletion")
		parentWasRenamed := false
		if fileNode.Original == nil {
			debugInd(4, "no previous version of the node exist")
			// if parent wasn't renamed its a bug
			if fileNode.Parent == nil || fileNode.Parent.State != opCreate || fileNode.Parent.Original == nil || fileNode.Parent.Original.State != opRename {
			debugInd(4, "BUG? deleting path %v which was created in same diff?", path)
				os.Exit(1)

			// parent node was renamed
			} else {
				parentWasRenamed = true
				debugInd(5, "parent was renamed, that's normal")
			}
		}

		// parent was renamed: not deleting the node in the new files
		if parentWasRenamed {
			debugInd(5, "parent was renamed, not deleting the node (New files)")

		// parent not renamed
		} else {

			// deleting the node in new files tree
			debugInd(4, "deleting the node in the Parent tree (New tree)")
			delete(fileNode.Parent.Children, fileNode.Name)
		}

		// flag the new one as deleted
		debugInd(4, "flaging the node as '%v'", opDelete)
		fileNode.State = opDelete
		debugInd(4, "now node is: %v", fileNode)

		// If we deleted /this/ node, it sure as hell needs no children.
		debugInd(4, "deleting the node children")
		fileNode.Children = nil

		// old version exist
		if fileNode.Original != nil {
			debugInd(5, "node had a previous version")
			debugInd(5, "setting its State to '%v'", opDelete)
			debugInd(5, "deleting the old node children")
			// Leave behind a sentinel in the Original structure.
			fileNode.Original.State = opDelete
			err := fileNode.Original.verifyDelete(path)
			if err != nil {
				return err
			}
			fileNode.Original.Children = nil
		}

		debugInd(5, "now node is: %v", fileNode)

	// not a deletion, and the current operation is different from the current node
	} else if Op != fileNode.State {

		// not a creation
		if (fileNode.State != opCreate) {
			debugInd(3, "the current node Op is not '%v'", fileNode.State)

			// do not allow a "subclass" of modifications (i.e.: times, perms, own, attrs) to override the top class modification (i.e.: changed)
			if (fileNode.State == opModify && (Op == opTimes || Op == opPermissions || Op == opOwnership || Op == opAttributes)) {
				debugInd(4, "current operation '%v' is a subclass modification", Op)
				debugInd(4, "not overriding the node Op (%v)", fileNode.State)

			// overriding the current node operation
			} else {
				debugInd(4, "replacing it with current operation '%v'", Op)
				fileNode.State = Op
				debugInd(4, "now node is: %v", fileNode)
			}

		// current node Op is opCreate
		} else {
			debugInd(3, "current node Op is '%v': not overriding the node Op", fileNode.State)
		}

	// current operation is the same as the current node
	} else {
		debugInd(3, "current operation (%v) is the same as the node Op (%v)", Op, fileNode.State)
		debugInd(3, "not overriding the node Op")
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
	debugInd(3, "searching for 'from' node")
	fromNode := diff.updateBothTreesAndReturnNode(from, false)
	debugInd(3, "found: %v", fromNode)

	// renaming specifics
	if Op == opRename {
		debugInd(3, "it's a renaming")

		// deleting the node from the new files tree, because it should only exist in the old tree
		debugInd(4, "removing it from its parent node '%v' (New tree)", fromNode.Parent.Name)
		delete(fromNode.Parent.Children, fromNode.Name)
		// Note: now the fromNode is orphan

		if fromNode.Original != nil {

			// if fromNode had an original, we must mark that path destroyed.
			debugInd(4, "set old node St to '%v' (Original tree)", opRename)
			fromNode.Original.State = opRename
			debugInd(4, "old node is now: %v", fromNode.Original)
		}
	}

	// to node
	debugInd(3, "searching for 'to' node")
	toNode := diff.updateBothTreesAndReturnNode(to, true)
	debugInd(3, "found: %v", toNode)

	// renaming specifics
	if Op == opRename {
		debugInd(3, "it's a renaming (bis)")

		// attaching the old node (which was orphan) to the new files tree
		debugInd(4, "adding the 'from' node to the 'to' node parent tree at name '%v' (New tree)", toNode.Name)
		toNode.Parent.Children[toNode.Name] = fromNode

		// updating its name
		debugInd(4, "'from' node Name is replaced by the 'to' node Name '%v'", toNode.Name)
		fromNode.Name = toNode.Name

		// ensuring its status is 'added'
		debugInd(4, "'from' node Op is set to '%v'", opCreate)
		fromNode.State = opCreate

		// updating the parent node
		debugInd(4, "'from' node Parent is assigned the 'to' node Parent '%v'", toNode.Parent.Name)
		fromNode.Parent = toNode.Parent

		debugInd(4, "now node is: %v", toNode.Parent.Children[toNode.Name])
	}
}

// updateBothTreesAndReturnNode return the searched node, after it has updated both Diff trees (old and new)
func (diff *diffInst) updateBothTreesAndReturnNode(path string, isNew bool) *nodeInst {
	if diff.New.Original == nil {
		debugInd(4, "the New tree is not referencing the Original one, fixing that")
		diff.New.Original = &diff.Original
	}
	if path == "" {
		debugInd(4, "empty path, returning the node from the top level of the New tree '%v'", diff.New.Name)
		return &diff.New
	}

	// foreach part of the path (in the 'New' tree)
	debugInd(4, "splitted path in parts, processing each one ...")
	parts := strings.Split(path, "/")

	parent := &diff.New
	for i, part := range parts {
		nodeName := strings.Trim(part, "\x00")
		debugInd(5, "- %v", nodeName)
		debugInd(6, "parent node is %v", parent)
		if parent.Children == nil {
			debugInd(7, "no children: set a new empty children list/map")
			parent.Children = make(map[string]*nodeInst)
		}

		// get the node in the parent tree (New tree)
		debugInd(6, "getting the node in the children (New tree)")
		newNode := parent.Children[nodeName]
		debugInd(7, "found: %v", newNode)

		// the parent part/node doesn't exist
		if newNode == nil {
			debugInd(6, "creating the node")

			// creating it
			parent.Children[nodeName] = &nodeInst{}
			newNode = parent.Children[nodeName]
			newNode.Name = nodeName
			newNode.Parent = parent
			debugInd(7, "created: %v", newNode)
			debugInd(7, "added to its parent node (New tree)")

			// no previous version of the parent node
			oldParent := parent.Original
			if oldParent == nil {
				debugInd(7, "no previous version of the parent node (Original tree)")
				// was !(isNew && i == len(parts)-1) which is the same, but replaced for consistency reason
				if !isNew || i < len(parts)-1 {
					debugInd(7, "isNew is 'false' or the parent part isn't the last one")
					// Either a path has a route in the oldParent, or it's been
					// explicitly created. Once we traverse into a path without
					// an oldParent, we know the full tree, so getting here is a
					// sign we did it wrong.
					fmt.Fprintf(os.Stderr, "BUG? referenced path %v cannot exist\n", path)
					os.Exit(1)
				}

			// the parent node have a previous version
			} else {
				debugInd(7, "the parent node have an old version (Original tree)")
				debugInd(8, "old parent: %v", oldParent)
				if oldParent.Children == nil {
					debugInd(9, "no children: set a new empty children list/map")
					oldParent.Children = make(map[string]*nodeInst)
				}

				// the parent node was renamed
				//if parent.State == opCreate && oldParent.State == opDelete {
				if parent.State == opCreate && oldParent.State == opRename {

					// do not create the old node version
					// because it is supposed to be processed by the new files tree
					debugInd(8, "old parent node was renamed, not creating the old node in it")

				// no renaming of the parent node
				} else {

					// get the node in the old tree (Original tree)
					debugInd(8, "getting the old node (Original tree)")
					oldNode := oldParent.Children[nodeName]
					debugInd(9, "found: %v", oldNode)

					// the node didn't exist before
					if oldNode == nil {
						if !isNew || i < len(parts)-1 {
							debugInd(8, "isNew is 'false' or the parent part isn't the last one")

							// Was meant to already exist, so make sure it did!
							debugInd(8, "creating old node")
							oldParent.Children[nodeName] = &nodeInst{}
							oldNode = oldParent.Children[nodeName]
							oldNode.Name = nodeName
							oldNode.Parent = oldParent
							newNode.Original = oldNode
							debugInd(9, "old node created: %v", oldNode)
							debugInd(9, "added to old parent node '%v' (Original tree)", oldParent.Name)
						} else {
							debugInd(9, "not creating the old node because it is new (%t)", isNew)
						}

					// old node exist
					} else {
						debugInd(8, "previous node version: %v", oldNode)
					}
				}
			}

		// the parent part/node exists
		} else {
			debugInd(6, "node exists (New tree)")
			if isNew && i == len(parts)-1 {
				debugInd(7, "isNew is 'true' and the parent part is the last one")

				// As this is the target of a create, we should expect to see
				// nothing here.
				fmt.Fprintf(os.Stderr, "BUG? overwritten path %v already existed\n", path)
				os.Exit(1)
			}
		}
		parent = newNode
	}
	debugInd(4, "returning node '%v'", parent)
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

// getNodePath return the new path of a node
func getNodePath(node *nodeInst, isNew bool) string {
	if node != nil {
		var path string = node.Name
		var current *nodeInst = node.Parent
		if current != nil {
			for {
				debugInd(3, "current node: %v", current)
				if current.Name != "/" {
					path = current.Name + "/" + path
				} else {
					path = "/" + path
				}
				debugInd(3, "path (increment): %v", path)
				current = current.Parent
				if current == nil {
					break
				}
			}
		}
		if len(path) > 0 {
			debugInd(3, "path (final): '%v'", path)
			return path
		}
	}
	return ""
}

// Changes return the list of changes for a diff double tree
func (diff *diffInst) Changes() []string {
	newFiles := make(map[string]*nodeInst)
	oldFiles := make(map[string]*nodeInst)
	debug("--- new ---")
	resolvePathsAndFlatten(&diff.New, "", newFiles)
	debug("------")
	debug("--- old ---")
	resolvePathsAndFlatten(&diff.Original, "", oldFiles)
	debug("------")
	debug("processing %d old files ...", len(oldFiles))
	var ret []string

	// old files
	for path, node := range oldFiles {
		debugInd(1, "- old node: %v # %v", node, path)

		// getting the same path in the new files
		var newFileNode *nodeInst = newFiles[path]

		debugInd(2, "new file node: %v", newFileNode)

		// all the modified files (found in new files and old node Op is opUnspec)
		if newFileNode != nil && node.State == opUnspec {
			debugInd(2, "found in new files (%v) and old node St is '%v'", newFileNode, opUnspec)
			debugInd(2, "that's a changed file")

			ret = append(ret, fmt.Sprintf("%7v: %v", newFileNode.State, path))
			debugInd(4, "appended (node.St:%v): %7v: %v (%v) (%v)", opUnspec, newFileNode.State, path, newFileNode, node)

			// deleting the node from the new ones, to avoid being processed twice for the same info
			debugInd(3, "deleting node in new files")
			delete(newFiles, path)

		// not a modified file (not in new files, or node Op is not opUnspec)
		} else {
			debugInd(2, "not found in new files (%v) or old node St is not '%v'", newFileNode, opUnspec)
			debugInd(2, "not a changed file (%v)", node.State)

			// renaming, is handled by the new files
			if node.State == opRename {
				debugInd(3, "was renamed, not append it to the list of changes (handled by new files)")

			// not a rename
			} else {
				debugInd(2, "old node St is not '%v' and not '%v', or not in new files (%v), or new file Op is not '%v'", opDelete, opRename, newFileNode, opCreate)
				ret = append(ret, fmt.Sprintf("%7v: %v", node.State, path))
				debugInd(3, "appended (rest): %7v: %v", node.State, path)
			}
		}
	}

	// new files
	debug("processing %d new files ...", len(newFiles))
	for path, node := range newFiles {
		debugInd(1, "- new node: %v # %v", node, path)

		// renaming
		if node.State == opCreate && node.Original != nil && node.Original.State == opRename {
			debugInd(2, "was renamed")
			debugInd(2, "getting old node path...")
			oldNodePath := getNodePath(node.Original, false)
			debugInd(2, "old node path: '%v'", oldNodePath)
			if len(oldNodePath) > 0 {
				ret = append(ret, fmt.Sprintf("%7v: %v to %v", node.Original.State, oldNodePath, path))
				debugInd(2, "appended (new): %7v: %v to %v", node.Original.State, oldNodePath, path)
			}

		} else {
			ret = append(ret, fmt.Sprintf("%7v: %v", node.State, path))
			debugInd(2, "appended (new): %7v: %v", node.State, path)
		}
	}
	return ret
}

// resolvePathsAndFlatten generate a flat slice with full path mapped to nodes
func resolvePathsAndFlatten(node *nodeInst, prefix string, ret map[string]*nodeInst) {
	debug("resolvePathsAndFlatten() %v%v (%v)", prefix, node.Name, node)
	newPrefix := prefix + node.Name
	debugInd(1, "newPrefix: '%v'", newPrefix)
	if newPrefix != "" {
		debugInd(1, "replacing node %v by %v", ret[newPrefix], node)
		ret[newPrefix] = node
	} else {
		debugInd(1, "replacing node %v by %v (empty prefix)", ret["/"], node)
		ret["/"] = node
	}
	if (node.State == opCreate && (node.Original == nil || node.Original.State != opRename)) {
		debugInd(1, "stoping because node St is '%v' (and its original one wasn't renamed)", opCreate)
		return
	}
	debugInd(1, "iterating over node %d children", len(node.Children))
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
		debug("peekAndDiscard() need to read more bytes '%v' than there are buffered '%v'", n, buffered)
		debug("peekAndDiscard() increasing the buffer size to match the need")
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
	debug("command size: '%v' (%v)", cmdSize, cmdSizeB)
	cmdTypeB, err := peekAndDiscard(input, 2)
	if err != nil {
		return nil, fmt.Errorf("short read on command type: %v", err)
	}
	cmdType := binary.LittleEndian.Uint16(cmdTypeB)
	debug("command type: '%v' (%v)", cmdType, cmdTypeB)
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
	debugInd(3, "param type: '%v' (expected: %v, raw: %v)", expectedType, paramType, command.data[0:2])
	if int(paramType) != expectedType {
		return "", fmt.Errorf("expect type %v; got %v", expectedType, paramType)
	}
	paramLength := binary.LittleEndian.Uint16(command.data[2:4])
	debugInd(3, "param length: '%v' (raw: %v)", paramLength, command.data[2:4])
	if int(paramLength)+4 > len(command.data) {
		return "", fmt.Errorf("short command param; length was %v but only %v left", paramLength, len(command.data)-4)
	}
	ret := strings.Trim(string(command.data[4 : 4+paramLength]), "\x00")
	debugInd(3, "param: '%v' (str:'%v', raw: %v)", ret, string(command.data[4 : 4+paramLength]), command.data[4 : 4+paramLength])
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
	debug("reading each command until EOF ...")
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
		debug("%v -> %v", command.Type.Name, command.Type.Op)

		// analyze the command ...
		switch command.Type.Op {

		// unspecified: bug
		case opUnspec:
			ret = fmt.Errorf("unexpected command %v", command)

		// ignored operation
		case opIgnore:
			debugInd(1, "ignoring (as specified in command definitions)")

		// two path ops
		case opRename:
			debugInd(1, "'%v' operation", command.Type.Op)
			debugInd(2, "reading param (C.BTRFS_SEND_A_PATH) ...")

			// reading 'from' and 'to' params
			var fromPath string
			var toPath string
			fromPath, err = command.ReadParam(C.BTRFS_SEND_A_PATH)
			if err != nil {
				ret = err
				break
			}
			debugInd(2, "from: '%v'", fromPath)
			debugInd(2, "reading param (C.BTRFS_SEND_A_PATH_TO) ...")
			toPath, err = command.ReadParam(C.BTRFS_SEND_A_PATH_TO)
			if err != nil {
				ret = err
				break
			}
			debugInd(2, "to: '%v'", toPath)

			// process to represent the renaming in the diff double tree
			debugInd(2, "updating the diff double tree")
			diff.processTwoParamsOp(command.Type.Op, fromPath, toPath)
			debugInd(2, "'%v' operation processed", command.Type.Op)

		// end operation
		case opEnd:
			debugInd(1, "END operation")
			stop = true
			break

		// other operations
		default:
			debugInd(1, "other operation (%v)", command.Type.Op)

			// read the 'path' param
			var path string
			if (command.Type.Name == "BTRFS_SEND_C_CLONE" && command.Type.Op == opModify) {
				debugInd(2, "reading param (C.BTRFS_SEND_A_FILE_OFFSET) ...")
				_, err = command.ReadParam(C.BTRFS_SEND_A_FILE_OFFSET)
				if err != nil {
					ret = err
					break
				}
				debugInd(2, "reading param (C.BTRFS_SEND_A_CLONE_LEN) ...")
				_, err = command.ReadParam(C.BTRFS_SEND_A_CLONE_LEN)
				if err != nil {
					ret = err
					break
				}
				debugInd(2, "reading param (C.BTRFS_SEND_A_CLONE_PATH) ...")
				path, err = command.ReadParam(C.BTRFS_SEND_A_CLONE_PATH)
			} else {
				debugInd(2, "reading param (C.BTRFS_SEND_A_PATH) ...")
				path, err = command.ReadParam(C.BTRFS_SEND_A_PATH)
			}
			if err != nil {
				ret = err
				break
			}
			debugInd(2, "path: '%v'", path)

			// adding the operation to the list of changes
			debugInd(2, "processing operation '%v'", command.Type.Op)
			err = diff.processSingleParamOp(command.Type.Op, path)
			if err != nil {
				ret = err
				break
			}
			debugInd(2, "operation '%v' processed", command.Type.Op)
		}
		if stop || err != nil {
			debugInd(1, "breaking out of the loop")
			break
		}
	}
	return ret
}

// getSubVolUID return the subvolume UID
func getSubVolUID(path string) (C.__u64, error) {
	var sus C.struct_subvol_uuid_search
	var subvolInfo *C.struct_subvol_info
	debug("opening path '%s'", path)
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
	debug("opening subvolume '%s'", subvolume)
	subvolDir, err := os.OpenFile(subvolume, os.O_RDONLY, 0777)
	if err != nil {
		return fmt.Errorf("open returned %v", err)
	}
	sourceUID, err := getSubVolUID(source)
	if err != nil {
		fmt.Fprintf(os.Stderr, "getSubVolUID returns %v\n", err)
		os.Exit(1)
	}
	debug("sourceUID %v", sourceUID)
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
	debug("opening file '%v'", streamfile)
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
	debugMode = status
	debug("DEBUG mode enabled")
}

// ConsiderUtimeOp consider the Utime instruction (eventually turned into a 'changed' operation)
func ConsiderUtimeOp(asOpModify bool) {
	var opRef operation = opTimes
	if asOpModify {
		opRef = opModify
	}
	debug("Utimes will be considered as '%v'", opRef)
	commandsDefs[C.BTRFS_SEND_C_UTIMES] = commandMapOp{Name: "BTRFS_SEND_C_UTIMES", Op: opRef}
}

// ConsiderChmodOp consider the Chmod instruction (eventually turned into a 'changed' operation)
func ConsiderChmodOp(asOpModify bool) {
	var opRef operation = opPermissions
	if asOpModify {
		opRef = opModify
	}
	debug("Chmod will be considered as '%v'", opRef)
	commandsDefs[C.BTRFS_SEND_C_CHMOD] = commandMapOp{Name: "BTRFS_SEND_C_CHMOD", Op: opRef}
}

// ConsiderChownOp consider the Chown instruction (eventually turned into a 'changed' operation)
func ConsiderChownOp(asOpModify bool) {
	var opRef operation = opOwnership
	if asOpModify {
		opRef = opModify
	}
	debug("Chown will be considered as '%v'", opRef)
	commandsDefs[C.BTRFS_SEND_C_CHOWN] = commandMapOp{Name: "BTRFS_SEND_C_CHOWN", Op: opRef}
}

// ConsiderXattrOp consider the Xattr instruction (eventually turned into a 'changed' operation)
func ConsiderXattrOp(asOpModify bool) {
	var opRef operation = opAttributes
	if asOpModify {
		opRef = opModify
	}
	debug("Xattr will be considered as '%v'", opRef)
	commandsDefs[C.BTRFS_SEND_C_SET_XATTR] = commandMapOp{Name: "BTRFS_SEND_C_SET_XATTR", Op: opRef}
	commandsDefs[C.BTRFS_SEND_C_REMOVE_XATTR] = commandMapOp{Name: "BTRFS_SEND_C_REMOVE_XATTR", Op: opRef}
}
