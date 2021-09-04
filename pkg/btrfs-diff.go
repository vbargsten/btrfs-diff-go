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
	"os"
	"encoding/binary"
	"bufio"
	"fmt"
	"unsafe"
	"syscall"
	"strings"
	"io"
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
	opDelete
	opRename // Special cased -- we need two paths
	opEnd
)

var names []string = []string{"!!!", "ignored", "added", "changed", "deleted", "renamed", "END"}

func (op operation) String() string {
	return names[op]
}

type commandType struct {
	Name string
	Op   operation
}

type commandInst struct {
	Type *commandType
	body []byte
}

func initCommandsDefinitions() *[C.__BTRFS_SEND_C_MAX]commandType {
	var commandsDefs [C.__BTRFS_SEND_C_MAX]commandType
	commandsDefs[C.BTRFS_SEND_C_UNSPEC] = commandType{Name: "BTRFS_SEND_C_UNSPEC", Op: opUnspec}

	commandsDefs[C.BTRFS_SEND_C_SUBVOL] = commandType{Name: "BTRFS_SEND_C_SUBVOL", Op: opIgnore}
	commandsDefs[C.BTRFS_SEND_C_SNAPSHOT] = commandType{Name: "BTRFS_SEND_C_SNAPSHOT", Op: opIgnore}

	commandsDefs[C.BTRFS_SEND_C_MKFILE] = commandType{Name: "BTRFS_SEND_C_MKFILE", Op: opCreate}
	commandsDefs[C.BTRFS_SEND_C_MKDIR] = commandType{Name: "BTRFS_SEND_C_MKDIR", Op: opCreate}
	commandsDefs[C.BTRFS_SEND_C_MKNOD] = commandType{Name: "BTRFS_SEND_C_MKNOD", Op: opCreate}
	commandsDefs[C.BTRFS_SEND_C_MKFIFO] = commandType{Name: "BTRFS_SEND_C_MKFIFO", Op: opCreate}
	commandsDefs[C.BTRFS_SEND_C_MKSOCK] = commandType{Name: "BTRFS_SEND_C_MKSOCK", Op: opCreate}
	commandsDefs[C.BTRFS_SEND_C_SYMLINK] = commandType{Name: "BTRFS_SEND_C_SYMLINK", Op: opCreate}

	commandsDefs[C.BTRFS_SEND_C_RENAME] = commandType{Name: "BTRFS_SEND_C_RENAME", Op: opRename}
	commandsDefs[C.BTRFS_SEND_C_LINK] = commandType{Name: "BTRFS_SEND_C_LINK", Op: opCreate}
	commandsDefs[C.BTRFS_SEND_C_UNLINK] = commandType{Name: "BTRFS_SEND_C_UNLINK", Op: opDelete}
	commandsDefs[C.BTRFS_SEND_C_RMDIR] = commandType{Name: "BTRFS_SEND_C_RMDIR", Op: opDelete}

	commandsDefs[C.BTRFS_SEND_C_SET_XATTR] = commandType{Name: "BTRFS_SEND_C_SET_XATTR", Op: opModify}
	commandsDefs[C.BTRFS_SEND_C_REMOVE_XATTR] = commandType{Name: "BTRFS_SEND_C_REMOVE_XATTR", Op: opModify}

	commandsDefs[C.BTRFS_SEND_C_WRITE] = commandType{Name: "BTRFS_SEND_C_WRITE", Op: opModify}
	commandsDefs[C.BTRFS_SEND_C_CLONE] = commandType{Name: "BTRFS_SEND_C_CLONE", Op: opModify}

	commandsDefs[C.BTRFS_SEND_C_TRUNCATE] = commandType{Name: "BTRFS_SEND_C_TRUNCATE", Op: opModify}
	commandsDefs[C.BTRFS_SEND_C_CHMOD] = commandType{Name: "BTRFS_SEND_C_CHMOD", Op: opModify}
	commandsDefs[C.BTRFS_SEND_C_CHOWN] = commandType{Name: "BTRFS_SEND_C_CHOWN", Op: opModify}
	commandsDefs[C.BTRFS_SEND_C_UTIMES] = commandType{Name: "BTRFS_SEND_C_UTIMES", Op: opModify}

	commandsDefs[C.BTRFS_SEND_C_END] = commandType{Name: "BTRFS_SEND_C_END", Op: opEnd}
	commandsDefs[C.BTRFS_SEND_C_UPDATE_EXTENT] = commandType{Name: "BTRFS_SEND_C_UPDATE_EXTENT", Op: opModify}
	// Sanity check (hopefully no holes).
	for i, command := range commandsDefs {
		if i != C.BTRFS_SEND_C_UNSPEC && command.Op == opUnspec {
			return nil
		}
	}
	return &commandsDefs
}

var commandsDefs *[C.__BTRFS_SEND_C_MAX]commandType = initCommandsDefinitions()

type nodeInst struct {
	Children   map[string]*nodeInst
	Name       string
	ChangeType operation
	Parent     *nodeInst
	Original   *nodeInst
}

type diffInst struct {
	Original nodeInst
	New      nodeInst
}

func (diff *diffInst) tagPath(path string, changeType operation) {
	if debug {
		fmt.Fprintf(os.Stdout, "[DEBUG] TRACE %10v %v\n", changeType, path)
	}
	fileNode := diff.find(path, changeType == opCreate)
	if changeType == opDelete {
		if fileNode.Original == nil {
			fmt.Fprintf(os.Stderr, "deleting path %v which was created in same diff?\n", path)
		}
		delete(fileNode.Parent.Children, fileNode.Name)
	} else { // Why this? if fileNode.Original != nil {
		if !(fileNode.ChangeType == opCreate && changeType == opModify) {
			fileNode.ChangeType = changeType
		}
	}
	if changeType == opDelete {
		// If we deleted /this/ node, it sure as hell needs no children.
		fileNode.Children = nil
		if fileNode.Original != nil {
			// Leave behind a sentinel in the Original structure.
			fileNode.Original.ChangeType = opDelete
			fileNode.Original.verifyDelete(path)
			fileNode.Original.Children = nil
		}
	}
	//fmt.Fprintf(os.Stderr, "intermediate=%v\n", diff)
}

func (node *nodeInst) verifyDelete(path string) {
	for _, child := range node.Children {
		if child.ChangeType != opDelete && child.ChangeType != opRename {
			fmt.Fprintf(os.Stderr, "deleting parent of node %v in %v which is not gone", node, path)
		}
	}
}

func (diff *diffInst) rename(from string, to string) {
	if debug {
		fmt.Fprintf(os.Stdout, "[DEBUG] TRACE %10v %v\n", "rename", from)
		fmt.Fprintf(os.Stdout, "[DEBUG] TRACE %10v %v\n", "rename_to", to)
	}
	fromNode := diff.find(from, false)
	delete(fromNode.Parent.Children, fromNode.Name)
	if fromNode.Original != nil {
		// if fromNode had an original, we must mark that path destroyed.
		fromNode.Original.ChangeType = opRename
	}
	toNode := diff.find(to, true)
	toNode.Parent.Children[toNode.Name] = fromNode
	fromNode.Name = toNode.Name
	fromNode.ChangeType = opCreate
	fromNode.Parent = toNode.Parent
	//fmt.Fprintf(os.Stderr, "intermediate=%v\n", diff)
}

func (diff *diffInst) find(path string, isNew bool) *nodeInst {
	if diff.New.Original == nil {
		diff.New.Original = &diff.Original
	}
	if path == "" {
		return &diff.New
	}
	parts := strings.Split(path, "/")
	current := &diff.New
	for i, part := range parts {
		nodeName := strings.Trim(part, "\x00")
		if current.Children == nil {
			current.Children = make(map[string]*nodeInst)
		}
		newNode := current.Children[nodeName]
		if newNode == nil {
			current.Children[nodeName] = &nodeInst{}
			newNode = current.Children[nodeName]
			original := current.Original
			if original == nil {
				if !(isNew && i == len(parts)-1) {
					// Either a path has a route in the original, or it's been
					// explicitly created. Once we traverse into a path without
					// an original, we know the full tree, so getting here is a
					// sign we did it wrong.
					fmt.Fprintf(os.Stderr, "referenced path %v cannot exist\n", path)
					os.Exit(1)
				}
			} else {
				if original.Children == nil {
					original.Children = make(map[string]*nodeInst)
				}
				newOriginal := original.Children[nodeName]
				if newOriginal == nil {
					if !isNew || i < len(parts)-1 {
						if debug {
							fmt.Fprintf(os.Stderr, "[DEBUG] ACK %v %v %v %v %v\n", original, isNew, path, nodeName, newOriginal)
						}
						// Was meant to already exist, so make sure it did!
						original.Children[nodeName] = &nodeInst{}
						newOriginal = original.Children[nodeName]
						newOriginal.Name = nodeName
						newOriginal.Parent = original
						newNode.Original = newOriginal
					}
				}
			}
			newNode.Name = nodeName
			newNode.Parent = current
		} else if isNew && i == len(parts)-1 {
			// As this is the target of a create, we should expect to see
			// nothing here.
			fmt.Fprintf(os.Stderr, "overwritten path %v already existed\n", path)
		}
		current = newNode
	}
	return current
}

func (node *nodeInst) String() string {
	return fmt.Sprintf("(%v, %v, %v)", node.Children, node.ChangeType, node.Name)
}

func (diff *diffInst) String() string {
	return "\n\t" + strings.Join((diff.Changes())[:], "\n\t") + "\n"
}

func (diff *diffInst) Changes() []string {
	newFiles := make(map[string]*nodeInst)
	oldFiles := make(map[string]*nodeInst)
	changes(&diff.New, "", newFiles)
	changes(&diff.Original, "", oldFiles)
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG] new: %v\n[DEBUG] %v\n", newFiles, &diff.New)
		fmt.Fprintf(os.Stderr, "[DEBUG] old: %v\n[DEBUG] %v\n", oldFiles, &diff.Original)
	}
	var ret []string
	for name, node := range oldFiles {
		if newFiles[name] != nil && node.ChangeType == opUnspec {
			if node.Children == nil {
				// specific case when there might be an empty change detected on the root of the subvolume
				if name == "/" && newFiles[name].Name == "" {
					if debug {
						fmt.Fprintf(os.Stderr, "[DEBUG] not appending %v (node.Children: nil, node.ChangeType:%v, new_node:%v)\n", name, opUnspec, newFiles[name])
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
			if node.ChangeType != opDelete && node.ChangeType != opRename {
				fmt.Fprintf(os.Stderr, "unexpected ChangeType on original %v: %v", name, node.ChangeType)
			}
			if (node.ChangeType == opDelete || node.ChangeType == opRename) && newFiles[name] != nil && newFiles[name].ChangeType == opCreate {
				ret = append(ret, fmt.Sprintf("%10v: %v", opModify, name))
				if debug {
					fmt.Fprintf(os.Stderr, "[DEBUG] appended (opDelete||opRename): %10v: %v\n", opModify, name)
				}
				delete(newFiles, name)
			} else {
				//fmt.Fprintf(os.Stderr, "DEBUG DEBUG %v %v %v\n ", node.ChangeType, newFiles[name], name)
				ret = append(ret, fmt.Sprintf("%10v: %v", node.ChangeType, name))
				if debug {
					fmt.Fprintf(os.Stderr, "[DEBUG] appended (rest): %10v: %v\n", node.ChangeType, name)
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

func changes(node *nodeInst, prefix string, ret map[string]*nodeInst) {
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG] changes(%v, %v)\n", node.Name, prefix)
	}
	newPrefix := prefix + node.Name
	if (newPrefix != "") {
		ret[newPrefix] = node
	}
	if node.ChangeType == opCreate {
		// TODO diff equality only
		return
	}
	for _, child := range node.Children {
		changes(child, newPrefix+"/", ret)
	}
}

func peekAndDiscard(input *bufio.Reader, n int) ([]byte, error) {
	if n > input.Buffered() {
		if debug {
			fmt.Fprintf(os.Stdout, "[DEBUG] peekAndDiscard() need to read more bytes '%v' than there are buffered '%v'\n", n, input.Buffered())
			fmt.Fprintf(os.Stdout, "[DEBUG] peekAndDiscard() increasing the buffer size to match the need\n")
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
		return nil, fmt.Errorf("Short read on command size: %v", err)
	}
	cmdTypeB, err := peekAndDiscard(input, 2)
	if err != nil {
		return nil, fmt.Errorf("Short read on command type: %v", err)
	}
	if _, err := peekAndDiscard(input, 4); err != nil {
		return nil, fmt.Errorf("Short read on command checksum: %v", err)
	}
	cmdSize := binary.LittleEndian.Uint32(cmdSizeB)
	cmdData, err := peekAndDiscard(input, int(cmdSize))
	if err != nil {
		return nil, fmt.Errorf("Short read on command body: %v", err)
	}
	cmdType := binary.LittleEndian.Uint16(cmdTypeB)
	if cmdType < 0 || cmdType > C.BTRFS_SEND_C_MAX {
		return nil, fmt.Errorf("Stream contains invalid command type %v", cmdType)
	}
	if debug {
		fmt.Fprintf(os.Stdout, "[DEBUG] Cmd %v; type %v\n", cmdData, commandsDefs[cmdType].Name)
	}
	return &commandInst{
		Type: &commandsDefs[cmdType],
		body: cmdData,
	}, nil
}

func (command *commandInst) ReadParam(expectedType int) (string, error) {
	if len(command.body) < 4 {
		return "", fmt.Errorf("No more parameters")
	}
	paramType := binary.LittleEndian.Uint16(command.body[0:2])
	if int(paramType) != expectedType {
		return "", fmt.Errorf("Expect type %v; got %v", expectedType, paramType)
	}
	paramLength := binary.LittleEndian.Uint16(command.body[2:4])
	if int(paramLength)+4 > len(command.body) {
		return "", fmt.Errorf("Short command param; length was %v but only %v left", paramLength, len(command.body)-4)
	}
	ret := string(command.body[4 : 4+paramLength])
	command.body = command.body[4+paramLength:]
	return ret, nil
}

func readStream(stream *os.File, diff *diffInst, channel chan error) {
	channel <- doReadStream(stream, diff)
}

func doReadStream(stream *os.File, diff *diffInst) error {
	defer stream.Close()
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
		return fmt.Errorf("Unexpected stream version %v", ver)
	}
	for true {
		command, err := readCommand(input)
		if err != nil {
			return err
		}
		if command.Type.Op == opUnspec {
			return fmt.Errorf("Unexpected command %v", command)
		} else if command.Type.Op == opIgnore {
			continue
		} else if command.Type.Op == opRename {
			fromPath, err := command.ReadParam(C.BTRFS_SEND_A_PATH)
			if err != nil {
				return err
			}
			toPath, err := command.ReadParam(C.BTRFS_SEND_A_PATH_TO)
			if err != nil {
				return err
			}
			if debug {
				fmt.Fprintf(os.Stdout, "[DEBUG] TRACE %25v %v %v\n", command.Type.Name, fromPath, toPath)
			}
			diff.rename(fromPath, toPath)
		} else if command.Type.Op == opEnd {
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG] END\n")
			}
			break
		} else {
			var thepath string
			path, err := command.ReadParam(C.BTRFS_SEND_A_PATH)
			if err != nil {
				if err.Error() != "Expect type 15; got 18" {
					return err
				}
				path, err := command.ReadParam(C.BTRFS_SEND_A_FILE_OFFSET)
				if err != nil {
					return err
				}
				thepath = path
			} else {
				thepath = path
			}
			if debug {
				fmt.Fprintf(os.Stdout, "[DEBUG] TRACE %25v %v\n", command.Type.Name, thepath)
			}
			diff.tagPath(thepath, command.Type.Op)
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
	root_f, err := os.OpenFile(path, os.O_RDONLY, 0777)
	if err != nil {
		return 0, fmt.Errorf("open returned %v\n", err)
	}
	r := C.subvol_uuid_search_init(C.int(root_f.Fd()), &sus)
	if r < 0 {
		return 0, fmt.Errorf("subvol_uuid_search_init returned %v\n", r)
	}
	subvolInfo, err = C.subvol_uuid_search(&sus, 0, nil, 0, C.CString(path), C.subvol_search_by_path)
	if subvolInfo == nil {
		return 0, fmt.Errorf("subvol_uuid_search returned %v\n", err)
	}
	return C.__u64(subvolInfo.root_id), nil
}

func btrfsSendSyscall(stream *os.File, source string, subvolume string) error {
	defer stream.Close()
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG] opening subvolume '%s'\n", subvolume)
	}
	subvol_f, err := os.OpenFile(subvolume, os.O_RDONLY, 0777)
	if err != nil {
		return fmt.Errorf("open returned %v\n", err)
	}
	root_id, err := getSubVolUID(source)
	if err != nil {
		fmt.Fprintf(os.Stderr, "getSubVolUID returns %v\n", err)
		os.Exit(1)
	}
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG] root_id %v\n", root_id)
	}
	var subvol_fd C.uint = C.uint(subvol_f.Fd())
	var opts C.struct_btrfs_ioctl_send_args
	opts.send_fd = C.__s64(stream.Fd())
	opts.clone_sources = &root_id
	opts.clone_sources_count = 1
	opts.parent_root = root_id
	opts.flags = C.BTRFS_SEND_FLAG_NO_FILE_DATA
	ret, _, err := syscall.Syscall(syscall.SYS_IOCTL, uintptr(subvol_fd), C.BTRFS_IOC_SEND, uintptr(unsafe.Pointer(&opts)))
	if ret != 0 {
		return err
	}
	return nil
}

func btrfsSendDiff(source, subvolume string) (*diffInst, error) {
	read, write, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("pipe returned %v\n", err)
	}

	var diff diffInst = diffInst{}
	channel := make(chan error)
	go readStream(read, &diff, channel)
	err = btrfsSendSyscall(write, source, subvolume)
	if err != nil {
		return nil, fmt.Errorf("btrfsSendSyscall returns %v\n", err)
	}
	err = <-channel
	if err != nil {
		return nil, fmt.Errorf("readStream returns %v\n", err)
	}
	return &diff, nil
}

func btrfsStreamFileDiff(streamfile string) (*diffInst, error) {
	if debug {
		fmt.Fprintf(os.Stdout, "[DEBUG] opening file '%v'\n", streamfile)
	}
	f, err := os.Open(streamfile)
	if err != nil {
		return nil, fmt.Errorf("open returned %v\n", err)
	}
	defer f.Close()

	var diff diffInst = diffInst{}
	channel := make(chan error)
	go readStream(f, &diff, channel)
	if err != nil {
		return nil, fmt.Errorf("btrfsGetSyscall returns %v\n", err)
	}
	err = <-channel
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("readStream returns %v\n", err)
	}
	return &diff, nil
}

func GetChangesFromTwoSubvolumes(child string, parent string) ([]string, error) {
	p_stat, err := os.Stat(parent)
	if err != nil {
		return nil, err
	}
	if ! p_stat.IsDir() {
		return nil, fmt.Errorf("Error: '%s' is not a directory\n", parent)
	}
	c_stat, err := os.Stat(child)
	if err != nil {
		return nil, err
	}
	if ! c_stat.IsDir() {
		return nil, fmt.Errorf("Error: '%s' is not a directory\n", child)
	}
	diff, err := btrfsSendDiff(parent, child)
	if err != nil {
		return nil, err
	}
	return diff.Changes(), nil
}

func GetChangesFromStreamFile(streamfile string) ([]string, error) {
	f_stat, err := os.Lstat(streamfile)
	if err != nil {
		return nil, err
	}
	if ! f_stat.Mode().IsRegular() {
		return nil, fmt.Errorf("Error: '%s' is not a file\n", streamfile)
	}
	diff, err := btrfsStreamFileDiff(streamfile)
	if err != nil {
		return nil, err
	}
	return diff.Changes(), nil
}

func SetDebug(status bool) {
	debug = status
}
