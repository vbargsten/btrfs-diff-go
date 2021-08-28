//+build linux

// to build it run :
//   GOOS=linux GOARCH=amd64 go build -a -v btrfs-diff.go
// it requires: golang and libbtrfs-dev, so on GNU Linux/Debian do :
//   sudo apt install golang libbtrfs-dev

package main

// We get the constants from this header.

// #include <btrfs/send.h>
// #include <btrfs/send-utils.h>
// #include <btrfs/ioctl.h>
// #cgo LDFLAGS: -lbtrfs
import "C"

import "os"
import "encoding/binary"
import "bufio"
import "fmt"
import "unsafe"
import "syscall"
import "strings"
import "path"
import "path/filepath"
import "io"

// NAUGHTYNESS:
// For a recursive delete, we get a rename, then a delete on the renamed copy.
// * We need understand that if we rm a renamed path, we should unrename anything inside it for the diff.
// For a create, we get a garbage name, then a rename.
// * We need to understand that if we get a rename of a file that was new, we must rename all the stuff we did to it.

var debug bool = false

type Op int

const (
	OpUnspec Op = iota
	OpIgnore
	OpCreate
	OpModify
	OpDelete
	OpRename // Special cased -- we need two paths
	OpEnd
)

var names []string = []string{"!!!", "ignored", "added", "changed", "deleted", "renamed", "END"}

func (op Op) String() string {
	return names[op]
}

type CommandSpec struct {
	Name string
	Op   Op
}

type Command struct {
	Type *CommandSpec
	body []byte
}

func initCommands() *[C.__BTRFS_SEND_C_MAX]CommandSpec {
	var commands [C.__BTRFS_SEND_C_MAX]CommandSpec
	commands[C.BTRFS_SEND_C_UNSPEC] = CommandSpec{Name: "BTRFS_SEND_C_UNSPEC", Op: OpUnspec}

	commands[C.BTRFS_SEND_C_SUBVOL] = CommandSpec{Name: "BTRFS_SEND_C_SUBVOL", Op: OpIgnore}
	commands[C.BTRFS_SEND_C_SNAPSHOT] = CommandSpec{Name: "BTRFS_SEND_C_SNAPSHOT", Op: OpIgnore}

	commands[C.BTRFS_SEND_C_MKFILE] = CommandSpec{Name: "BTRFS_SEND_C_MKFILE", Op: OpCreate}
	commands[C.BTRFS_SEND_C_MKDIR] = CommandSpec{Name: "BTRFS_SEND_C_MKDIR", Op: OpCreate}
	commands[C.BTRFS_SEND_C_MKNOD] = CommandSpec{Name: "BTRFS_SEND_C_MKNOD", Op: OpCreate}
	commands[C.BTRFS_SEND_C_MKFIFO] = CommandSpec{Name: "BTRFS_SEND_C_MKFIFO", Op: OpCreate}
	commands[C.BTRFS_SEND_C_MKSOCK] = CommandSpec{Name: "BTRFS_SEND_C_MKSOCK", Op: OpCreate}
	commands[C.BTRFS_SEND_C_SYMLINK] = CommandSpec{Name: "BTRFS_SEND_C_SYMLINK", Op: OpCreate}

	commands[C.BTRFS_SEND_C_RENAME] = CommandSpec{Name: "BTRFS_SEND_C_RENAME", Op: OpRename}
	commands[C.BTRFS_SEND_C_LINK] = CommandSpec{Name: "BTRFS_SEND_C_LINK", Op: OpCreate}
	commands[C.BTRFS_SEND_C_UNLINK] = CommandSpec{Name: "BTRFS_SEND_C_UNLINK", Op: OpDelete}
	commands[C.BTRFS_SEND_C_RMDIR] = CommandSpec{Name: "BTRFS_SEND_C_RMDIR", Op: OpDelete}

	commands[C.BTRFS_SEND_C_SET_XATTR] = CommandSpec{Name: "BTRFS_SEND_C_SET_XATTR", Op: OpModify}
	commands[C.BTRFS_SEND_C_REMOVE_XATTR] = CommandSpec{Name: "BTRFS_SEND_C_REMOVE_XATTR", Op: OpModify}

	commands[C.BTRFS_SEND_C_WRITE] = CommandSpec{Name: "BTRFS_SEND_C_WRITE", Op: OpModify}
	commands[C.BTRFS_SEND_C_CLONE] = CommandSpec{Name: "BTRFS_SEND_C_CLONE", Op: OpModify}

	commands[C.BTRFS_SEND_C_TRUNCATE] = CommandSpec{Name: "BTRFS_SEND_C_TRUNCATE", Op: OpModify}
	commands[C.BTRFS_SEND_C_CHMOD] = CommandSpec{Name: "BTRFS_SEND_C_CHMOD", Op: OpModify}
	commands[C.BTRFS_SEND_C_CHOWN] = CommandSpec{Name: "BTRFS_SEND_C_CHOWN", Op: OpModify}
	commands[C.BTRFS_SEND_C_UTIMES] = CommandSpec{Name: "BTRFS_SEND_C_UTIMES", Op: OpModify}

	commands[C.BTRFS_SEND_C_END] = CommandSpec{Name: "BTRFS_SEND_C_END", Op: OpEnd}
	commands[C.BTRFS_SEND_C_UPDATE_EXTENT] = CommandSpec{Name: "BTRFS_SEND_C_UPDATE_EXTENT", Op: OpModify}
	// Sanity check (hopefully no holes).
	for i, command := range commands {
		if i != C.BTRFS_SEND_C_UNSPEC && command.Op == OpUnspec {
			return nil
		}
	}
	return &commands
}

var commands *[C.__BTRFS_SEND_C_MAX]CommandSpec = initCommands()

type Node struct {
	Children   map[string]*Node
	Name       string
	ChangeType Op
	Parent     *Node
	Original   *Node
}

type Diff struct {
	Original Node
	New      Node
}

func (diff *Diff) tagPath(path string, changeType Op) {
	if debug {
		fmt.Fprintf(os.Stdout, "[DEBUG] TRACE %10v %v\n", changeType, path)
	}
	fileNode := diff.find(path, changeType == OpCreate)
	if changeType == OpDelete {
		if fileNode.Original == nil {
			fmt.Fprintf(os.Stderr, "deleting path %v which was created in same diff?\n", path)
		}
		delete(fileNode.Parent.Children, fileNode.Name)
	} else { // Why this? if fileNode.Original != nil {
		if !(fileNode.ChangeType == OpCreate && changeType == OpModify) {
			fileNode.ChangeType = changeType
		}
	}
	if changeType == OpDelete {
		// If we deleted /this/ node, it sure as hell needs no children.
		fileNode.Children = nil
		if fileNode.Original != nil {
			// Leave behind a sentinel in the Original structure.
			fileNode.Original.ChangeType = OpDelete
			fileNode.Original.verifyDelete(path)
			fileNode.Original.Children = nil
		}
	}
	//fmt.Fprintf(os.Stderr, "intermediate=%v\n", diff)
}

func (node *Node) verifyDelete(path string) {
	for _, child := range node.Children {
		if child.ChangeType != OpDelete && child.ChangeType != OpRename {
			fmt.Fprintf(os.Stderr, "deleting parent of node %v in %v which is not gone", node, path)
		}
	}
}

func (diff *Diff) rename(from string, to string) {
	if debug {
		fmt.Fprintf(os.Stdout, "[DEBUG] TRACE %10v %v\n", "rename", from)
		fmt.Fprintf(os.Stdout, "[DEBUG] TRACE %10v %v\n", "rename_to", to)
	}
	fromNode := diff.find(from, false)
	delete(fromNode.Parent.Children, fromNode.Name)
	if fromNode.Original != nil {
		// if fromNode had an original, we must mark that path destroyed.
		fromNode.Original.ChangeType = OpRename
	}
	toNode := diff.find(to, true)
	toNode.Parent.Children[toNode.Name] = fromNode
	fromNode.Name = toNode.Name
	fromNode.ChangeType = OpCreate
	fromNode.Parent = toNode.Parent
	//fmt.Fprintf(os.Stderr, "intermediate=%v\n", diff)
}

func (diff *Diff) find(path string, isNew bool) *Node {
	if diff.New.Original == nil {
		diff.New.Original = &diff.Original
	}
	if path == "" {
		return &diff.New
	}
	parts := strings.Split(path, "/")
	current := &diff.New
	for i, part := range parts {
		if current.Children == nil {
			current.Children = make(map[string]*Node)
		}
		newNode := current.Children[part]
		if newNode == nil {
			current.Children[part] = &Node{}
			newNode = current.Children[part]
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
					original.Children = make(map[string]*Node)
				}
				newOriginal := original.Children[part]
				if newOriginal == nil {
					if !isNew || i < len(parts)-1 {
						if debug {
							fmt.Fprintf(os.Stderr, "[DEBUG] ACK %v %v %v %v %v\n", original, isNew, path, part, newOriginal)
						}
						// Was meant to already exist, so make sure it did!
						original.Children[part] = &Node{}
						newOriginal = original.Children[part]
						newOriginal.Name = part
						newOriginal.Parent = original
						newNode.Original = newOriginal
					}
				}
			}
			newNode.Name = part
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

func (node *Node) String() string {
	return fmt.Sprintf("(%v, %v, %v)", node.Children, node.ChangeType, node.Name)
}

func (diff *Diff) String() string {
	return "\n\t" + strings.Join((diff.Changes())[:], "\n\t") + "\n"
}

func (diff *Diff) Changes() []string {
	newFiles := make(map[string]*Node)
	oldFiles := make(map[string]*Node)
	changes(&diff.New, "", newFiles)
	changes(&diff.Original, "", oldFiles)
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG] new: %v\n[DEBUG] %v\n", newFiles, &diff.New)
		fmt.Fprintf(os.Stderr, "[DEBUG] old: %v\n[DEBUG] %v\n", oldFiles, &diff.Original)
	}
	var ret []string
	for name, node := range oldFiles {
		if newFiles[name] != nil && node.ChangeType == OpUnspec {
			if node.Children == nil {
				// TODO diff equality only
				ret = append(ret, fmt.Sprintf("%10v: %v", OpModify, name))
				if debug {
					fmt.Fprintf(os.Stderr, "[DEBUG] appended (node.Children == nil): %10v: %v (%v) (%v)\n", OpModify, name, newFiles[name], node)
				}
			}
			delete(newFiles, name)
		} else {
			if node.ChangeType != OpDelete && node.ChangeType != OpRename {
				fmt.Fprintf(os.Stderr, "unexpected ChangeType on original %v: %v", name, node.ChangeType)
			}
			if (node.ChangeType == OpDelete || node.ChangeType == OpRename) && newFiles[name] != nil && newFiles[name].ChangeType == OpCreate {
				ret = append(ret, fmt.Sprintf("%10v: %v", OpModify, name))
				if debug {
					fmt.Fprintf(os.Stderr, "[DEBUG] appended (OpDelete||OpRename): %10v: %v\n", OpModify, name)
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
		ret = append(ret, fmt.Sprintf("%10v: %v", OpCreate, name))
		if debug {
			fmt.Fprintf(os.Stderr, "[DEBUG] appended (new): %10v: %v\n", OpCreate, name)
		}
	}
	return ret
}

func changes(node *Node, prefix string, ret map[string]*Node) {
	if debug {
		fmt.Fprintf(os.Stderr, "[DEBUG] changes(%v, %v)\n", node.Name, prefix)
	}
	newPrefix := prefix + node.Name
	if (newPrefix != "") {
		ret[newPrefix] = node
	}
	if node.ChangeType == OpCreate {
		// TODO diff equality only
		return
	}
	for _, child := range node.Children {
		changes(child, newPrefix+"/", ret)
	}
}

func peekAndDiscard(input *bufio.Reader, n int) ([]byte, error) {
	data, err := input.Peek(n)
	if err != nil {
		return nil, err
	}
	if _, err := input.Discard(n); err != nil {
		return nil, err
	}
	return data, nil
}

func readCommand(input *bufio.Reader) (*Command, error) {
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
		fmt.Fprintf(os.Stdout, "[DEBUG] Cmd %v; type %v\n", cmdData, commands[cmdType].Name)
	}
	return &Command{
		Type: &commands[cmdType],
		body: cmdData,
	}, nil
}

func (command *Command) ReadParam(expectedType int) (string, error) {
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

func readStream(stream *os.File, diff *Diff, channel chan error) {
	channel <- doReadStream(stream, diff)
}

func doReadStream(stream *os.File, diff *Diff) error {
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
		if command.Type.Op == OpUnspec {
			return fmt.Errorf("Unexpected command %v", command)
		} else if command.Type.Op == OpIgnore {
			continue
		} else if command.Type.Op == OpRename {
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
		} else if command.Type.Op == OpEnd {
			if debug {
				fmt.Fprintf(os.Stderr, "[DEBUG] END\n")
			}
			break
		} else {
			path, err := command.ReadParam(C.BTRFS_SEND_A_PATH)
			if err != nil {
				return err
			}
			if debug {
				fmt.Fprintf(os.Stdout, "[DEBUG] TRACE %25v %v\n", command.Type.Name, path)
			}
			diff.tagPath(path, command.Type.Op)
		}
	}
	return nil
}

func getSubvolUid(path string) (C.__u64, error) {
	var sus C.struct_subvol_uuid_search
	var subvol_info *C.struct_subvol_info
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
	subvol_info, err = C.subvol_uuid_search(&sus, 0, nil, 0, C.CString(path), C.subvol_search_by_path)
	if subvol_info == nil {
		return 0, fmt.Errorf("subvol_uuid_search returned %v\n", err)
	}
	return C.__u64(subvol_info.root_id), nil
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
	root_id, err := getSubvolUid(source)
	if err != nil {
		fmt.Fprintf(os.Stderr, "getSubvolUid returns %v\n", err)
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

func btrfsSendDiffs(source, subvolume string) (*Diff, error) {
	read, write, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("pipe returned %v\n", err)
	}

	var diff Diff = Diff{}
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

func btrfsStreamFileDiffs(streamfile string) (*Diff, error) {
	if debug {
		fmt.Fprintf(os.Stdout, "[DEBUG] opening file '%v'\n", streamfile)
	}
	f, err := os.Open(streamfile)
	if err != nil {
		return nil, fmt.Errorf("open returned %v\n", err)
	}
	defer f.Close()

	var diff Diff = Diff{}
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


func usage(progname string) {
	fmt.Printf(`
%[1]s - Analyse the differences between two related btrfs subvolumes.

USAGE

	%[1]s [ --debug ] PARENT CHILD
		Analyse the difference between btrfs PARENT and CHILD.

	%[1]s [ --debug ] -f|--file STREAM
		Analyse the differences from a STREAM file (output from 'btrfs send').

	%[1]s [ -h | --help ]
		Display help.

ARGUMENTS

	PARENT
		A btrfs subvolume that is the parent of the CHILD one.

	CHILD
		A btrfs subvolume that is the child of the PARENT one.

OPTIONS

	-h | --help
		Display help.

	--debug
		Be more verbose.

	-f|--file STREAM
		Use a STREAM file to get the btrfs operations.
		This stream file must have been generated by the command
		'btrfs send' (with or without the option --no-data).

EXAMPLES

	Get the differences between two snapshots.
	$ %[1]s /backup/btrfs-sp/rootfs/2020-12-25_22h00m00.shutdown.safe \
		/backup/btrfs-sp/rootfs/2019-12-25_21h00m00.shutdown.safe

AUTHORS

	Originally written by: David Buckley
	Extended, fixed, and maintained by: Michael Bideau

REPORTING BUGS
	Report bugs to: <https://github.com/mbideau/btrfs-diff-go/issues>

COPYRIGHT

	Copyright © 2020-2021 Michael Bideau.
	License GPLv3+: GNU GPL version 3 or later <https://gnu.org/licenses/gpl.html>
	This is free software: you are free to change and redistribute it.
	There is NO WARRANTY, to the extent permitted by law.

	Info: original license chosen by David Buckley was MIT, but it allows sublicensing, so I
	      chose to sublicense it to GPLv3+ to ensure code sharing

SEE ALSO

	Home page: <https://github.com/mbideau/btrfs-diff-go>

`, progname)
}

func main() {
	var first string
	var second string
	if (len(os.Args) <= 2 || os.Args[1] == "-h" || os.Args[1] == "--help") {
		usage(path.Base(os.Args[0]))
	} else {
		if (len(os.Args) == 4 && os.Args[1] == "--debug") {
			fmt.Println("[DEBUG] Debug enabled")
			debug = true
			first = os.Args[2]
			second = os.Args[3]
		}

		var thediff *Diff

		// --file option specified with a stream file
		if (first == "-f" || first == "--file") {
			var streamfile string
			streamfile, _ = filepath.Abs(second)

			f_stat, f_err := os.Lstat(streamfile)
			if f_err != nil {
				fmt.Fprintf(os.Stderr, "%v\n", f_err)
				os.Exit(1)
			}
			if ! f_stat.Mode().IsRegular() {
				fmt.Fprintf(os.Stderr, "Error: '%s' is not a file\n", streamfile)
				os.Exit(1)
			}

			diff, err := btrfsStreamFileDiffs(streamfile)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%v\n", err)
				os.Exit(1)
			}
			thediff = diff

		// parent and child arguments
		} else {

			var parent string
			var child string
			parent, _ = filepath.Abs(first)
			child, _ = filepath.Abs(second)

			p_stat, p_err := os.Stat(parent)
			if p_err != nil {
				fmt.Fprintf(os.Stderr, "%v\n", p_err)
				os.Exit(1)
			}
			if ! p_stat.IsDir() {
				fmt.Fprintf(os.Stderr, "Error: '%s' is not a directory\n", parent)
				os.Exit(1)
			}
			c_stat, c_err := os.Stat(child)
			if c_err != nil {
				fmt.Fprintf(os.Stderr, "%v\n", c_err)
				os.Exit(1)
			}
			if ! c_stat.IsDir() {
				fmt.Fprintf(os.Stderr, "Error: '%s' is not a directory\n", child)
				os.Exit(1)
			}

			diff, err := btrfsSendDiffs(parent, child)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%v\n", err)
				os.Exit(1)
			}
			thediff = diff
		}

		changes := thediff.Changes()
		if len(changes) > 0 {
			fmt.Fprintf(os.Stdout, "%v\n", strings.Join(changes, "\n"))
			os.Exit(1)
		}
	}
}
