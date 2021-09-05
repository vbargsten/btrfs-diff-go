package main

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	btrfsdiff "github.com/mbideau/btrfs-diff-go/pkg"
)

func usage(progname string) {
	fmt.Printf(`
%[1]s - Analyse the differences between two related btrfs subvolumes.

USAGE

	%[1]s [OPTIONS] PARENT CHILD
		Analyse the difference between btrfs PARENT and CHILD.

	%[1]s [OPTIONS] -f|--file STREAM
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

	--with-times[=changed]
		By defautl time modifications are ignored. With that option
		they will be taken into account. They are labelled as 'times'
		but if you also specify '=changed' they will be labelled
		'changed'.

	--with-perms[=changed]
		By defautl permission modifications are ignored. With that option
		they will be taken into account. They are labelled as 'perms'
		but if you also specify '=changed' they will be labelled
		'changed'.

	--with-own[=changed]
		By defautl ownership modifications are ignored. With that option
		they will be taken into account. They are labelled as 'own'
		but if you also specify '=changed' they will be labelled
		'changed'.

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

	var optionHelp bool = false
	var optionDebug bool = false
	var optionFile string = ""
	var optionWithTimes bool = false
	var optionTimesAsChanged bool = false
	var optionWithPerms bool = false
	var optionPermsAsChanged bool = false
	var optionWithOwn bool = false
	var optionOwnAsChanged bool = false
	var argSubvolParent string = ""
	var argSubvolChild string = ""

	skip := false
	for index, arg := range os.Args[1:] {

		// hacky way to compensate for lack of 'continue 2' instruction
		if skip {
			skip = false
			continue
		}

		switch(arg) {
			case "-h", "--help":
				optionHelp = true
			case "--debug":
				optionDebug = true
			case "--with-times":
				optionWithTimes = true
			case "--with-times=changed":
				optionTimesAsChanged = true
			case "--with-perms":
				optionWithPerms = true
			case "--with-perms=changed":
				optionPermsAsChanged = true
			case "--with-own":
				optionWithOwn = true
			case "--with-own=changed":
				optionOwnAsChanged = true
			case "-f", "--file":
				if len(os.Args) > index+2 {
					optionFile = os.Args[index+2]
					skip = true
				}
			default:
				if len(argSubvolParent) > 0 {
					argSubvolParent = arg
				} else if len(argSubvolChild) > 0 {
					argSubvolChild = arg
				}
		}
	}

	if len(os.Args) <= 2 || optionHelp {
		usage(path.Base(os.Args[0]))
		os.Exit(0)
	}

	if optionDebug {
		btrfsdiff.SetDebug(true)
	}

	if optionWithTimes || optionTimesAsChanged {
		btrfsdiff.ConsiderUtimeOp(optionTimesAsChanged)
	}
	if optionWithPerms || optionPermsAsChanged {
		btrfsdiff.ConsiderChmodOp(optionPermsAsChanged)
	}
	if optionWithOwn || optionOwnAsChanged {
		btrfsdiff.ConsiderChownOp(optionOwnAsChanged)
	}

	var changes []string
	var err error
	if len(optionFile) > 0 {
		var streamfile string
		streamfile, err = filepath.Abs(optionFile)
		if err == nil {
			changes, err = btrfsdiff.GetChangesFromStreamFile(streamfile)
		}
	} else {
		var parent string
		var child string
		parent, err = filepath.Abs(argSubvolParent)
		if err == nil {
			child, err = filepath.Abs(argSubvolChild)
			if err == nil {
				changes, err = btrfsdiff.GetChangesFromTwoSubvolumes(child, parent)
			}
		}
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	// if there are changes/differences
	if len(changes) > 0 {

		// sort the list alphabetically (default)
		sort.Strings(changes)

		// print changes
		fmt.Printf("%v\n", strings.Join(changes, "\n"))

		// exit 1 if there are differences
		os.Exit(1)
	}
}
