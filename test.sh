#!/bin/bash

set -e

THIS_DIR="$(realpath "$(dirname "$0")")"

BTRFS_DIFF_BIN="$THIS_DIR"/btrfs-diff
if [ "$TMPDIR" = '' ]; then
    TMPDIR="$THIS_DIR"/.tmp
fi
if [ "$TEST_DIR" = '' ]; then
    TEST_DIR="$THIS_DIR"/testdir
fi
DATA_DIR="$TEST_DIR"/data
SNAPS_DIR="$TEST_DIR"/snaps

if [ ! -x "$BTRFS_DIFF_BIN" ]; then
    echo "Binary '' not found or not executable" >&2
    exit 1
fi

echo "Test directory: '$TEST_DIR'"

[ ! -d "$TEST_DIR" ] && mkdir -p "$TEST_DIR"

use_sudo=
if [ "$(id -u)" != '0' ]; then
    use_sudo=sudo
fi

echo "-- Removing existing data and snapshots"
[ -d "$DATA_DIR" ] && $use_sudo btrfs subvolume delete "$DATA_DIR" > /dev/null
if [ -d "$SNAPS_DIR" ]; then
    [ -n "$(ls "$SNAPS_DIR")" ] && for snap in "$SNAPS_DIR"/*; do
        $use_sudo btrfs subvolume delete "$snap" > /dev/null
    done
fi
rm -f "$TMPDIR"/a_raw "$TMPDIR"/a "$TMPDIR"/b "$TMPDIR"/b_diff "$TMPDIR"/diff "$TMPDIR"/diff.out "$TMPDIR"/diff.src

echo "-- Creating a subvolume that will contains the data: '$DATA_DIR' (read-write)"
btrfs subvolume create "$DATA_DIR" > /dev/null

echo "-- Creating a subvolume that will contains the snapshots: '$SNAPS_DIR' (read-only)"
[ ! -d "$SNAPS_DIR" ] && mkdir -p "$SNAPS_DIR"

echo "-- Creating first snapshot (with no data)"
btrfs subvolume snapshot -r "$DATA_DIR" "$SNAPS_DIR"/000 > /dev/null

echo "-- Creating data and snapshots for each commands"
I=1
(
cat <<END
echo foo > foo_file
mkdir bar
mv foo_file bar
echo baz > bar/baz_file
ln bar/baz_file bar/baaz_file
mv bar/baz_file bar/foo_file
rm bar/foo_file
rm -rf bar
mkdir dir
touch dir/file
mkfifo dir/fifo
ln dir/file dir/hardlink
ln -s file dir/symlink
mv dir/hardlink dir/hardlink.rn
mv dir/symlink dir/symlink.rn
mv dir/fifo dir/fifo.rn
echo todel > dir/file_to_del
rm -rf dir
END
) | while read -r command; do
    (cd "$DATA_DIR"; sh -c "$command")
    btrfs subvolume snapshot -r "$DATA_DIR" "$SNAPS_DIR/$(printf "%03i" $I)" > /dev/null
    echo "$I: $command" >&2
    I=$((I + 1))
done

echo "-- Comparing snapshots between them with '$(basename "$BTRFS_DIFF_BIN")' "`
     `"then with 'diff' and printing unmatching lines (between both diff)"
failed=false
for A in "$SNAPS_DIR"/*; do
    for B in "$SNAPS_DIR"/*; do
        if [ "$A" = "$B" ]; then continue; fi
        $use_sudo "$BTRFS_DIFF_BIN" "$A" "$B" > "$TMPDIR"/a_raw 2>&1 || true
        cut -b4- "$TMPDIR"/a_raw | sort | grep -v '^changed: $' > "$TMPDIR"/a || true
        LC_ALL=C diff -qr "$A" "$B" | \
            sed "s|$A|old|; s|$B|new|g; s|: |/|; s/Only in new/  added: /; s/Only in old/deleted: /; s|Files old/.* and new/\(.*\) differ|changed: /\1|" | \
            sed "/File .* is a fifo while file .* is a fifo/d" | \
            sort > "$TMPDIR"/b || true
        # Filter things that were spuriously added (can happen due to utimes changes and stuff).
        # Then filter only changes (else we spit out headers for the stuff we filtered).
        if ! LC_ALL=C diff -u5 "$TMPDIR"/a "$TMPDIR"/b >"$TMPDIR"/b_diff && \
                [ "$(cat -s "$TMPDIR"/b_diff | grep -v '^-changed' | grep -c '^[+-][^+-]')" -ne 0 ]; then
            echo "FAIL: $A $B" | sed "s|$TEST_DIR/\?||g"
            cat -s "$TMPDIR"/b_diff | grep -v '^-changed' | grep '^[+-][^+-]' | sed "s|^|$A $B: |" | sed "s|$TEST_DIR/\?||g"
            failed=true
        fi
    done
done
if [ "$failed" = 'false' ]; then
    echo "SUCCESS"
else
    echo "FAIL"
    exit 1
fi

echo "-- Now testing against a tricky stream file ..."
# from: https://git.kernel.org/pub/scm/linux/kernel/git/kdave/btrfs-progs.git/tree/tests/misc-tests/016-send-clone-src
#       'multi-clone-src-v4.8.2.stream.xz' has been extracted and 'multi-clone-src-v4.8.2.stream' renamed to 'test.data'
failed=true
if ! "$BTRFS_DIFF_BIN" --file "$THIS_DIR"/test.data >"$TMPDIR"/diff.out; then
    cat > "$TMPDIR"/diff.src <<ENDCAT
   deleted: /file2_1
     added: /file1_1
     added: /file1_2
ENDCAT
    if diff "$TMPDIR"/diff.out "$TMPDIR"/diff.src; then
        failed=false
    fi
fi
if [ "$failed" = 'false' ]; then
    echo "SUCCESS"
else
    echo "FAIL"
    exit 1
fi

# cleanup
echo "-- Removing existing data and snapshots"
[ -d "$DATA_DIR" ] && $use_sudo btrfs subvolume delete "$DATA_DIR" > /dev/null
if [ -d "$SNAPS_DIR" ]; then
    [ -n "$(ls "$SNAPS_DIR")" ] && for snap in "$SNAPS_DIR"/*; do
        $use_sudo btrfs subvolume delete "$snap" > /dev/null
    done
fi
rm -f "$TMPDIR"/a_raw "$TMPDIR"/a "$TMPDIR"/b "$TMPDIR"/b_diff "$TMPDIR"/diff "$TMPDIR"/diff.out "$TMPDIR"/diff.src
