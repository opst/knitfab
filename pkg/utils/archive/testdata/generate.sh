#! /bin/bash

# this script is to generate testcases for testxing symlink resolution.

# testcase 1: having symlink to file
# testcase-1/linked/
# ├── a.txt
# └── dir1/
#     └── c.txt  --> ../a.txt
#
# testcase-1/resolved/
# ├── a.txt
# └── dir1/
#     └── c.txt

mkdir -p testcase-1/linked/dir1
echo "a" > testcase-1/linked/a.txt
ln -s ../a.txt testcase-1/linked/dir1/c.txt

mkdir -p testcase-1/resolved/dir1
echo "a" > testcase-1/resolved/a.txt
cp testcase-1/resolved/a.txt testcase-1/resolved/dir1/c.txt

# testcase 2: having symlink to directory
# testcase-2/linked/
# ├── a.txt
# ├── dir1/
# │   └── b.txt
# └── dir2/ --> dir1
#
# testcase-2/resolved/
# ├── a.txt
# ├── dir1/
# │   └── b.txt
# └── dir2/
#     └── b.txt

mkdir -p testcase-2/linked/dir1
echo "a" > testcase-2/linked/a.txt
echo "b" > testcase-2/linked/dir1/b.txt
ln -s dir1 testcase-2/linked/dir2

mkdir -p testcase-2/resolved/dir1
echo "a" > testcase-2/resolved/a.txt
echo "b" > testcase-2/resolved/dir1/b.txt
mkdir -p testcase-2/resolved/dir2/
cp testcase-2/resolved/dir1/b.txt testcase-2/resolved/dir2

# testcase 3: having symlink flip-flop
# testcase-3/linked/
# ├── dir1/
# │   ├── b.txt
# │   └── dir1-2/ --> ../dir2
# └── dir2/
#     ├── c.txt
#     └── dir2-2/ --> ../dir1
#
# # no resolved version. there are infinite loop.

mkdir -p testcase-3/linked/dir1/
echo "b" > testcase-3/linked/dir1/b.txt
mkdir -p testcase-3/linked/dir2/
echo "c" > testcase-3/linked/dir2/c.txt
ln -s ../dir2/ testcase-3/linked/dir1/dir1-2
ln -s ../dir1/ testcase-3/linked/dir2/dir2-2

# testcase 4: having symlink to symlink
# testcase-4/linked/
# ├── a.txt --> b.txt
# ├── b.txt --> c.txt
# └── c.txt --> a.txt

mkdir -p testcase-4/linked
echo "a" > testcase-4/linked/a.txt
ln -s a.txt testcase-4/linked/b.txt
ln -s b.txt testcase-4/linked/c.txt
rm testcase-4/linked/a.txt
ln -s c.txt testcase-4/linked/a.txt
