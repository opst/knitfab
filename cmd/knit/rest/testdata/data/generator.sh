#! /bin/bash

# This script generates a testdata.

mkdir ./root

#
# + root
#   + a1/
#       + a2/
#           + file-a2.txt
#       + file-a1.txt
#   + b/file2.txt
#   + c/file3.txt --> symlink to a1/file-a1.txt
#   + d/
#       + e/ --> symlink to a1/a2
#

mkdir -p ./root/a1/a2
echo "file-a2.txt" > ./root/a1/a2/file-a2.txt
echo "file-a1.txt" > ./root/a1/file-a1.txt
mkdir ./root/b
echo "file2.txt" > ./root/b/file2.txt
mkdir ./root/c
ln -s ../a1/file-a1.txt ./root/c/file3.txt
mkdir ./root/d
ln -s ../a1/a2 ./root/d/e

#
# # dereferenced version of root:
# + root
#   + a1/
#       + a2/
#           + file-a2.txt
#       + file-a1.txt
#   + b/file2.txt
#   + c/file3.txt (same content as a1/file-a1.txt)
#   + d/
#       + e/
#           + file-a2.txt (same content as a1/a2/file-a2.txt)
#

mkdir -p ./root-dereferenced/a1/a2
echo "file-a1.txt" > ./root-dereferenced/a1/file-a1.txt
echo "file-a2.txt" > ./root-dereferenced/a1/a2/file-a2.txt
mkdir ./root-dereferenced/b
echo "file2.txt" > ./root-dereferenced/b/file2.txt
mkdir ./root-dereferenced/c
echo "file-a1.txt" > ./root-dereferenced/c/file3.txt
mkdir -p ./root-dereferenced/d/e
echo "file-a2.txt" > ./root-dereferenced/d/e/file-a2.txt
