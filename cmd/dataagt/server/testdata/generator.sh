#! /bin/bash

# This script is used to generate test data for the dataagt server.

# + root/
#   + a/
#     + b/
#       + file1.txt
#       + file2.txt
#   + c/
#     + file3.txt
#     + file4.txt --> symlink to root/a/b/file1.txt
#   + d/ --> symlink to root/a/b

# Create the root directory
mkdir -p root/a/b root/c root/d
echo "file1" > root/a/b/file1.txt
echo "file2" > root/a/b/file2.txt
echo "file3" > root/c/file3.txt
ln -s ../a/b/file1.txt root/c/file4.txt
ln -s a/b root/d

cd root
tar -czf ../root.tar.gz *