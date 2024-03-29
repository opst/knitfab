package internal

import (
	"errors"
	"os"
	"path/filepath"
)

// like filepath.Walk, but do not follows symlinks.
//
// LWalk visits directories/files in lexical order and depth-first arrival order (preorder).
//
// The visit function is called for each file or directory visited by LWalk.
//
// The leave function is called after all the directory's contents have been visited (postorder).
//
// # Args
//
// - dir: the root directory to walk.
//
// - visit: the function called for each file or directory visited by LWalk.
// It can be nil when you don't need to visit files in preorder.
//
// The path argument is the absolute path to the item,
//
// The info argument to visit comes from os.Lstat of the item,
// or will be nil if there was an error walking to path.
//
// The err argument is the error from os.Lstat itself.
//
// If it returns filepath.SkipDir, LWalk skips the directory's contents.
// If it returns filepath.SkipAll, LWalk skips all directories and files not visited yet,
// and returns nil.
// If it returns ResolveSymlink, LWalk resolves the symlink and continues walking.
//
// If visit returns other error, LWalk stops the walk and returns the error.
// In this case, leave is not called for the item caused error.
//
// - leave: the function called after all the directory's contents have been visited.
// It can be nil when you don't need to visit files in postorder.
//
// The path and info arguments are same as visit.
//
// The err argument is the error from walking the directory's contents.
//
// Note that both info and err can be nil togather when a lstat for a file is failed.
//
// If it returns filepath.SkipDir, it is ignored.
//
// If it returns filepath.SkipAll, LWalk stops the walk and returns nil.
//
// if leave returns other error, LWalk stops the walk and returns the error.
func LWalk(
	dir string,
	visit func(path string, info os.FileInfo, err error) error,
	leave func(path string, info os.FileInfo, err error) error,
) error {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return err
	}

	info, err := os.Lstat(dir)
	if err != nil {
		info = nil
	}

	if visit != nil {
		if err := visit(dir, info, err); errors.Is(err, filepath.SkipDir) {
			return nil
		} else if err != nil {
			return err
		}
	}

	var walkerr error
	if info.IsDir() {
		walkerr = lwalk(dir, visit, leave)
	}

	if leave == nil {
		return walkerr
	}

	if err := leave(dir, info, walkerr); errors.Is(err, filepath.SkipDir) {
		return nil
	} else if err != nil {
		return err
	}
	return nil
}

func lwalk(
	dir string,
	visit func(path string, info os.FileInfo, err error) error,
	leave func(path string, info os.FileInfo, err error) error,
) error {
	d, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, de := range d {
		path := filepath.Join(dir, de.Name())
		info, err := os.Lstat(path)
		if err != nil {
			info = nil
		}
		if visit != nil {
			if err := visit(path, info, err); errors.Is(err, filepath.SkipDir) {
				continue
			} else if err != nil {
				return err
			}
		}

		var walkerr error
		if info.IsDir() {
			walkerr = lwalk(path, visit, leave)
		}

		if leave == nil {
			if walkerr != nil {
				return walkerr
			}
			continue
		}

		if err := leave(path, info, walkerr); errors.Is(err, filepath.SkipDir) {
			// pass: this item is already visited.
		} else if err != nil {
			return err
		}
	}

	return nil
}
