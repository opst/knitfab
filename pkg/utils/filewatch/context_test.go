package filewatch_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/opst/knitfab/pkg/utils/filewatch"
)

func TestUntilModifyContext_FileCreated(t *testing.T) {
	t.Run("when a file is created in a watched directory, it cancels context", func(t *testing.T) {
		dir := t.TempDir()

		basectx := context.Background()
		ctx, cancel, err := filewatch.UntilModifyContext(basectx, dir)
		if err != nil {
			t.Fatal(err)
		}
		defer cancel()

		if err := ctx.Err(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		file := filepath.Join(dir, "file")
		if f, err := os.Create(file); err != nil {
			t.Fatal(err)
		} else {
			f.Close()
		}

		deadlineCh := make(<-chan time.Time)
		if dl, ok := t.Deadline(); ok {
			deadlineCh = time.After(time.Until(dl) - 1*time.Second)
		}
		select {
		case <-ctx.Done():
			return
		case <-deadlineCh:
		}
		t.Fatalf("expected error, but got nil")
	})
}

func TestUntilModifyContext_FileWritten(t *testing.T) {
	t.Run("when a file is written in a watched directory, it cancels context", func(t *testing.T) {
		dir := t.TempDir()
		file := filepath.Join(dir, "file")
		if f, err := os.Create(file); err != nil {
			t.Fatal(err)
		} else {
			f.Close()
		}

		basectx := context.Background()
		ctx, cancel, err := filewatch.UntilModifyContext(basectx, dir)
		if err != nil {
			t.Fatal(err)
		}
		defer cancel()

		if err := ctx.Err(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := os.WriteFile(file, []byte("content"), 0644); err != nil {
			t.Fatal(err)
		}

		deadlineCh := make(<-chan time.Time)
		if dl, ok := t.Deadline(); ok {
			deadlineCh = time.After(time.Until(dl) - 1*time.Second)
		}
		select {
		case <-ctx.Done():
			return
		case <-deadlineCh:
		}
		t.Fatalf("expected error, but got nil")
	})

	t.Run("when a watched file is written, it cancels context", func(t *testing.T) {
		dir := t.TempDir()
		file := filepath.Join(dir, "file")
		if f, err := os.Create(file); err != nil {
			t.Fatal(err)
		} else {
			f.Close()
		}

		basectx := context.Background()
		ctx, cancel, err := filewatch.UntilModifyContext(basectx, file)
		if err != nil {
			t.Fatal(err)
		}
		defer cancel()

		if err := ctx.Err(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := os.WriteFile(file, []byte("content"), 0644); err != nil {
			t.Fatal(err)
		}

		deadlineCh := make(<-chan time.Time)
		if dl, ok := t.Deadline(); ok {
			deadlineCh = time.After(time.Until(dl) - 1*time.Second)
		}
		select {
		case <-ctx.Done():
			return
		case <-deadlineCh:
		}
		t.Fatalf("expected error, but got nil")
	})
}

func TestUntilModifyContext_FileDeleted(t *testing.T) {
	t.Run("when a file in the watched directory is deleted, it cancels context", func(t *testing.T) {
		dir := t.TempDir()
		file := filepath.Join(dir, "file")
		if f, err := os.Create(file); err != nil {
			t.Fatal(err)
		} else {
			f.Close()
		}

		basectx := context.Background()
		ctx, cancel, err := filewatch.UntilModifyContext(basectx, dir)
		if err != nil {
			t.Fatal(err)
		}
		defer cancel()

		if err := ctx.Err(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := os.Remove(file); err != nil {
			t.Fatal(err)
		}

		deadlineCh := make(<-chan time.Time)
		if dl, ok := t.Deadline(); ok {
			deadlineCh = time.After(time.Until(dl) - 1*time.Second)
		}
		select {
		case <-ctx.Done():
			return
		case <-deadlineCh:
		}
		t.Fatalf("expected error, but got nil")
	})

	t.Run("when the watched file is deleted, it cancels context", func(t *testing.T) {
		dir := t.TempDir()
		file := filepath.Join(dir, "file")
		if f, err := os.Create(file); err != nil {
			t.Fatal(err)
		} else {
			f.Close()
		}

		basectx := context.Background()
		ctx, cancel, err := filewatch.UntilModifyContext(basectx, dir)
		if err != nil {
			t.Fatal(err)
		}
		defer cancel()

		if err := ctx.Err(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := os.Remove(file); err != nil {
			t.Fatal(err)
		}

		deadlineCh := make(<-chan time.Time)
		if dl, ok := t.Deadline(); ok {
			deadlineCh = time.After(time.Until(dl) - 1*time.Second)
		}
		select {
		case <-ctx.Done():
			return
		case <-deadlineCh:
		}
		t.Fatalf("expected error, but got nil")
	})
}

func TestUntilModifyContext_FileRenamed(t *testing.T) {
	t.Run("when a file in the watched directory is renamed, it cancels context", func(t *testing.T) {
		dir := t.TempDir()
		file := filepath.Join(dir, "file")
		if f, err := os.Create(file); err != nil {
			t.Fatal(err)
		} else {
			f.Close()
		}

		basectx := context.Background()
		ctx, cancel, err := filewatch.UntilModifyContext(basectx, dir)
		if err != nil {
			t.Fatal(err)
		}
		defer cancel()

		if err := ctx.Err(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := os.Rename(file, filepath.Join(dir, "renamed")); err != nil {
			t.Fatal(err)
		}

		deadlineCh := make(<-chan time.Time)
		if dl, ok := t.Deadline(); ok {
			deadlineCh = time.After(time.Until(dl) - 1*time.Second)
		}
		select {
		case <-ctx.Done():
			return
		case <-deadlineCh:
		}
		t.Fatalf("expected error, but got nil")
	})

	t.Run("when the watched file is renamed, it cancels context", func(t *testing.T) {
		dir := t.TempDir()
		file := filepath.Join(dir, "file")
		if f, err := os.Create(file); err != nil {
			t.Fatal(err)
		} else {
			f.Close()
		}

		basectx := context.Background()
		ctx, cancel, err := filewatch.UntilModifyContext(basectx, dir)
		if err != nil {
			t.Fatal(err)
		}
		defer cancel()

		if err := ctx.Err(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if err := os.Rename(file, filepath.Join(dir, "renamed")); err != nil {
			t.Fatal(err)
		}

		deadlineCh := make(<-chan time.Time)
		if dl, ok := t.Deadline(); ok {
			deadlineCh = time.After(time.Until(dl) - 1*time.Second)
		}
		select {
		case <-ctx.Done():
			return
		case <-deadlineCh:
		}
		t.Fatalf("expected error, but got nil")
	})
}

func TestUntilModifyContext_FileModeChanged(t *testing.T) {
	t.Run("when a file in the watched directory is changed its mode, it cancels context", func(t *testing.T) {
		dir := t.TempDir()
		file := filepath.Join(dir, "file")
		if f, err := os.Create(file); err != nil {
			t.Fatal(err)
		} else {
			f.Close()
		}

		basectx := context.Background()
		ctx, cancel, err := filewatch.UntilModifyContext(basectx, dir)
		if err != nil {
			t.Fatal(err)
		}
		defer cancel()

		if err := ctx.Err(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// surely change mode despite of umask.
		if err := os.Chmod(file, os.FileMode(0o700)); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(file, os.FileMode(0o644)); err != nil {
			t.Fatal(err)
		}

		deadlineCh := make(<-chan time.Time)
		if dl, ok := t.Deadline(); ok {
			deadlineCh = time.After(time.Until(dl) - 1*time.Second)
		}
		select {
		case <-ctx.Done():
			return
		case <-deadlineCh:
		}
		t.Fatalf("expected error, but got nil")
	})

	t.Run("when the watched file mode is changed, it cancels context", func(t *testing.T) {
		dir := t.TempDir()
		file := filepath.Join(dir, "file")
		if f, err := os.Create(file); err != nil {
			t.Fatal(err)
		} else {
			f.Close()
		}

		basectx := context.Background()
		ctx, cancel, err := filewatch.UntilModifyContext(basectx, dir)
		if err != nil {
			t.Fatal(err)
		}
		defer cancel()

		if err := ctx.Err(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// surely change mode despite of umask.
		if err := os.Chmod(file, os.FileMode(0o700)); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(file, os.FileMode(0o644)); err != nil {
			t.Fatal(err)
		}

		deadlineCh := make(<-chan time.Time)
		if dl, ok := t.Deadline(); ok {
			deadlineCh = time.After(time.Until(dl) - 1*time.Second)
		}
		select {
		case <-ctx.Done():
			return
		case <-deadlineCh:
		}
		t.Fatalf("expected error, but got nil")
	})
}
