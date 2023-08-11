package merklet

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"

	"github.com/cbergoon/merkletree"
)

var hasher = func() hash.Hash {
	return sha256.New()
}

type filePathBasedContent struct {
	path  string
	fstat os.FileInfo
}

func (t filePathBasedContent) CalculateHash() ([]byte, error) {
	file, err := os.Open(t.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	h := hasher()
	if _, err := io.Copy(h, file); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

func (t filePathBasedContent) Equals(other merkletree.Content) (bool, error) {
	b, ok := other.(filePathBasedContent)
	if !ok {
		return false, nil
	}
	return t.path == b.path && t.fstat == b.fstat, nil
}

type readableContent struct {
	r io.Reader
}

func (t readableContent) CalculateHash() ([]byte, error) {
	h := hasher()
	if _, err := io.Copy(h, t.r); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

func (t readableContent) Equals(other merkletree.Content) (bool, error) {
	b, ok := other.(readableContent)
	if !ok {
		return false, nil
	}
	return t.r == b.r, nil
}

var (
	_ merkletree.Content = &filePathBasedContent{}
	_ merkletree.Content = &readableContent{}
)

func NewFromFilePaths(paths []string) (*merkletree.MerkleTree, error) {
	if len(paths) == 0 {
		return nil, errors.New("Empty data")
	}
	contents := make([]merkletree.Content, len(paths))
	for i, p := range paths {
		fstat, err := os.Stat(p)
		if err != nil {
			return nil, err
		}
		if fstat.IsDir() {
			return nil, errors.New(fmt.Sprintf("the path:%s must not be a directory", p))
		}
		contents[i] = filePathBasedContent{p, fstat}
	}
	return merkletree.NewTree(contents)
}

func NewFromInputStreams[T io.Reader](shards []T) (*merkletree.MerkleTree, error) {
	if shards == nil || len(shards) == 0 {
		return nil, errors.New("empty shards")
	}
	mtcs := make([]merkletree.Content, len(shards))
	for i, s := range shards {
		mtcs[i] = readableContent{s}
	}
	return merkletree.NewTree(mtcs)
}
