package cesstash

import (
	"errors"
	"io"
	"mime/multipart"
	"os"
)

type Filer interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
}

type FileHeader interface {
	Open() (Filer, error)
	Size() int64
	Filename() string
}

type normalFileHeader struct {
	fileStat os.FileInfo
	filePath string
}

var _ FileHeader = &normalFileHeader{}

func (t normalFileHeader) Open() (Filer, error) {
	return os.Open(t.filePath)
}

func (t normalFileHeader) Size() int64 {
	return t.fileStat.Size()
}

func (t normalFileHeader) Filename() string {
	return t.filePath
}

func NormalFile(filename string) (FileHeader, error) {
	fstat, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}
	nfh := normalFileHeader{fstat, filename}
	return &nfh, nil
}

type multipartFileHeader struct {
	fh *multipart.FileHeader
}

var _ FileHeader = &multipartFileHeader{}

func (t multipartFileHeader) Open() (Filer, error) {
	return t.fh.Open()
}

func (t multipartFileHeader) Size() int64 {
	return t.fh.Size
}

func (t multipartFileHeader) Filename() string {
	return t.fh.Filename
}

func MultipartFile(fh *multipart.FileHeader) (FileHeader, error) {
	if fh == nil {
		return nil, errors.New("fh argument must not be nil")
	}
	mfh := multipartFileHeader{fh}
	return &mfh, nil
}
