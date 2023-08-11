package segment

import (
	"cmps/pkg/utils/erasure"
	"cmps/pkg/utils/hash"
	"cmps/pkg/utils/merklet"
	"fmt"
	"io"
	"os"
	"path/filepath"

	cesspat "github.com/CESSProject/cess-go-sdk/core/pattern"

	"github.com/pkg/errors"
)

const SEGMENT_SIZE = cesspat.SegmentSize

func doSegmentsWrite[W io.Writer](data io.Reader, dst []W, size int64) error {
	if size == 0 {
		return errors.New("size must not be zero")
	}
	if len(dst) == 0 {
		return errors.New("dst must not be empty")
	}
	segCount := size / SEGMENT_SIZE
	if size%int64(SEGMENT_SIZE) != 0 {
		paddingSize := SEGMENT_SIZE - (size - segCount*SEGMENT_SIZE)
		data = io.MultiReader(data, io.LimitReader(zeroPaddingReader{}, paddingSize))
		segCount++
	}
	if int(segCount) != len(dst) {
		return errors.New("segmuent dst count not match")
	}

	// Split into equal-length segment.
	for i := range dst {
		n, err := io.CopyN(dst[i], data, SEGMENT_SIZE)
		if err != io.EOF && err != nil {
			return err
		}
		if n != SEGMENT_SIZE {
			return errors.New("write short data")
		}
	}

	return nil
}

type zeroPaddingReader struct{}

var _ io.Reader = &zeroPaddingReader{}

func (t zeroPaddingReader) Read(p []byte) (n int, err error) {
	n = len(p)
	for i := 0; i < n; i++ {
		p[i] = 0
	}
	return n, nil
}

func splitStreamToSegments(inputStream io.Reader, size int64, outputDir string) ([]string, error) {
	segmentCount := int(size / SEGMENT_SIZE)
	if size%int64(SEGMENT_SIZE) != 0 {
		segmentCount++
	}
	segmentFiles := make([]*os.File, segmentCount)
	for i := 0; i < segmentCount; i++ {
		f, err := os.Create(filepath.Join(outputDir, fmt.Sprintf("seg-%d", i)))
		if err != nil {
			return nil, err
		}
		segmentFiles[i] = f
	}
	if err := doSegmentsWrite(inputStream, segmentFiles, size); err != nil {
		return nil, err
	}
	segmentPaths := make([]string, segmentCount)
	for i, f := range segmentFiles {
		segmentPaths[i] = f.Name()
		f.Close()
	}
	return segmentPaths, nil
}

func splitFileToSegments(srcFile *os.File, outputDir string) ([]string, error) {
	fstat, err := srcFile.Stat()
	if err != nil {
		return nil, err
	}
	if fstat.IsDir() {
		return nil, errors.New("not a file")
	}

	return splitStreamToSegments(srcFile, fstat.Size(), outputDir)
}

func shardSegmentToFrags(segmentFile *os.File, outputDir string) ([]string, error) {
	fragPaths := make([]string, 0, 16)
	builder := erasure.RsecEncodeByFile(segmentFile)
	builder.WithShards(cesspat.DataShards, cesspat.ParShards)
	builder.WithShardCreater(
		func(index int) (io.WriteCloser, error) {
			p := filepath.Join(outputDir, fmt.Sprintf("frag-%d", index))
			f, err := os.Create(p)
			if err != nil {
				return nil, err
			}
			fragPaths = append(fragPaths, p)
			return f, nil
		})
	builder.WithShardOpener(
		func(index int) (io.ReadCloser, int64, error) {
			f, err := os.Open(filepath.Join(outputDir, fmt.Sprintf("frag-%d", index)))
			if err != nil {
				return nil, 0, err
			}
			fstat, err := f.Stat()
			if err != nil {
				return nil, 0, err
			}
			return f, fstat.Size(), nil
		})
	rsece, err := builder.Build()
	if err != nil {
		return nil, err
	}

	if err := rsece.Encode(); err != nil {
		return nil, err
	}
	return fragPaths, nil
}

type Fragment struct {
	Hash     *hash.H256 `json:"hash"`
	FilePath string     `json:"filePath"`
}

type Segment struct {
	Hash     *hash.H256 `json:"hash"`
	FilePath string     `json:"filePath"`
	Frags    []Fragment `json:"frags"`
}

type FileSegmentMeta struct {
	OutputDir string     `json:"outputDir"`
	InputSize int64      `json:"inputSize"`
	Name      string     `json:"name"`
	RootHash  *hash.H256 `json:"rootHash"`
	Segments  []Segment  `json:"segments"`
}

func (t *FileSegmentMeta) ExtractFrags() []Fragment {
	frags := make([]Fragment, 0, len(t.Segments)*3)
	for _, seg := range t.Segments {
		for _, frag := range seg.Frags {
			frags = append(frags, frag)
		}
	}
	return frags
}

func CreateByStream(inputStream io.Reader, size int64, outputDir string) (*FileSegmentMeta, error) {
	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		err = os.Mkdir(outputDir, 0755)
		if err != nil {
			return nil, err
		}
	}
	segPaths, err := splitStreamToSegments(inputStream, size, outputDir)
	if err != nil {
		return nil, err
	}
	merkletree, err := merklet.NewFromFilePaths(segPaths)
	if err != nil {
		return nil, err
	}

	result := &FileSegmentMeta{
		OutputDir: outputDir,
		InputSize: size,
		RootHash:  (*hash.H256)(merkletree.MerkleRoot()),
		Segments:  make([]Segment, len(segPaths)),
	}
	for i, sp := range segPaths {
		f, err := os.Open(sp)
		if err != nil {
			return nil, err
		}
		fragsDir := filepath.Join(outputDir, fmt.Sprintf("seg-%d_frags", i))
		if _, err := os.Stat(fragsDir); errors.Is(err, os.ErrNotExist) {
			err := os.Mkdir(fragsDir, os.ModePerm)
			if err != nil {
				return nil, err
			}
		}
		fragPaths, err := shardSegmentToFrags(f, fragsDir)
		if err != nil {
			return nil, err
		}
		frags := make([]Fragment, 0, len(fragPaths))
		for _, fp := range fragPaths {
			fragHash, err := calcFileHash(fp)
			if err != nil {
				return nil, err
			}
			frags = append(frags, Fragment{FilePath: fp, Hash: fragHash})
		}
		spHash, err := calcFileHash(sp)
		if err != nil {
			return nil, err
		}
		result.Segments[i] = Segment{
			Hash:     spHash,
			FilePath: sp,
			Frags:    frags,
		}
	}

	return result, err
}

func calcFileHash(filePath string) (*hash.H256, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return hash.Calculate(f)
}

func CreateByFile(srcFile *os.File, outputDir string) (*FileSegmentMeta, error) {
	fstat, err := srcFile.Stat()
	if err != nil {
		return nil, err
	}
	if fstat.IsDir() {
		return nil, errors.New("not a file")
	}
	return CreateByStream(srcFile, fstat.Size(), outputDir)
}
