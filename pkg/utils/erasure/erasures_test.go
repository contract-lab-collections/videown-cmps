package erasure

import (
	"bufio"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/pkg/errors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func generateFile(fileName string, numOfNums int64) error {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	for i := int64(1); i <= numOfNums; i++ {
		err := writer.WriteByte(byte(i & 0xff))
		if err != nil {
			return err
		}
	}
	return writer.Flush()
}

type shardCber struct {
	shardCreater func(index int) (io.WriteCloser, error)
	shardOpener  func(index int) (io.ReadCloser, int64, error)
}

func shardCberForEncode(targetDir string, filePrefix string) shardCber {
	shardCreater := func(index int) (io.WriteCloser, error) {
		return os.Create(filepath.Join(targetDir, fmt.Sprintf("%s.%d", filePrefix, index)))
	}
	shardOpener := func(index int) (io.ReadCloser, int64, error) {
		f, err := os.Open(filepath.Join(targetDir, fmt.Sprintf("%s.%d", filePrefix, index)))
		if err != nil {
			return nil, 0, err
		}
		fstat, err := f.Stat()
		if err != nil {
			return nil, 0, err
		}
		return f, fstat.Size(), nil
	}
	return shardCber{shardCreater, shardOpener}
}

func shardCberForDecode(shardsDir string, filePrefix string) shardCber {
	shardCreater := func(index int) (io.WriteCloser, error) {
		return os.Create(filepath.Join(shardsDir, fmt.Sprintf("%s.%d", filePrefix, index)))
	}
	shardOpener := func(index int) (io.ReadCloser, int64, error) {
		fp := filepath.Join(shardsDir, fmt.Sprintf("%s.%d", filePrefix, index))
		fstat, err := os.Stat(fp)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, 0, nil
			} else {
				return nil, 0, err
			}
		}
		f, err := os.Open(fp)
		if err != nil {
			return nil, 0, err
		}
		return f, fstat.Size(), nil
	}
	return shardCber{shardCreater, shardOpener}
}

func prepareDir(subDirName string, clear bool) (string, error) {
	dir := filepath.Join(".test-tmp", subDirName)
	if clear {
		cmd := exec.Command("rm", "-rf", dir)
		if err := cmd.Run(); err != nil {
			return "", err
		}
		cmd = exec.Command("mkdir", "-p", dir)
		if err := cmd.Run(); err != nil {
			return "", err
		}
	}
	return dir, nil
}

func sha256hash(input io.Reader) ([]byte, error) {
	h := sha256.New()
	if _, err := io.Copy(h, input); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

func fileSha256Hash(filepath string) ([]byte, error) {
	input, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	return sha256hash(input)
}

func encodeAndDecode(t *testing.T, inputFilename string, expectSize int64) {
	assert := assert.New(t)
	require := require.New(t)
	clearDir := true

	//encode
	encodeTargetDir, err := prepareDir("", clearDir)
	require.NoError(err)
	defer os.RemoveAll(encodeTargetDir)

	inputFilePath := filepath.Join(encodeTargetDir, inputFilename)
	if clearDir {
		require.NoError(generateFile(inputFilePath, expectSize))
		t.Logf("input file: %s is ready, size: %d", inputFilePath, expectSize)
	}
	inputFileInfo, err := os.Stat(inputFilePath)
	require.NoError(err)
	require.Equal(expectSize, inputFileInfo.Size())

	inputFile, err := os.Open(inputFilePath)
	require.NoError(err)
	defer inputFile.Close()

	scber := shardCberForEncode(encodeTargetDir, inputFilename)
	b := RsecEncodeByFile(inputFile)
	b.WithShardCreater(scber.shardCreater).WithShardOpener(scber.shardOpener)
	rsece, err := b.Build()
	require.NoError(err)
	require.Equal(expectSize, rsece.InputSize())
	datas, pars := reedSolomonRule(inputFileInfo.Size())
	shards := datas + pars
	t.Logf("fileSize:%d, dataShards:%d, parityShards:%d", inputFileInfo.Size(), rsece.DataShards(), rsece.ParityShards())
	require.Equal(rsece.DataShards(), datas)
	require.Equal(rsece.ParityShards(), pars)
	//DO encode
	require.NoError(rsece.Encode())
	//verify shards
	dirEntries, err := os.ReadDir(encodeTargetDir)
	require.NoError(err)
	assert.Equal(shards+1, len(dirEntries))

	//decode
	os.Remove(filepath.Join(encodeTargetDir, inputFilename+".0"))
	os.Remove(filepath.Join(encodeTargetDir, inputFilename+".1"))

	outputFilepath := filepath.Join(encodeTargetDir, "recover_"+inputFilename)
	outputFile, err := os.Create(outputFilepath)
	require.NoError(err)
	defer outputFile.Close()
	scber = shardCberForDecode(encodeTargetDir, inputFilename)
	b1 := RsecDecodeByStream(outputFile, expectSize)
	b1.WithShardCreater(scber.shardCreater).WithShardOpener(scber.shardOpener).WithShards(datas, pars)
	rsecd, err := b1.Build()
	require.NoError(err)
	err = rsecd.Decode()
	require.NoError(err)
	outputFileInfo, err := os.Stat(outputFilepath)
	require.NoError(err)
	require.Equal(inputFileInfo.Size(), outputFileInfo.Size())
	//verfiy two file hash
	inputFileHash, err := fileSha256Hash(inputFilePath)
	require.NoError(err)
	outputFileHash, err := fileSha256Hash(outputFilepath)
	require.NoError(err)
	assert.Equal(inputFileHash, outputFileHash)
}

func TestEncodeDecode1Gib(t *testing.T) {
	encodeAndDecode(t, "abc.dat", 1<<30) //1 GiB
}

func TestEncodeDecode2Gib(t *testing.T) {
	encodeAndDecode(t, "abc.dat", 2<<30) //2 GiB
}

func TestEncodeDecode5Gib(t *testing.T) {
	encodeAndDecode(t, "abc.dat", 5<<30) //5 GiB
}
