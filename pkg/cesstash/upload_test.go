package cesstash

//TODO: to complete the Unit Test
// import (
// 	"bufio"
// 	"crypto/rand"
// 	"io"
// 	"os"
// 	"path/filepath"
// 	"testing"

// 	"github.com/stretchr/testify/require"
// )

// func generateFile(fileName string, numOfNums int64) error {
// 	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
// 	if err != nil {
// 		return err
// 	}
// 	defer file.Close()
// 	writer := bufio.NewWriter(file)
// 	for i := int64(1); i <= numOfNums; i++ {
// 		err := writer.WriteByte(byte(i & 0xff))
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	return writer.Flush()
// }

// func generateFileWithRandContent(fileName string, size int64) error {
// 	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
// 	if err != nil {
// 		return err
// 	}
// 	defer file.Close()
// 	_, err = io.CopyN(file, rand.Reader, size)
// 	return err
// }

// func TestCutLargeFile(t *testing.T) {
// 	require := require.New(t)
// 	dir := ".test-tmp"
// 	require.NoError(os.MkdirAll(dir, 0744))
// 	p := filepath.Join(dir, "large.dat")
// 	require.NoError(generateFile(p, 5<<30)) //5 GiB
// 	defer os.Remove(p)
// 	t.Log("large file is ready")
// 	fstat, err := os.Stat(p)
// 	require.NoError(err)

// 	f, err := os.Open(p)
// 	r, err := cutToChunks(f, fstat.Size(), dir)
// 	require.NoError(err)
// 	require.True(len(r.chunkPaths) > 0)
// 	t.Log(r)
// }
