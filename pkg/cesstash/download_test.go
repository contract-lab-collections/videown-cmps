package cesstash

//TODO: to complete the Unit Test
// import (
// 	"cmps/pkg/cessc"

// 	"os"
// 	"path/filepath"
// 	"testing"
// 	"time"

// 	"github.com/centrifuge/go-substrate-rpc-client/v4/types"
// 	"github.com/centrifuge/go-substrate-rpc-client/v4/types/codec"

// 	"github.com/stretchr/testify/assert"
// )

// const fileId string = "d998cdd4a52fddb9cfc65e98ab42afbe2b279faf588bb1ea5b4e70ccf6db0af4"

// func newFileBlockId(str string) cessc.FileBlockId {
// 	r, err := cessc.NewFileBlockId(str)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return *r
// }

// func newIpv4Address(ipStr string, port uint16) cessc.Ipv4Type {
// 	r, err := cessc.NewIpv4Address(ipStr, port)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return *r
// }

// func MustNewAccountId(hex string) types.AccountID {
// 	b, err := codec.HexDecodeString(hex)
// 	if err != nil {
// 		panic(err)
// 	}
// 	acc, err := types.NewAccountID(b)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return *acc
// }

// func buildFileMeta() (fileMeta cessc.FileMetaInfo) {
// 	minerAcc := MustNewAccountId("0x1ec940be673d3613e94c4d44e3f4621422c1a0778a53a34b2b45f3118f823c03")
// 	chunks := [6]cessc.BlockInfo{}
// 	chunks[0] = cessc.BlockInfo{
// 		MinerId:   types.NewU64(8),
// 		BlockSize: types.NewU64(11224153),
// 		BlockId:   newFileBlockId("d998cdd4a52fddb9cfc65e98ab42afbe2b279faf588bb1ea5b4e70ccf6db0af4.000"),
// 		MinerIp:   newIpv4Address("127.0.0.1", 17001),
// 		MinerAcc:  minerAcc,
// 		BlockNum:  16,
// 	}
// 	chunks[1] = cessc.BlockInfo{
// 		MinerId:   types.NewU64(8),
// 		BlockSize: types.NewU64(11224153),
// 		BlockId:   newFileBlockId("d998cdd4a52fddb9cfc65e98ab42afbe2b279faf588bb1ea5b4e70ccf6db0af4.001"),
// 		MinerIp:   newIpv4Address("127.0.0.1", 17001),
// 		MinerAcc:  minerAcc,
// 		BlockNum:  16,
// 	}
// 	chunks[2] = cessc.BlockInfo{
// 		MinerId:   types.NewU64(8),
// 		BlockSize: types.NewU64(11224153),
// 		BlockId:   newFileBlockId("d998cdd4a52fddb9cfc65e98ab42afbe2b279faf588bb1ea5b4e70ccf6db0af4.002"),
// 		MinerIp:   newIpv4Address("127.0.0.1", 17001),
// 		MinerAcc:  minerAcc,
// 		BlockNum:  16,
// 	}
// 	chunks[3] = cessc.BlockInfo{
// 		MinerId:   types.NewU64(8),
// 		BlockSize: types.NewU64(11224153),
// 		BlockId:   newFileBlockId("d998cdd4a52fddb9cfc65e98ab42afbe2b279faf588bb1ea5b4e70ccf6db0af4.003"),
// 		MinerIp:   newIpv4Address("127.0.0.1", 17001),
// 		MinerAcc:  minerAcc,
// 		BlockNum:  16,
// 	}
// 	chunks[4] = cessc.BlockInfo{
// 		MinerId:   types.NewU64(8),
// 		BlockSize: types.NewU64(11224153),
// 		BlockId:   newFileBlockId("d998cdd4a52fddb9cfc65e98ab42afbe2b279faf588bb1ea5b4e70ccf6db0af4.004"),
// 		MinerIp:   newIpv4Address("127.0.0.1", 17001),
// 		MinerAcc:  minerAcc,
// 		BlockNum:  16,
// 	}
// 	chunks[5] = cessc.BlockInfo{
// 		MinerId:   types.NewU64(8),
// 		BlockSize: types.NewU64(11224153),
// 		BlockId:   newFileBlockId("d998cdd4a52fddb9cfc65e98ab42afbe2b279faf588bb1ea5b4e70ccf6db0af4.005"),
// 		MinerIp:   newIpv4Address("127.0.0.1", 17001),
// 		MinerAcc:  minerAcc,
// 		BlockNum:  16,
// 	}

// 	fileMeta = cessc.FileMetaInfo{
// 		Size:      types.NewU64(44896612),
// 		Index:     1,
// 		State:     []byte("active"),
// 		BlockInfo: chunks[:],
// 		UserBriefs: []cessc.UserBrief{
// 			{
// 				User:        MustNewAccountId("0x882be63cfe247ac0e62d42c59037de5fecc6283f2d5c3ca4696c489c7fd94720"),
// 				File_name:   []byte("testvideo.mp4"),
// 				Bucket_name: []byte("videown"),
// 			},
// 		},
// 	}
// 	return fileMeta
// }

// type cesscParam struct {
// 	rpcAddress   string
// 	secretPhrase string
// 	dataDir      string
// }

// func buildCesscParam() *cesscParam {
// 	return &cesscParam{
// 		rpcAddress:   "wss://testnet-rpc1.cess.cloud/ws/",
// 		secretPhrase: "tray fine poem nothing glimpse insane carbon empty grief dismiss bird nurse",
// 		dataDir:      "./.workdata",
// 	}
// }

// func buildCessc(param *cesscParam) cessc.Chainer {
// 	client, err := cessc.NewCesscWithSecretPhrase(param.rpcAddress, param.secretPhrase, 5*time.Second)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return client
// }

// func TestDownloadChunk(t *testing.T) {
// 	cesscParam := buildCesscParam()
// 	fsth := Must.NewFileStash(cesscParam.dataDir, cesscParam.secretPhrase, buildCessc(cesscParam))

// 	tmpChunkDir, err := os.MkdirTemp(fsth.chunksDir, "down-chunks-")
// 	assert.NoError(t, err)
// 	defer os.RemoveAll(tmpChunkDir)

// 	fileMeta := buildFileMeta()
// 	err = fsth.doDownloadChunk(&fileMeta.BlockInfo[0], &downloadDesc{
// 		cessFileId: fileId,
// 		meta:       &fileMeta,
// 		targetDir:  tmpChunkDir,
// 	})
// 	assert.NoError(t, err)

// 	fstat, err := os.Stat(filepath.Join(tmpChunkDir, fileMeta.BlockInfo[0].BlockId.String()))
// 	assert.NoError(t, err)
// 	assert.Equal(t, int64(fileMeta.BlockInfo[0].BlockSize), fstat.Size())
// }

// func TestDownloadFile(t *testing.T) {
// 	cesscParam := buildCesscParam()
// 	fsth := Must.NewFileStash(cesscParam.dataDir, cesscParam.secretPhrase, buildCessc(cesscParam))

// 	fileMeta := buildFileMeta()
// 	fbi, err := fsth.downloadFile(fileId, &fileMeta)
// 	assert.NoError(t, err)
// 	assert.Equal(t, int64(fileMeta.Size), fbi.Size)

// 	os.RemoveAll(fsth.Dir())
// }

// func buildFileMeta1() (fileMeta cessc.FileMetaInfo, _ string) {
// 	chunks := [6]cessc.BlockInfo{}
// 	chunks[0] = cessc.BlockInfo{
// 		MinerId:   types.NewU64(2382),
// 		BlockSize: types.NewU64(7028194),
// 		BlockId:   newFileBlockId("2608f4ae80e1cf53335b57b0db68eb7836313cac6a50563841e23585e5dac94f.000"),
// 		MinerIp:   newIpv4Address("182.150.116.152", 15001),
// 		MinerAcc:  MustNewAccountId("0xd24e0cc19e31d94ce06b3f2aaca43bc3473efcdc4d30a36a35d04c29a677ea4e"),
// 		BlockNum:  16,
// 	}
// 	chunks[1] = cessc.BlockInfo{
// 		MinerId:   types.NewU64(3119),
// 		BlockSize: types.NewU64(7028194),
// 		BlockId:   newFileBlockId("2608f4ae80e1cf53335b57b0db68eb7836313cac6a50563841e23585e5dac94f.001"),
// 		MinerIp:   newIpv4Address("136.243.215.250", 15036),
// 		MinerAcc:  MustNewAccountId("0xd26eaaefb0ea3ee3515a258a207ddf7051d5c86bce0f43647fbf7f24a0a26237"),
// 		BlockNum:  16,
// 	}
// 	chunks[2] = cessc.BlockInfo{
// 		MinerId:   types.NewU64(3734),
// 		BlockSize: types.NewU64(7028194),
// 		BlockId:   newFileBlockId("2608f4ae80e1cf53335b57b0db68eb7836313cac6a50563841e23585e5dac94f.002"),
// 		MinerIp:   newIpv4Address("183.232.117.117", 15005),
// 		MinerAcc:  MustNewAccountId("0x0c0e9bccd9973deef65d3c93eff377f7ad1ec2bd2c5c6949f9c199f4cfa7176a"),
// 		BlockNum:  16,
// 	}
// 	chunks[3] = cessc.BlockInfo{
// 		MinerId:   types.NewU64(3227),
// 		BlockSize: types.NewU64(7028194),
// 		BlockId:   newFileBlockId("2608f4ae80e1cf53335b57b0db68eb7836313cac6a50563841e23585e5dac94f.003"),
// 		MinerIp:   newIpv4Address("136.243.215.250", 15144),
// 		MinerAcc:  MustNewAccountId("0xba757a7c280774848c678bcca18210851315a89c71b7f4365264096f1f22e05c"),
// 		BlockNum:  16,
// 	}
// 	chunks[4] = cessc.BlockInfo{
// 		MinerId:   types.NewU64(2411),
// 		BlockSize: types.NewU64(7028194),
// 		BlockId:   newFileBlockId("2608f4ae80e1cf53335b57b0db68eb7836313cac6a50563841e23585e5dac94f.004"),
// 		MinerIp:   newIpv4Address("182.150.116.152", 15003),
// 		MinerAcc:  MustNewAccountId("0x7c87d950c40be6b89e492fe875ae0231556caa51a8a76501946f0f4c35ec122b"),
// 		BlockNum:  16,
// 	}
// 	chunks[5] = cessc.BlockInfo{
// 		MinerId:   types.NewU64(3572),
// 		BlockSize: types.NewU64(7028194),
// 		BlockId:   newFileBlockId("2608f4ae80e1cf53335b57b0db68eb7836313cac6a50563841e23585e5dac94f.005"),
// 		MinerIp:   newIpv4Address("136.243.215.242", 15176),
// 		MinerAcc:  MustNewAccountId("0xf89c1e115d8a57506c715eeba98754335d8ce5edc684837aa72444d31305be4c"),
// 		BlockNum:  16,
// 	}

// 	fileMeta = cessc.FileMetaInfo{
// 		Size:      types.NewU64(28112776),
// 		Index:     3123,
// 		State:     []byte("active"),
// 		BlockInfo: chunks[:],
// 		UserBriefs: []cessc.UserBrief{
// 			{
// 				User:        MustNewAccountId("0xb4ac8b37a20935b0b390c61cd849ea533b060bb97e32b68801ec3b6220a8e131"),
// 				File_name:   []byte("testvideo2.mp4"),
// 				Bucket_name: []byte("videown"),
// 			},
// 		},
// 	}
// 	return fileMeta, "2608f4ae80e1cf53335b57b0db68eb7836313cac6a50563841e23585e5dac94f"
// }

// func TestDownloadChunk1(t *testing.T) {
// 	cesscParam := buildCesscParam()
// 	fsth := Must.NewFileStash(cesscParam.dataDir, cesscParam.secretPhrase, nil) //buildCessc(cesscParam))

// 	tmpChunkDir, err := os.MkdirTemp(fsth.chunksDir, "down-chunks-")
// 	assert.NoError(t, err)
// 	defer os.RemoveAll(tmpChunkDir)

// 	fileMeta, fileId := buildFileMeta1()
// 	err = fsth.doDownloadChunk(&fileMeta.BlockInfo[0], &downloadDesc{
// 		cessFileId: fileId,
// 		meta:       &fileMeta,
// 		targetDir:  tmpChunkDir,
// 	})
// 	assert.NoError(t, err)

// 	fstat, err := os.Stat(filepath.Join(tmpChunkDir, fileMeta.BlockInfo[0].BlockId.String()))
// 	assert.NoError(t, err)
// 	assert.Equal(t, int64(fileMeta.BlockInfo[0].BlockSize), fstat.Size())
// }

// func TestDownloadFile1(t *testing.T) {
// 	cesscParam := buildCesscParam()
// 	fsth := Must.NewFileStash(cesscParam.dataDir, cesscParam.secretPhrase, buildCessc(cesscParam))

// 	fileMeta, fileId := buildFileMeta1()
// 	fbi, err := fsth.downloadFile(fileId, &fileMeta)
// 	assert.NoError(t, err)
// 	assert.Equal(t, int64(fileMeta.Size), fbi.Size)

// 	os.RemoveAll(fsth.Dir())
// }
