package cesstash

import (
	"cmps/pkg/cesstash/shim/cesssc"
	"cmps/pkg/utils/hash"
	"encoding/json"
	"fmt"
	"io"

	"os"
	"path/filepath"
	"time"

	cesspat "github.com/CESSProject/cess-go-sdk/core/pattern"
	cessdk "github.com/CESSProject/cess-go-sdk/core/sdk"
	cesskeyring "github.com/CESSProject/go-keyring"
	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	"github.com/pkg/errors"
)

var Logger logr.Logger

func init() {
	Logger = funcr.New(func(prefix, args string) {
		fmt.Println(prefix, args)
	}, funcr.Options{
		Verbosity: 3,
		LogCaller: funcr.All,
	}).WithName("cesstash")
}

func SetLogger(logger logr.Logger) {
	Logger = logger.WithName("cesstash")
}

type CessStash struct {
	fileStashDir    string
	chunksDir       string
	keyring         *cesskeyring.KeyRing
	cessc           cessdk.SDK
	cessfsc         *cesssc.CessStorageClient
	log             logr.Logger
	stashWhenUpload bool

	relayHandlers       map[FileHash]*RelayHandler
	relayHandlerPutChan chan *RelayHandler
}

type FileHash = hash.H256

const (
	_FileStashDirName = "stashs"
	_DataFilename     = "data"
	_MetaFilename     = "meta.json"
)

var Must _must

type _must struct {
}

func (t _must) NewFileStash(parentDir string, secretPhrase string, cessc cessdk.SDK, cessfsc *cesssc.CessStorageClient) *CessStash {
	fs, err := NewFileStash(parentDir, secretPhrase, cessc, cessfsc)
	if err != nil {
		panic(err)
	}
	return fs
}

func NewFileStash(parentDir string, secretPhrase string, cessc cessdk.SDK, cessfsc *cesssc.CessStorageClient) (*CessStash, error) {
	keyring, err := cesskeyring.FromURI(secretPhrase, cesskeyring.NetSubstrate{})
	if err != nil {
		return nil, err
	}

	fsd := filepath.Join(parentDir, _FileStashDirName)
	if err := os.MkdirAll(fsd, 0755); err != nil {
		return nil, err
	}
	ckd := filepath.Join(parentDir, _FileStashDirName, ".chunks")
	if err := os.MkdirAll(ckd, 0755); err != nil {
		return nil, err
	}
	fsth := &CessStash{
		log:                 Logger.WithName("filestash"),
		fileStashDir:        fsd,
		chunksDir:           ckd,
		keyring:             keyring,
		cessc:               cessc,
		cessfsc:             cessfsc,
		relayHandlers:       make(map[FileHash]*RelayHandler),
		relayHandlerPutChan: make(chan *RelayHandler),
	}
	startCleanCompleteRelayHandlerTask(fsth)
	return fsth, nil
}

func startCleanCompleteRelayHandlerTask(fsth *CessStash) {
	go func() {
		for {
			for k, rh := range fsth.relayHandlers {
				if rh.IsProcessing() {
					continue
				}
				if rh.ReRelayIfAbort() {
					continue
				}
				if rh.CanClean() {
					rh.close()
					delete(fsth.relayHandlers, k)
					fsth.log.Info("clean relay handler", "uploadId", rh.Id())
				}
				time.Sleep(time.Second)
			}
			time.Sleep(time.Second)
		}
	}()
}

func (t *CessStash) Dir() string { return t.fileStashDir }

func (t *CessStash) SetStashWhenUpload(value bool) { t.stashWhenUpload = value }

type FileBriefInfo struct {
	OriginName string
	CessFileId string
	FileHash   string
	FilePath   string
	Size       int64
}

type SimpleFileMeta struct {
	FileHash   string `json:"fileHash"`
	CessFileId string `json:"cessFileId"`
	OriginName string `json:"originName"`
}

func (t *CessStash) FileInfoById(cessFileId string) (*FileBriefInfo, error) {
	dataFilename := filepath.Join(t.fileStashDir, cessFileId, _DataFilename)
	fstat, err := os.Stat(dataFilename)
	if err != nil {
		return nil, err
	}
	r := FileBriefInfo{
		OriginName: cessFileId,
		CessFileId: cessFileId,
		FilePath:   dataFilename,
		Size:       fstat.Size(),
	}
	sfm, err := t.loadSimpleFileMeta(cessFileId)
	if sfm != nil && sfm.OriginName != "" {
		r.OriginName = sfm.OriginName
	}
	return &r, err
}

func (t *CessStash) loadSimpleFileMeta(cessFileId string) (*SimpleFileMeta, error) {
	metabs, err := os.ReadFile(filepath.Join(t.fileStashDir, cessFileId, _MetaFilename))
	if err != nil {
		return nil, err
	}

	var sfm SimpleFileMeta
	if err := json.Unmarshal(metabs, &sfm); err != nil {
		return nil, err
	}
	return &sfm, nil
}

func (t *CessStash) storeSimpleFileMeta(sfm *SimpleFileMeta) error {
	if sfm.CessFileId == "" {
		return errors.New("fileHash field must not be empty")
	}
	bytes, err := json.Marshal(sfm)
	if err != nil {
		return err
	}
	metaFilename := filepath.Join(t.fileStashDir, sfm.CessFileId, _MetaFilename)
	if err := os.WriteFile(metaFilename, bytes, os.ModePerm); err != nil {
		return err
	}
	return nil
}

func (t *CessStash) ensureCessFileDir(dirname string) (string, error) {
	fileHashDir := filepath.Join(t.fileStashDir, dirname)
	if _, err := os.Stat(fileHashDir); os.IsNotExist(err) {
		err = os.Mkdir(fileHashDir, 0755)
		if err != nil {
			return "", errors.Wrap(err, "make cess file dir error")
		}
	}
	return fileHashDir, nil
}

func (t *CessStash) createEmptyCessFile(cessFileId string) (*os.File, error) {
	dir, err := t.ensureCessFileDir(cessFileId)
	if err != nil {
		return nil, err
	}
	return os.Create(filepath.Join(dir, "data"))
}

func (t *CessStash) DownloadFile(cessFileId string) (*FileBriefInfo, error) {
	fmeta, err := t.cessc.QueryFileMetadata(cessFileId)
	if err != nil {
		return nil, err
	}
	t.log.V(1).Info("download file", "fileMeta", fmeta)

	if int(fmeta.State) != cesspat.Active {
		return nil, errors.New("the file is pending")
	}

	return t.downloadFile(cessFileId, &fmeta)
}

func (t *CessStash) RemoveFile(cessFileId string) error {
	fileHashDir := filepath.Join(t.fileStashDir, cessFileId)
	if _, err := os.Stat(fileHashDir); os.IsNotExist(err) {
		return nil
	}
	return os.RemoveAll(fileHashDir)
}

func (t *CessStash) stashFile(cessFileId string, src io.Reader, originName string) error {
	fhdf, err := t.createEmptyCessFile(cessFileId)
	if err != nil {
		return err
	}
	if _, err := io.Copy(fhdf, src); err != nil {
		return err
	}

	return t.storeSimpleFileMeta(&SimpleFileMeta{
		CessFileId: cessFileId,
		OriginName: filepath.Base(originName),
	})
}
