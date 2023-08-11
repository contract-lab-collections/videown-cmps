package cesstash

import (
	"fmt"
	"io"
	"sync"

	"cmps/pkg/cesstash/shim"
	"cmps/pkg/cesstash/shim/cesssc"
	"cmps/pkg/utils/erasure"
	"cmps/pkg/utils/hash"
	"os"
	"path/filepath"
	"time"

	cesspat "github.com/CESSProject/cess-go-sdk/core/pattern"
	cessdk "github.com/CESSProject/cess-go-sdk/core/sdk"
	"github.com/centrifuge/go-substrate-rpc-client/v4/types"
	"github.com/go-logr/logr"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/exp/slices"

	"github.com/pkg/errors"
)

type downloadDesc struct {
	cessFileId string
	meta       *cesspat.FileMetadata
	targetDir  string
}

func (t *CessStash) downloadFile(cessFileId string, fmeta *cesspat.FileMetadata) (*FileBriefInfo, error) {
	targetFile, err := t.createEmptyCessFile(cessFileId)
	if err != nil {
		return nil, err
	}
	defer targetFile.Close()
	tmpDownloadDir, err := os.MkdirTemp(t.chunksDir, "download-")
	defer os.RemoveAll(tmpDownloadDir)
	if err != nil {
		return nil, errors.Wrap(err, "make download tmp dir error")
	}

	dm, err := newDownloader(cessFileId, fmeta, tmpDownloadDir, targetFile, t.log, t.cessc, t.cessfsc)
	if err != nil {
		return nil, err
	}

	if err := dm.Run(); err != nil {
		return nil, err
	}

	fstat, err := targetFile.Stat()
	if err != nil {
		return nil, err
	}

	sfm := SimpleFileMeta{CessFileId: cessFileId, OriginName: shim.FirstFilename(fmeta)}
	t.storeSimpleFileMeta(&sfm)

	return &FileBriefInfo{
		OriginName: sfm.OriginName,
		CessFileId: cessFileId,
		FilePath:   targetFile.Name(),
		Size:       fstat.Size(),
	}, nil
}

type FragmentInfo struct {
	index              int
	hash               hash.H256
	belongSegmentIndex int
}

type fragmentGot struct {
	flagMap  map[int]string //key: fragment index, value: fragment filepath
	gotCount int
}

type Downloader struct {
	cessc          cessdk.SDK
	cessfsc        *cesssc.CessStorageClient
	log            logr.Logger
	cessFileId     string
	expectFileSize uint64
	downloadDir    string
	peerFragsMap   map[peer.ID][]FragmentInfo
	segFragGotMap  map[int]*fragmentGot //key: segment index
	outputFile     *os.File
}

func (t *Downloader) PeerSet() []peer.ID {
	peers := make([]peer.ID, 0, len(t.peerFragsMap))
	for k := range t.peerFragsMap {
		peers = append(peers, k)
	}
	return peers
}

func newDownloader(
	cessFileId string,
	fmeta *cesspat.FileMetadata,
	downloadDir string, targetFile *os.File,
	log logr.Logger,
	cessc cessdk.SDK,
	cessfsc *cesssc.CessStorageClient) (*Downloader, error) {

	log = log.WithValues("cessFileId", cessFileId)
	cachedMinerMap := make(map[types.AccountID]cesspat.MinerInfo)
	peerFragsMap := make(map[peer.ID][]FragmentInfo)
	segFragGotMap := make(map[int]*fragmentGot)
	for i, seg := range fmeta.SegmentList {
		for j, frag := range seg.FragmentList {
			m, ok := cachedMinerMap[frag.Miner]
			if !ok {
				var err error
				m, err = cessc.QueryStorageMiner(frag.Miner[:])
				if err != nil {
					return nil, errors.Wrap(err, "cessc.QueryStorageMiner()")
				}
				cachedMinerMap[frag.Miner] = m
			}
			peerId, err := peer.IDFromBytes(([]byte)(string(m.PeerId[:])))
			if err != nil {
				return nil, errors.Wrap(err, "peer.IDFromBytes()")
			}
			fragHash, err := toH256Type(frag.Hash)
			if err != nil {
				return nil, errors.Wrap(err, "toH256Type()")
			}
			fragInfos, ok := peerFragsMap[peerId]
			if !ok {
				fragInfos = make([]FragmentInfo, 0, 10)
			}
			f := FragmentInfo{j, *fragHash, i}
			fragInfos = append(fragInfos, f)
			peerFragsMap[peerId] = fragInfos
		}
		segFragGotMap[i] = &fragmentGot{
			flagMap:  make(map[int]string),
			gotCount: 0,
		}
	}
	return &Downloader{
		cessc:          cessc,
		cessfsc:        cessfsc,
		log:            log,
		cessFileId:     cessFileId,
		downloadDir:    downloadDir,
		peerFragsMap:   peerFragsMap,
		segFragGotMap:  segFragGotMap,
		expectFileSize: fmeta.FileSize.Uint64(),
		outputFile:     targetFile,
	}, nil
}

func (t *Downloader) Run() error {
	var start = time.Now()
	var wg sync.WaitGroup
	wg.Add(len(t.peerFragsMap))
	downloadErrs := make(map[peer.ID]error)
	for peerId, fis := range t.peerFragsMap {
		go func(wg *sync.WaitGroup, peerId peer.ID, fis []FragmentInfo) {
			defer wg.Done()
			fragInfos := make([]FragmentInfo, len(fis))
			copy(fragInfos, fis)
			if err := t.downloadFragsByPeer(peerId, fragInfos); err != nil {
				downloadErrs[peerId] = err
			}
		}(&wg, peerId, fis)
	}
	wg.Wait()
	if len(downloadErrs) > 0 {
		err := errors.New("download error")
		t.log.Error(err, "downloadErrors", downloadErrs)
		return err
	}

	t.log.V(1).Info("begin recover segments")
	segFilepaths, err := t.recoverSegments()
	if err != nil {
		return err
	}

	t.log.V(1).Info("begin merge segments")
	if err := t.merge(segFilepaths); err != nil {
		return err
	}

	t.log.V(1).Info("download cost(ms)", "cost", time.Since(start).Milliseconds())
	return nil
}

func (t *Downloader) downloadFragsByPeer(peerId peer.ID, fragInfos []FragmentInfo) error {
	log := t.log.WithValues("peerId", peerId.String())
	cessfsc := t.cessfsc
	rounds := 0
	for {
		addrInfo, e := cessfsc.DHTFindPeer(peerId.String())
		if e != nil {
			log.Error(e, "finding address error")
			rounds++
			log.V(1).Info("retry finding peers address 5 seconds later", "innerRounds", rounds)
			time.Sleep(5 * time.Second)
			continue
		}
		if err := cessfsc.Connect(cessfsc.GetCtxQueryFromCtxCancel(), addrInfo); err != nil {
			log.Error(err, "connect peer error")
		}
		log.V(1).Info("peer connected ", "address", addrInfo)
		break
	}

	n := len(fragInfos)
	log.V(1).Info("begin download frags by peer", "p", fmt.Sprintf("%d/%d", 0, n))

	totalDones := 0
	for {
		for i, frag := range fragInfos {
			fgEntry, ok := t.segFragGotMap[frag.belongSegmentIndex]
			if !ok {
				panic(fmt.Sprintf("not found segFragGot entry of segment %d", frag.belongSegmentIndex))
			}
			if fgEntry.gotCount >= cesspat.DataShards {
				continue
			}

			fragFilepath := filepath.Join(t.downloadDir, fmt.Sprintf("seg%d-frag%d", frag.belongSegmentIndex, frag.index))
			err := cessfsc.ReadFileAction(peerId, t.cessFileId, frag.hash.Hex(), fragFilepath, cesspat.FragmentSize)
			if err != nil {
				log.Error(err, "cessfsc.ReadFileAction()", "p", fmt.Sprintf("%d/%d", 0, n))
				continue
			}
			fgEntry.flagMap[frag.index] = fragFilepath
			fgEntry.gotCount++
			totalDones++

			log.V(1).Info("frag downloaded", "fragFilepath", fragFilepath, "p", fmt.Sprintf("%d/%d", totalDones, n))
			fragInfos = slices.Delete(fragInfos, i, i+1)
			break
		}
		if len(fragInfos) == 0 {
			break
		}
	}

	return nil
}

func (t *Downloader) recoverSegments() ([]string, error) {
	for i, sfg := range t.segFragGotMap {
		if sfg.gotCount < cesspat.DataShards {
			return nil, errors.Errorf("no satisfied fragment count of segment %d ", i)
		}
	}
	segFilepaths := make([]string, 0, len(t.segFragGotMap))
	for i, sfg := range t.segFragGotMap {
		fragFilepaths := make([]string, 0, 3)
		for _, path := range sfg.flagMap {
			if len(path) > 0 {
				fragFilepaths = append(fragFilepaths, path)
			}
		}
		segFilepath := filepath.Join(t.downloadDir, fmt.Sprintf("seg%d", i))
		err := t.recoverSegment(fragFilepaths, segFilepath)
		if err != nil {
			return nil, errors.Wrap(err, "recoverSegment()")
		}
		segFilepaths = append(segFilepaths, segFilepath)
	}
	return segFilepaths, nil
}

func (t *Downloader) merge(segFilepaths []string) error {
	segLen := len(segFilepaths)
	segReaders := make([]io.Reader, 0, segLen)
	for i, path := range segFilepaths {
		segmFile, err := os.Open(path)
		if err != nil {
			return err
		}
		defer segmFile.Close()
		if i+1 == segLen {
			// handle the last segment
			segReaders = append(
				segReaders,
				io.LimitReader(segmFile, int64(t.expectFileSize-uint64(segLen*cesspat.SegmentSize))),
			)
		} else {
			segReaders = append(segReaders, segmFile)
		}
	}
	reader := io.MultiReader(segReaders...)
	n, err := io.Copy(t.outputFile, reader)
	if err != nil {
		return err
	}
	if n != int64(t.expectFileSize) {
		return fmt.Errorf("unexpect file size")
	}
	return nil
}

func (t *Downloader) recoverSegment(fragFiles []string, outputPath string) error {
	fragsDir := filepath.Dir(fragFiles[0])
	builder, err := erasure.RsecDecodeByFilePath(outputPath, cesspat.SegmentSize)
	builder.WithShards(cesspat.DataShards, cesspat.ParShards)
	builder.WithShardCreater(
		func(index int) (io.WriteCloser, error) {
			var ff string
			if index >= len(fragFiles) {
				ff = filepath.Join(fragsDir, fmt.Sprintf("frag%d", index))
			} else {
				ff = fragFiles[index]
			}
			f, err := os.Create(ff)
			if err != nil {
				return nil, err
			}
			return f, nil
		})
	builder.WithShardOpener(
		func(index int) (io.ReadCloser, int64, error) {
			fstat, err := os.Stat(fragFiles[index])
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return nil, 0, nil
				} else {
					return nil, 0, err
				}
			}
			f, err := os.Open(fstat.Name())
			if err != nil {
				return nil, 0, err
			}
			return f, fstat.Size(), nil
		})
	rsecd, err := builder.Build()
	if err != nil {
		return err
	}
	return rsecd.Decode()
}
