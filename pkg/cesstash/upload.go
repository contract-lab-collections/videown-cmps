package cesstash

import (
	"cmps/pkg/utils/hash"
	"encoding/json"
	"fmt"
	"io"
	"runtime/debug"
	"sync"

	"os"
	"path/filepath"
	"time"

	"cmps/pkg/cesstash/shim/segment"

	cesspat "github.com/CESSProject/cess-go-sdk/core/pattern"
	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/centrifuge/go-substrate-rpc-client/v4/types"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	_FINISH_STEP = "finish"
	_ABORT_STEP  = "abort"
)

type HandleStep struct {
	Step  string
	Msg   string
	Data  any
	Error error
}

func (t HandleStep) IsComplete() bool { return t.Step == _FINISH_STEP }
func (t HandleStep) IsAbort() bool    { return t.Error != nil }
func (t HandleStep) String() string {
	b, err := json.Marshal(t)
	if err != nil {
		return fmt.Sprintf("step:%s <json marshal error:%v>", t.Step, err)
	}
	return fmt.Sprint("progress", string(b))
}
func (t HandleStep) MarshalJSON() ([]byte, error) {
	type tmpType struct {
		Step  string `json:"step,omitempty"`
		Msg   string `json:"msg,omitempty"`
		Data  any    `json:"data,omitempty"`
		Error string `json:"error,omitempty"`
	}
	tt := tmpType{
		Step: t.Step,
		Msg:  t.Msg,
		Data: t.Data,
	}
	if t.Error != nil {
		tt.Error = t.Error.Error()
	}
	return json.Marshal(&tt)
}

type StepEmiter interface {
	EmitStep(step HandleStep)
}

func calculateFileHash(fileHeader FileHeader) (*FileHash, error) {
	file, err := fileHeader.Open()
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return hash.Calculate(file)
}

func (t *CessStash) createFsm(file FileHeader, outputDir string) (*segment.FileSegmentMeta, error) {
	toUploadFile, err := file.Open()
	if err != nil {
		return nil, err
	}
	defer toUploadFile.Close()
	fsmHome, err := os.MkdirTemp(outputDir, "fsm_tmp_*")
	if err != nil {
		return nil, err
	}

	fsm, err := segment.CreateByStream(toUploadFile, file.Size(), fsmHome)
	if err != nil {
		return nil, errors.Wrap(err, "shard file error")
	}
	fsm.Name = file.Filename()

	normalizeDir := filepath.Join(outputDir, fmt.Sprintf("fsm-%s", fsm.RootHash.Hex()))
	fstat, _ := os.Stat(normalizeDir)
	// the same cessFileId file exist
	if fstat != nil {
		fsm.OutputDir = normalizeDir
		os.RemoveAll(fsmHome)
		return fsm, nil
	}

	if err := os.Rename(fsmHome, normalizeDir); err == nil {
		fsm.OutputDir = normalizeDir
	} else {
		t.log.Error(err, "rename fsm dir error")
	}

	if t.stashWhenUpload {
		_, err = toUploadFile.Seek(0, io.SeekStart)
		if err != nil {
			return nil, errors.Wrap(err, "seek file error when stash")
		}
		err = t.stashFile(fsm.RootHash.Hex(), toUploadFile, file.Filename())
		if err != nil {
			return nil, errors.Wrap(err, "stash file error")
		}
	}
	return fsm, nil
}

func (t *CessStash) Upload(fileHeader FileHeader, accountId types.AccountID, bucketName string, forceUploadIfPending bool) (*RelayHandler, error) {
	fsm, err := t.createFsm(fileHeader, t.chunksDir)
	if err != nil {
		return nil, err
	}
	cessFileId := *fsm.RootHash
	rh := t.relayHandlers[cessFileId]
	if rh == nil {
		rh = &RelayHandler{
			log:                  t.log.WithValues("cessFileId", cessFileId),
			id:                   cessFileId,
			accountId:            accountId,
			bucketName:           bucketName,
			fileStash:            t,
			forceUploadIfPending: forceUploadIfPending,
			fsm:                  fsm,
			state:                RelayState{FileHash: cessFileId.Hex()},
		}
		t.log.V(1).Info("allot relay handler")
		t.relayHandlers[cessFileId] = rh
		go func() {
			t.relayHandlerPutChan <- rh
		}()
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				err := r.(error)
				fmt.Printf("%+v\n", err)
				t.log.Error(err, "catch panic on relay process!", "stack", string(debug.Stack()))
			}
		}()
		rh.relay()
	}()

	return rh, nil
}

func (t *CessStash) GetRelayHandler(fileHash FileHash) (*RelayHandler, error) {
	rh, ok := t.relayHandlers[fileHash]
	if !ok {
		return nil, errors.New("relay handler not exists for upload")
	}
	return rh, nil
}

func (t *CessStash) AnyRelayHandler() <-chan *RelayHandler {
	return t.relayHandlerPutChan
}

func cleanChunks(chunkDir string) {
	err := os.RemoveAll(chunkDir)
	if err != nil {
		Logger.Error(err, "remove chunk dir error")
	}
}

type RelayState struct {
	FileHash          string       `json:"fileHash,omitempty"`
	Miners            []string     `json:"miners,omitempty"`
	Steps             []HandleStep `json:"steps,omitempty"`
	CompleteTime      time.Time    `json:"completeTime,omitempty"`
	Aborted           bool         `json:"aborted,omitempty"`
	retryRounds       int
	storedButTxFailed bool
}

func (t *RelayState) pushStep(step HandleStep) {
	t.Steps = append(t.Steps, step)
	if step.Step == _FINISH_STEP || step.Step == _ABORT_STEP {
		t.CompleteTime = time.Now()
		if step.Step == _ABORT_STEP {
			t.Aborted = true
		} else {
			t.Aborted = false
		}
	}
}

func (t *RelayState) IsProcessing() bool {
	return t.CompleteTime.IsZero()
}

func (t *RelayState) IsComplete() bool {
	return !t.IsProcessing()
}

func (t *RelayState) IsCompleteBefore(ref time.Time, d time.Duration) bool {
	return t.IsComplete() && ref.After(t.CompleteTime.Add(d))
}

func (t *RelayState) IsAbort() bool {
	return t.IsComplete() && t.Aborted
}

type RelayHandler struct {
	id         FileHash
	accountId  types.AccountID
	bucketName string
	fileStash  *CessStash
	state      RelayState
	stateMutex sync.RWMutex
	stepChan   chan HandleStep
	log        logr.Logger

	forceUploadIfPending bool
	fsm                  *segment.FileSegmentMeta
	minersReassignTimes  int
}

func (t *RelayHandler) Id() FileHash { return t.id }

func (t *RelayHandler) ListenerProgress() <-chan HandleStep {
	if t.stepChan == nil {
		t.stepChan = make(chan HandleStep)
	}
	return t.stepChan
}

func (t *RelayHandler) State() *RelayState {
	t.stateMutex.RLock()
	defer t.stateMutex.RUnlock()
	return &t.state
}

func (t *RelayHandler) IsProcessing() bool {
	return t.State().IsProcessing()
}

func (t *RelayHandler) CanClean() bool {
	t.stateMutex.RLock()
	defer t.stateMutex.RUnlock()
	s := t.state
	if s.IsProcessing() {
		return false
	}
	if s.IsAbort() {
		return s.IsCompleteBefore(time.Now(), 5*time.Second)
	} else {
		return s.IsCompleteBefore(time.Now(), 300*time.Second)
	}
}

func (t *RelayHandler) EmitStep(step HandleStep) {
	t.stateMutex.Lock()
	defer t.stateMutex.Unlock()
	t.state.pushStep(step)
	if t.log.V(1).Enabled() {
		log := t.log.V(1).WithCallDepth(2)
		kvs := make([]any, 0, 6)
		kvs = append(kvs, "step", step.Step)
		if step.Data != nil {
			kvs = append(kvs, "data", step.Data)
		}
		if len(step.Msg) > 0 {
			kvs = append(kvs, "msg", step.Msg)
		}
		if step.IsAbort() {
			log.Error(step.Error, "relay abort", kvs...)
		} else {
			log.Info("relay progress", kvs...)
		}
	}
	if t.stepChan != nil {
		t.stepChan <- step
	}
}

func EmitStep(emiter StepEmiter, step string, args ...any) {
	p := HandleStep{Step: step}
	if len(args) > 0 {
		if err, ok := args[0].(error); ok {
			p.Error = err
		} else if msg, ok := args[0].(string); ok {
			p.Msg = msg
		} else {
			p.Data = args[0]
		}
	}
	emiter.EmitStep(p)
}

func (t *RelayHandler) close() {
	if t.stepChan != nil {
		close(t.stepChan)
	}
}

type ShardedStep struct {
	CessFileId string `json:"cessFileId"`
}

func (t *RelayHandler) ReRelayIfAbort() bool {
	s := t.State()
	if !s.IsAbort() || s.storedButTxFailed {
		return false
	}
	//TODO: no-brain retry now, to be fix it!
	t.log.Info("new round relay()", "prevRetryRounds", s.retryRounds, "prevCompleteTime", s.CompleteTime)

	t.stateMutex.Lock()
	s.retryRounds++
	s.CompleteTime = time.Time{}
	t.stateMutex.Unlock()

	t.relay()
	return true
}

func (t *RelayHandler) relay() (retErr error) {
	defer func() {
		if retErr != nil {
			EmitStep(t, _ABORT_STEP, retErr)
		} else {
			EmitStep(t, _FINISH_STEP)
			cleanChunks(t.fsm.OutputDir)
		}
	}()

	EmitStep(t, "bucketing")
	if _, err := t.createBucketIfAbsent(); err != nil {
		return errors.Wrap(err, "create bucket error")
	}

	EmitStep(t, "uploading")
	t.log.V(1).Info("the fsm", "fsm", t.fsm)
	if err := t.upload(t.fsm); err != nil {
		return err
	}

	return nil
}

func (t *RelayHandler) createBucketIfAbsent() (string, error) {
	_, err := t.fileStash.cessc.QueryBucketInfo(t.accountId[:], t.bucketName)
	if err != nil {
		t.log.Error(err, "get bucket info error")
		txHash, err := t.fileStash.cessc.CreateBucket(t.accountId[:], t.bucketName)
		if err != nil {
			t.log.Error(err, "create bucket failed", "bucketName", t.bucketName)
		}
		t.log.Info("create bucket", "txn", txHash)
		return txHash, err
	}
	return "", nil
}

type declareFileResult struct {
	declareTxHash string
	fileMetaInfo  *cesspat.FileMetadata
}

func (t declareFileResult) IsDeclareApplied() bool { return len(t.declareTxHash) > 0 }

func (t declareFileResult) IsOriginFilePending() bool {
	return t.fileMetaInfo != nil && int(t.fileMetaInfo.State) != cesspat.Active
}

func (t declareFileResult) IsOriginFileActive() bool {
	return t.fileMetaInfo != nil && int(t.fileMetaInfo.State) == cesspat.Active
}

func (t declareFileResult) FileState() string {
	if t.fileMetaInfo == nil {
		return ""
	}
	return string(t.fileMetaInfo.State)
}

func makeMinersForProgress(miners []string) map[string][]string {
	return map[string][]string{"miners": miners}
}

func (t *RelayHandler) declaration(fsm *segment.FileSegmentMeta, actualDecl bool) (string, error) {
	user := cesspat.UserBrief{
		User:       t.accountId,
		BucketName: types.NewBytes([]byte(t.bucketName)),
		FileName:   types.NewBytes([]byte(fsm.Name)),
	}
	var dstSegs []cesspat.SegmentList
	if actualDecl {
		dstSegs = make([]cesspat.SegmentList, len(fsm.Segments))
		for i := 0; i < len(dstSegs); i++ {
			srcSeg := &fsm.Segments[i]
			dstSeg := &dstSegs[i]
			dstSeg.SegmentHash = toCessFileHashType(srcSeg.Hash)
			for j := 0; j < len(srcSeg.Frags); j++ {
				dstSeg.FragmentHash = append(dstSeg.FragmentHash, toCessFileHashType(srcSeg.Frags[j].Hash))
			}
		}
		EmitStep(t, "upload declaring")
	} else {
		EmitStep(t, "upload declaring: only record the relationship")
	}
	return t.fileStash.cessc.UploadDeclaration(fsm.RootHash.Hex(), dstSegs, user, uint64(fsm.InputSize))
}

func toCessFileHashType(hash *hash.H256) cesspat.FileHash {
	//FIXME: the chain define FileHash type use 64 bytes hash hex
	cessFileHash := cesspat.FileHash{}
	n := len(cessFileHash)
	hex := hash.Hex()
	for i := 0; i < n; i++ {
		cessFileHash[i] = types.U8(hex[i])
	}
	return cessFileHash
}

func toH256Type(fh cesspat.FileHash) (*hash.H256, error) {
	bytes := make([]byte, len(fh))
	for i, b := range fh {
		bytes[i] = byte(b)
	}
	return hash.ValueOf(string(bytes))
}

type LoopCtrlAction uint8

const (
	NONE LoopCtrlAction = iota
	RESTART
	FINISH
)

func (t *RelayHandler) upload(fsm *segment.FileSegmentMeta) error {
	if len(fsm.Segments) == 0 {
		return errors.New("the fsm fragments empty")
	}

	cessFileId := fsm.RootHash.Hex()
	cessc := t.fileStash.cessc
	log := t.log

LOOP_START:
	for {
		fmd, err := cessc.QueryFileMetadata(cessFileId)
		log.V(1).Info("got FileMetaData", "minersReassignTimes", t.minersReassignTimes, "fmd", fmd, "err", err)
		if err == nil {
			// the same cessFileId file is already on chain
			for _, v := range fmd.Owner {
				if t.accountId == v.User {
					// the user has already upload the file before
					EmitStep(t, "thunder", map[string]any{"alreadyUploaded": true})
					return nil
				}
			}
			// only record the relationship
			txn, err := t.declaration(fsm, false)
			if err != nil {
				return errors.Wrapf(err, "cessc.UploadDeclaration()")
			}
			EmitStep(t, "thunder", map[string]any{"txn": txn})
			return nil
		}

		storageOrder, err := cessc.QueryStorageOrder(cessFileId)
		if err != nil {
			if err.Error() == cesspat.ERR_Empty {
				txn, err := t.declaration(fsm, true)
				if err != nil {
					return errors.Wrapf(err, "cessc.UploadDeclaration()")
				}
				EmitStep(t, "upload declared", map[string]any{"txn": txn})
			}
			// } else if storageOrder.User.User == t.account {
			// 	EmitStep(t.se, "thunder")
			// 	return nil
		}
		t.minersReassignTimes = int(storageOrder.Count)
		t.log.V(1).Info("storage order miner reassigns", "minersReassignTimes", t.minersReassignTimes)

		tfi, err := t.extractToTargetFragInfo(&storageOrder)
		if err != nil {
			return err
		}

		if t.findingTargetPeers(cessFileId, tfi) == RESTART {
			goto LOOP_START
		}

		r := t.uploadFragments(cessFileId, fsm, tfi)
		if r == FINISH {
			goto STATE_POLL
		} else if r == RESTART {
			goto LOOP_START
		}
	}
STATE_POLL:
	return t.pollStorageState(cessFileId)
}

func (t *RelayHandler) isMinersReassigned(cessFileId string) bool {
	storageOrder, err := t.fileStash.cessc.QueryStorageOrder(cessFileId)
	if err != nil {
		return false
	}
	if int(storageOrder.Count) != t.minersReassignTimes {
		return true
	}
	return false
}

func (t *RelayHandler) findingTargetPeers(cessFileId string, tfi *TargetFragInfo) LoopCtrlAction {
	peerSet := make(PeerSet)
	maps.Copy(peerSet, tfi.peerSet)
	log := t.log
	totalPeers := len(peerSet)
	addressFoundPeers := 0
	rounds := 0
	log.V(1).Info("begin finding peers address", "targetPeers", totalPeers)
	cessfsc := t.fileStash.cessfsc
	for {
		for p := range peerSet {
			addrInfo, e := cessfsc.DHTFindPeer(p.String())
			if e != nil {
				t.log.Error(e, "finding address error", "peer", p.String())
				rounds++
				log.V(1).Info("retry finding peers address 5 seconds later",
					"innerRounds", rounds,
					"progress", fmt.Sprintf("%d/%d", addressFoundPeers, totalPeers))
				time.Sleep(5 * time.Second)
				if rounds%30 == 0 {
					if t.isMinersReassigned(cessFileId) {
						log.V(1).Info("miner reassign occurced, restart the outer round")
						return RESTART
					}
				}
				continue
			}
			addressFoundPeers++
			t.log.V(1).Info("got address", "peer", p.String(), "address", addrInfo)
			delete(peerSet, p)
			if err := cessfsc.Connect(cessfsc.GetCtxQueryFromCtxCancel(), addrInfo); err != nil {
				t.log.Error(err, "connect peer error")
			}
			break
		}
		if len(peerSet) == 0 {
			return NONE
		}
	}
}

func (t *RelayHandler) uploadFragments(cessFileId string, fsm *segment.FileSegmentMeta, tfi *TargetFragInfo) LoopCtrlAction {
	frags := fsm.ExtractFrags()
	log := t.log
	totalFrags := len(frags)
	rounds := 0
	uploadedFrags := 0
	for {
		doneIndex, donePeerId := t.roundWrite(cessFileId, frags, tfi, log)
		if doneIndex >= 0 {
			uploadedFrags++
			log.V(1).Info("fragment uploaded", "frag", frags[doneIndex],
				"peerId", donePeerId,
				"progress", fmt.Sprintf("%d/%d", uploadedFrags, totalFrags))
			frags = slices.Delete(frags, doneIndex, doneIndex+1)
			if len(frags) == 0 {
				EmitStep(t, "uploaded", map[string]any{"segments": len(fsm.Segments), "frags": totalFrags})
				return FINISH
			}
		} else {
			rounds++
			log.V(1).Info("retry 10 seconds later", "innerRounds", rounds, "progress", fmt.Sprintf("%d/%d", uploadedFrags, totalFrags))
			time.Sleep(10 * time.Second)
			if rounds%30 == 0 {
				if t.isMinersReassigned(cessFileId) {
					log.V(1).Info("miner reassign occurced, restart the outer round")
					return RESTART
				}
			}
		}
	}
}

func (t *RelayHandler) roundWrite(cessFileId string, frags []segment.Fragment, tfi *TargetFragInfo, log logr.Logger) (int, *peer.ID) {
	for i, frag := range frags {
		p, ok := tfi.fragPeerMap[*frag.Hash]
		if !ok {
			log.Error(errors.Errorf("missing hash: %s coresponding peer", frag.Hash), "")
			continue
		}

		if err := t.fileStash.cessfsc.WriteFileAction(p.PeerId, cessFileId, frag.FilePath); err != nil {
			if err.Error() != "no addresses" {
				log.Error(err, "WriteFileAction() error, try again later", "frag", frag, "peerId", p.PeerId, "account", p.Account)
			} // only log none "no addresses" error
			continue
		}
		return i, &p.PeerId
	}
	return -1, nil
}

func (t *RelayHandler) pollStorageState(cessFileId string) error {
	const MAX_RETRY = 60
	const SLEEP_SECS = 5 * time.Second
	cessc := t.fileStash.cessc
	//TODO: to optimize
	EmitStep(t, "storing", "begin polling the file storage state on chain")
	startTs := time.Now()

	i := 0
	for {
		storageOrder, err := cessc.QueryStorageOrder(cessFileId)
		t.log.V(1).Info("query storage state", "stage", storageOrder.Stage, "count", storageOrder.Count, "err", err)
		if err != nil {
			if err.Error() != cesspat.ERR_Empty {
				t.log.Error(err, "query storage state error, retry later")
				time.Sleep(3 * time.Second)
				continue
			} else {
				t.log.Error(errors.New("unreachable"), "")
				break
			}
		}

		fmd, err := cessc.QueryFileMetadata(cessFileId)
		if err == nil {
			EmitStep(t, "stored")
			break
		}
		t.log.V(1).Info("got FileMetaData", "fmd", fmd, "err", err)

		if i >= MAX_RETRY {
			return errors.Errorf("retry %d times, duration: %s, try again next round", i, time.Since(startTs))
		}
		i++
		time.Sleep(SLEEP_SECS)
	}
	return nil
}

type PeerAccountPair struct {
	PeerId  peer.ID
	Account types.AccountID
}

type PeerSet = map[peer.ID]struct{}

type TargetFragInfo struct {
	fragPeerMap map[hash.H256]PeerAccountPair
	peerSet     PeerSet
}

func (t *RelayHandler) extractToTargetFragInfo(storageOrder *cesspat.StorageOrder) (*TargetFragInfo, error) {
	result := &TargetFragInfo{
		fragPeerMap: make(map[hash.H256]PeerAccountPair),
		peerSet:     make(map[peer.ID]struct{}),
	}
	for _, m := range storageOrder.AssignedMiner {
		r, err := t.fileStash.cessc.QueryStorageMiner(m.Account[:])
		if err != nil {
			return nil, errors.Wrap(err, "cessc.QueryStorageMiner()")
		}
		peerId, err := peer.IDFromBytes(([]byte)(string(r.PeerId[:])))
		if err != nil {
			return nil, errors.Wrap(err, "peer.Decode()")
		}
		for _, fh := range m.Hash {
			lh, err := toH256Type(fh)
			if err != nil {
				return nil, errors.Wrap(err, "toH256Type()")
			}
			result.fragPeerMap[*lh] = PeerAccountPair{peerId, m.Account}
			result.peerSet[peerId] = struct{}{}
			//t.log.V(1).Info("frag hash peer pair", "fragHash", lh, "peerId", peerId)
		}
	}
	if len(result.fragPeerMap) == 0 {
		return nil, errors.New("storage order AssignedMiner must not be empty")
	}
	return result, nil
}
