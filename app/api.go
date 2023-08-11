package app

import (
	"cmps/app/resp"
	. "cmps/log"
	"cmps/pkg/cesstash"
	"cmps/pkg/utils/cessaddr"

	"cmps/pkg/utils/hash"
	"fmt"
	"mime/multipart"
	"net/http"

	cesspat "github.com/CESSProject/cess-go-sdk/core/pattern"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/pkg/errors"
)

const DEFAULT_BUCKET = "videown"

type (
	CessFileIdOnlyReq struct {
		CessFileId string `json:"fileHash" form:"fileHash" uri:"fileHash" binding:"required"`
	}

	BucketCreateReq struct {
		WalletAddress string `json:"walletAddress" form:"walletAddress" binding:"required"`
	}

	FilePutReq struct {
		WalletAddress        string                `form:"walletAddress" binding:"required"`
		File                 *multipart.FileHeader `form:"file" binding:"required"`
		ForceUploadIfPending bool                  `form:"force"`
	}

	FileDeleteReq struct {
		CessFileIdOnlyReq
		WalletAddress string `json:"walletAddress" form:"walletAddress" binding:"required"`
	}

	UploadStateReq struct {
		UploadId string `json:"uploadId" form:"uploadId" uri:"uploadId" binding:"required"`
	}
)

func (n CmpsApp) CreateBucket(c *gin.Context) {
	var req BucketCreateReq
	if err := c.ShouldBind(&req); err != nil {
		resp.Error(c, err)
		return
	}

	accountId, err := cessaddr.ToAccountIdByCessAddress(req.WalletAddress)
	if err != nil {
		resp.Error(c, err)
		return
	}

	txHash, err := n.cessc.CreateBucket((*accountId)[:], DEFAULT_BUCKET)
	if err != nil {
		resp.Error(c, err)
		return
	}
	resp.Ok(c, map[string]string{"txHash:": txHash})

}

func (n CmpsApp) UploadFile(c *gin.Context) {
	var req FilePutReq
	if err := c.ShouldBind(&req); err != nil {
		resp.Error(c, err)
		return
	}

	accountId, err := cessaddr.ToAccountIdByCessAddress(req.WalletAddress)
	if err != nil {
		resp.Error(c, err)
		return
	}

	fileh, err := cesstash.MultipartFile(req.File)
	if err != nil {
		resp.Error(c, err)
		return
	}

	rh, err := n.cessstash.Upload(fileh, *accountId, DEFAULT_BUCKET, req.ForceUploadIfPending)
	if err != nil {
		resp.Error(c, err)
		return
	}
	result := map[string]any{"uploadId": rh.Id()}
	resp.Ok(c, result)
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type LupMsgType int

const (
	LMT_STATE LupMsgType = 0 + iota
	LMT_PROGRESS
)

type LupMsg struct {
	Type LupMsgType `json:"type"`
	Msg  any        `json:"msg"`
}

func (n CmpsApp) ListenerUploadProgress(c *gin.Context) {
	Logger.Info("upload progress listener coming", "method", c.Request.Method, "url", c.Request.URL, "clientIp", c.ClientIP())
	var f UploadStateReq
	if err := c.ShouldBindUri(&f); err != nil {
		resp.Error(c, err)
		return
	}

	// TODO: to optimize to custom type bind
	fh, err := hash.ValueOf(f.UploadId)
	if err != nil {
		resp.Error(c, err)
		return
	}
	rh, err := n.cessstash.GetRelayHandler(*fh)
	if err != nil {
		resp.ErrorWithHttpStatus(c, err, 404)
		return
	}

	ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		Logger.Error(err, "websocket upgrade error")
		return
	}
	defer ws.Close()

	closeSignal := make(chan bool)
	ws.SetCloseHandler(func(code int, text string) error {
		Logger.V(1).Info("websocket close", "code", code, "text", text)
		closeSignal <- true
		return nil
	})
	pushMsgForRelayHandler(ws, rh, closeSignal)
	Logger.V(1).Info("upload progress listener finish", "uploadId", f.UploadId)
}

func pushMsgForRelayHandler(ws *websocket.Conn, rh *cesstash.RelayHandler, closeSignal <-chan bool) {
	err := ws.WriteJSON(LupMsg{LMT_STATE, rh.State()})
	if err != nil {
		Logger.Error(err, "write relay handler state to websocket error")
		return
	}
	for {
		select {
		case p := <-rh.ListenerProgress():
			err = ws.WriteJSON(LupMsg{LMT_PROGRESS, p})
			if err != nil {
				Logger.Error(err, "write relay handler progress to websocket error")
				return
			}
			if p.IsComplete() {
				return
			}
			if p.IsAbort() {
				Logger.Error(p.Error, "progress abort error")
				return
			}
		case <-closeSignal:
			Logger.V(1).Info("websocket close signal coming")
			return
		}
	}
}

func (n CmpsApp) DebugUploadProgress(c *gin.Context) {
	ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		Logger.Error(err, "websocket upgrade error")
		return
	}
	defer ws.Close()

	closeSignal := make(chan bool)
	wsClosed := false
	ws.SetCloseHandler(func(code int, text string) error {
		Logger.V(1).Info("websocket close", "code", code, "text", text)
		wsClosed = true
		closeSignal <- true
		return nil
	})
	ws.WriteMessage(websocket.TextMessage, []byte("waiting for file upload..."))
	for !wsClosed {
		rh := <-n.cessstash.AnyRelayHandler()
		pushMsgForRelayHandler(ws, rh, closeSignal)
	}
	Logger.V(1).Info("debug upload progress websocket listener finish")
}

func (n CmpsApp) GetFileState(c *gin.Context) {
	var req CessFileIdOnlyReq
	if err := c.ShouldBindUri(&req); err != nil {
		resp.Error(c, err)
		return
	}
	fmeta, err := n.cessc.QueryFileMetadata(req.CessFileId)
	if err != nil {
		if errors.Is(err, cesspat.ERR_RPC_EMPTY_VALUE) {
			resp.ErrorWithHttpStatus(c, err, 404)
		} else {
			resp.Error(c, err)
		}
		return
	}

	resp.Ok(c, fmeta)
}

func responseForFile(c *gin.Context, filepath, filename string) {
	c.Writer.Header().Add("Content-Disposition", fmt.Sprintf("attachment; filename=%v", filename))
	c.Writer.Header().Add("Content-Type", "application/octet-stream")
	c.File(filepath)
}

func (n CmpsApp) DownloadFile(c *gin.Context) {
	var req CessFileIdOnlyReq
	if err := c.ShouldBindUri(&req); err != nil {
		resp.Error(c, err)
		return
	}

	skipStash := c.Query("skipStash") != ""

	if !skipStash {
		fbi, err := n.cessstash.FileInfoById(req.CessFileId)
		if fbi != nil && fbi.Size > 0 {
			responseForFile(c, fbi.FilePath, fbi.OriginName)
			return
		} else {
			Logger.Error(err, "")
		}
	}

	fbi, err := n.cessstash.DownloadFile(req.CessFileId)
	if err != nil {
		Logger.Error(err, "filestash.DownloadFile()", "cessFileId", req.CessFileId)
		resp.Error(c, err)
		return
	}

	Logger.V(1).Info("download finished, begin response file", "filePath", fbi.FilePath)
	responseForFile(c, fbi.FilePath, fbi.OriginName)
}

func (n CmpsApp) DeleteFile(c *gin.Context) {
	var req FileDeleteReq
	if err := c.ShouldBind(&req); err != nil {
		resp.Error(c, err)
		return
	}
	accountId, err := cessaddr.ToAccountIdByCessAddress(req.WalletAddress)
	if err != nil {
		resp.Error(c, err)
		return
	}

	txHash, _, err := n.cessc.DeleteFile((*accountId)[:], []string{req.CessFileId})
	if err != nil {
		resp.Error(c, err)
		return
	}
	n.cessstash.RemoveFile(req.CessFileId)
	resp.Ok(c, txHash)
}
