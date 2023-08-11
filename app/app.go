package app

import (
	"cmps/config"
	. "cmps/log"
	"cmps/pkg/cesstash"
	"cmps/pkg/cesstash/shim/cesssc"
	"context"

	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/centrifuge/go-substrate-rpc-client/v4/signature"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"github.com/urfave/cli"

	cgs "github.com/CESSProject/cess-go-sdk"
	cessdk "github.com/CESSProject/cess-go-sdk/core/sdk"
)

type CmpsApp struct {
	gin         *gin.Engine
	config      *config.AppConfig
	keyringPair *signature.KeyringPair
	cessc       cessdk.SDK
	cesscsc     *cesssc.CessStorageClient
	cessstash   *cesstash.CessStash
}

func buildCesscc(cfg *config.CessSetting) (cessdk.SDK, error) {
	return cgs.New(
		context.Background(),
		"client",
		cgs.ConnectRpcAddrs([]string{cfg.RpcUrl}),
		cgs.Mnemonic(cfg.SecretPhrase),
		cgs.TransactionTimeout(time.Second*10),
	)
}

func buildCesssc(cfg *config.CessfscSetting, workDir string) (*cesssc.CessStorageClient, error) {
	return cesssc.New(cfg.P2pPort, workDir, cfg.BootAddrs, Logger.WithName("cstorec"))
}

func storeDir(parentDir string) string {
	return filepath.Join(parentDir, "db")
}

func setupGin(app *CmpsApp) error {
	gin.SetMode(gin.ReleaseMode)
	app.gin = gin.Default()
	app.gin.Use(cors.Default())
	addRoute(app)
	return nil
}

func addRoute(app *CmpsApp) {
	g := app.gin
	g.POST("/bucket", app.CreateBucket)
	g.GET("/file-state/:fileHash", app.GetFileState)
	g.PUT("/file", app.UploadFile)
	g.GET("/file/:fileHash", app.DownloadFile)
	g.DELETE("/file", app.DeleteFile)
	g.GET("/upload-progress/:uploadId", app.ListenerUploadProgress)
	g.GET("/upload-progress1", app.DebugUploadProgress)

	g.StaticFile("favicon.ico", "./static/favicon.ico")
	g.LoadHTMLFiles("./static/index.html")
	g.GET("/demo", func(c *gin.Context) {
		c.HTML(200, "index.html", nil)
	})
}

func buildCmpsApp(config *config.AppConfig) (*CmpsApp, error) {
	kp, err := signature.KeyringPairFromSecret(config.Cess.SecretPhrase, 0)
	if err != nil {
		return nil, err
	}

	workDir := config.App.WorkDir
	if _, err := os.Stat(workDir); os.IsNotExist(err) {
		err = os.Mkdir(workDir, 0755)
		if err != nil {
			return nil, errors.Wrap(err, "make CMPS work dir error")
		}
	}

	c, err := buildCesscc(config.Cess)
	if err != nil {
		return nil, errors.Wrap(err, "build cess chain client error")
	}
	sc, err := buildCesssc(config.Cessfsc, workDir)
	if err != nil {
		return nil, errors.Wrap(err, "build cess storage client error")
	}
	fs, err := cesstash.NewFileStash(workDir, config.Cess.SecretPhrase, c, sc)
	if err != nil {
		return nil, errors.Wrap(err, "build filestash error")
	}
	fs.SetStashWhenUpload(true)

	app := &CmpsApp{
		config:      config,
		keyringPair: &kp,
		cessc:       c,
		cessstash:   fs,
	}

	setupGin(app)
	return app, nil
}

var (
	App *CmpsApp
)

func Run(c *cli.Context) {
	config, err := config.NewConfig(c.String("config"))
	if err != nil {
		panic(err)
	}
	a, err := buildCmpsApp(config)
	if err != nil {
		panic(err)
	}

	App = a
	addr := config.App.ListenerAddress
	Logger.Info("http server listener on", "address", addr)
	go App.gin.Run(addr)

	signalHandle()
}

func signalHandle() {
	Logger.Info("server startup success!")
	var ch = make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		si := <-ch
		switch si {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			Logger.Info("stop the server process", "signal", si.String())

			Logger.Info("server shutdown success!")
			return
		case syscall.SIGHUP:
		default:
			return
		}
	}
}
