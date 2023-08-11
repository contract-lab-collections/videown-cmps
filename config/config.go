package config

import (
	"fmt"

	"os"
	"path/filepath"
	"strings"

	. "cmps/log"

	"github.com/spf13/viper"
)

type AppSetting struct {
	ListenerAddress  string
	WorkDir          string
	UsePreferByOrder bool
}

type CessSetting struct {
	RpcUrl       string
	SecretPhrase string
}

type CessfscSetting struct {
	BootAddrs []string
	P2pPort   uint16
}

type AppConfig struct {
	App     *AppSetting
	Cess    *CessSetting
	Cessfsc *CessfscSetting
}

const (
	_CONFIG_FILENAME = "config.yaml"
)

func getConfigFilePathByEnv(configDir string) (string, error) {
	configFilePaths := make([]string, 0, 2)
	appEnv := os.Getenv("APP_ENV")
	if appEnv != "" {
		configFilePaths = append(configFilePaths, filepath.Join(configDir, "conf-"+strings.ToLower(appEnv)+".yaml"))
	}
	configFilePaths = append(configFilePaths, filepath.Join(configDir, _CONFIG_FILENAME))
	for _, cfp := range configFilePaths {
		_, err := os.Stat(cfp)
		if err == nil {
			return cfp, nil
		}
	}
	return "", fmt.Errorf("don't exist any config file: %s", strings.Join(configFilePaths, ", "))
}

func parseConfigNameAndType(configFilePath string) (n string, t string) {
	fn := filepath.Base(configFilePath)
	ext := filepath.Ext(fn)
	return fn[:len(fn)-len(ext)], strings.TrimPrefix(ext, ".")
}

func loadConfigFile(configDir string) (*viper.Viper, error) {
	var configName, configType string
	{
		configFilePath, err := getConfigFilePathByEnv(configDir)
		if err != nil {
			return nil, err
		}
		Logger.Info("use config", "configFile", configFilePath)
		configName, configType = parseConfigNameAndType(configFilePath)
	}

	vp := viper.New()
	vp.SetConfigName(configName)
	vp.AddConfigPath(configDir)
	vp.SetConfigType(configType)
	err := vp.ReadInConfig()
	if err != nil {
		return nil, err
	}
	return vp, nil
}

func NewConfig(configDir string) (*AppConfig, error) {
	if configDir == "" {
		configDir = "config/"
	}
	vp, err := loadConfigFile(configDir)
	if err != nil {
		return nil, err
	}
	pCfg := new(AppConfig)
	if err := vp.UnmarshalKey("App", &pCfg.App); err != nil {
		return nil, err
	}
	if err := vp.UnmarshalKey("Cess", &pCfg.Cess); err != nil {
		return nil, err
	}
	if err := vp.UnmarshalKey("Cessfsc", &pCfg.Cessfsc); err != nil {
		return nil, err
	}

	return pCfg, nil
}
