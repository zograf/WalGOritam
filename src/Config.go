package src

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

type Conf struct {
	WalThreshold      int    `yaml:"walThreshold"`
	WalEntriesPerFile int    `yaml:"walEntriesPerFile"`
	WalLowWatermark   int    `yaml:"walLowWatermark"`
	MemtableThreshold uint16 `yaml:"memtableThreshold"`
	CacheSize         int    `yaml:"cacheSize"`
}

func NewConf(path string) Conf {

	c := &Conf{}
	yamlFile, err := ioutil.ReadFile(path)
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}

	return *c
}

var Config Conf = NewConf("config/config00.yaml")
