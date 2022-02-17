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

type Selected struct {
	Selected string `yaml:"selected"`
}

func NewConf() {
	path := "config/"

	s := &Selected{}
	yamlFile, err := ioutil.ReadFile(path + "selected.yaml")
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, s)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}
	c := generateConf(path + "config" + s.Selected + ".yaml")

	Config = c
}

func generateConf(path string) Conf {

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

var Config Conf
