package config

import (
	"fmt"
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v2"
)

type Source struct {
	Name     string
	Database string
	Schema   string
	Tables   []string
}

type Sources struct {
	Sources []Source
}

func ReadSources(path string) Sources {
	sourcesFile, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatalf("Could not read the config file")
	}
	sources := Sources{}
	err = yaml.Unmarshal(sourcesFile, &sources)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	fmt.Printf("--- config:\n%v\n\n", sources)
	return sources
}
