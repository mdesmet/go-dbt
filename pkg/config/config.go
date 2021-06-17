package config

import (
	"fmt"
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Name    string
	Profile string
	Models  map[string]interface{} // TODO: this should definitely be improved
}

func ReadConfig() Config {
	configFile, err := ioutil.ReadFile("dbt_project.yml")
	if err != nil {
		log.Fatalf("Could not read the config file")
	}
	config := Config{}
	err = yaml.Unmarshal(configFile, &config)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	fmt.Printf("--- config:\n%v\n\n", config)
	return config
}
