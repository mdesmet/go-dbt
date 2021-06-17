package config

import (
	"fmt"
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v2"
)

type Connection struct {
	Adapter                string `yaml:"type"`
	Threads                int
	Account                string
	User                   string
	Password               string
	PrivateKeyPath         string `yaml:"private_key_path"`
	PrivateKeyPassphrase   string `yaml:"private_key_passphrase"`
	Database               string
	Role                   string
	Warehouse              string
	Schema                 string
	ClientSessionKeepAlive bool `yaml:"client_session_keep_alive"`
}
type Connections struct {
	Outputs map[string]Connection
	Target  string
}

type Profiles map[string]Connections

func ReadProfiles() Profiles {
	configFile, err := ioutil.ReadFile("profiles.yml")
	if err != nil {
		log.Fatalf("Could not read the profiles file")
	}
	profiles := Profiles{}
	err = yaml.Unmarshal(configFile, &profiles)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	fmt.Printf("--- config:\n%v\n\n", profiles)
	return profiles
}
