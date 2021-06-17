package database

import (
	"crypto/rsa"
	"database/sql"
	"encoding/pem"
	"io/ioutil"
	"log"

	"github.com/mdesmet/go-dbt/pkg/config"
	"github.com/snowflakedb/gosnowflake"
	"github.com/youmark/pkcs8"
)

func Connect(profile *config.Connection) *sql.DB {
	dsn, err := dsn(profile)
	if err != nil {
		log.Fatal(err)
	}
	db, err := sql.Open("snowflake", dsn)
	log.Println(dsn)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func dsn(profile *config.Connection) (string, error) {
	var privateKey *rsa.PrivateKey
	if profile.PrivateKeyPath != "" {
		privateFileContent, err := ioutil.ReadFile(profile.PrivateKeyPath)
		if err != nil {
			log.Fatal(err)
		}
		der, _ := pem.Decode(privateFileContent)
		privateKey, err = pkcs8.ParsePKCS8PrivateKeyRSA(der.Bytes, []byte(profile.PrivateKeyPassphrase))
		if err != nil {
			log.Fatal(err)
		}
	}

	cfg := &gosnowflake.Config{
		Account:       profile.Account,
		Authenticator: gosnowflake.AuthTypeJwt,
		User:          profile.User,
		Role:          profile.Role,
		Database:      profile.Database,
		Warehouse:     profile.Warehouse,
		Schema:        profile.Schema,
		PrivateKey:    privateKey,
	}

	dsn, err := gosnowflake.DSN(cfg)
	return dsn, err
}
