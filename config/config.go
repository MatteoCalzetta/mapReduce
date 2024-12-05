package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

type Worker struct {
	ID      int
	Address string
}

var Workers []Worker

func loadWorkers() error {
	file, err := ioutil.ReadFile("../config/config.json")
	if err != nil {
		fmt.Println("Error opening config file ", err)
		return err
	}
	err = json.Unmarshal(file, &Workers)
	if err != nil {
		fmt.Println("Error unmarshaling ", err)
		return err
	}

	return nil
}

func GetWorkers() ([]Worker, error) {
	err := loadWorkers()
	if err != nil {
		fmt.Println("Oops, something went wrong ", err)
		os.Exit(1)
	}
	return Workers, err
}
