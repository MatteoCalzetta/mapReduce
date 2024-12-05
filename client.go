package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"os"
	"time"
)

func generateRandomInput(size, min, max int) []int {
	rand.Seed(time.Now().UnixNano())
	input := make([]int, size)
	for i := 0; i < size; i++ {
		input[i] = rand.Intn(max - min + 1)
	}
	return input
}

func main() {
	size := 100
	min := 0
	max := 30
	masterAddress := "127.0.0.1:8080"

	dataToProcess := generateRandomInput(size, min, max) //dati del

	var buf bytes.Buffer //dati in bytes per comunicazione

	lenght := int32(len(dataToProcess))
	err := binary.Write(&buf, binary.LittleEndian, lenght)
	if err != nil {
		fmt.Println("Error writing data to buffer:", err)
		os.Exit(1)
	}

	// Scrivi i dati reali (i numeri casuali)
	for _, val := range dataToProcess {
		err := binary.Write(&buf, binary.LittleEndian, int32(val)) // Scrivi ogni intero come 4 byte
		if err != nil {
			fmt.Println("Error writing data to buffer:", err)
			os.Exit(1)
		}
	}

	conn, err := net.Dial("tcp", masterAddress) //init connessione col server
	if err != nil {
		fmt.Println("Error connecting to master:", err)
		os.Exit(1)
	}
	fmt.Println("Connected to master on " + conn.LocalAddr().String())
	defer conn.Close()

	_, err = conn.Write(buf.Bytes())
	if err != nil {
		fmt.Println("Error sending data to master:", err)
		os.Exit(1)
	}
	fmt.Println("data to master has been sent", masterAddress)

}
