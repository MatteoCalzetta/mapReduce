package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"mapReduce/config"
	"net"
	"os"
)

func handleConnetion(conn net.Conn) {
	defer conn.Close()

	var buf [4]byte // Buffer per leggere la lunghezza (int32), so che sono 4 byte

	_, err := conn.Read(buf[:])
	if err != nil {
		fmt.Println("Errore durante la lettura della lunghezza:", err)
		return
	}

	var length int32
	err = binary.Read(bytes.NewReader(buf[:]), binary.LittleEndian, &length) // Leggi la lunghezza dei dati

	if err != nil {
		fmt.Println("Errore durante la lettura della lunghezza:", err)
		return
	}
	fmt.Println("Lunghezza dei dati ricevuti:", length)

	data := make([]int32, length) // Leggi i dati (i numeri inviati dal client)
	for i := int32(0); i < length; i++ {
		// Leggi ogni singolo intero (4 byte)
		err := binary.Read(conn, binary.LittleEndian, &data[i])
		if err != nil {
			fmt.Println("Errore durante la lettura dei dati:", err)
			return
		}
	}

	fmt.Println("Data from client: ", data) // Stampa i dati ricevuti

	workers, err := config.GetWorkers()
	if err != nil {
		fmt.Println("Error during workers loadup: ", err)
	}

	fmt.Println("Workers number: ", len(workers))

}

func main() {
	masterAddress := "127.0.0.1:8080"

	listener, err := net.Listen("tcp", masterAddress)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	defer listener.Close()
	fmt.Println("Listening on " + masterAddress)

	for { //lasciare in ascolto server per sempre
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		go handleConnetion(conn)
	}

}
