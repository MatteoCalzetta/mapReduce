package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func handleConnetion(conn net.Conn) {
	/*
		defer conn.Close()
		//clientAddress := conn.RemoteAddr().String()
		fmt.Println("siamo nel server e worka " + conn.RemoteAddr().String())

		reader := bufio.NewReader(conn)

		data, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error while retrieving data" + err.Error())
			return
		}
		fmt.Println(data)

		//sendResult(
	} */
	defer conn.Close()

	// Crea un reader per leggere dalla connessione
	var buf [4]byte // Buffer per leggere la lunghezza (int32)
	_, err := conn.Read(buf[:])
	if err != nil {
		fmt.Println("Errore durante la lettura della lunghezza:", err)
		return
	}

	// Leggi la lunghezza dei dati
	var length int32
	err = binary.Read(bytes.NewReader(buf[:]), binary.LittleEndian, &length)
	if err != nil {
		fmt.Println("Errore durante la lettura della lunghezza:", err)
		return
	}
	fmt.Println("Lunghezza dei dati ricevuti:", length)

	// Leggi i dati (i numeri inviati dal client)
	data := make([]int32, length)
	for i := int32(0); i < length; i++ {
		// Leggi ogni singolo intero (4 byte)
		err := binary.Read(conn, binary.LittleEndian, &data[i])
		if err != nil {
			fmt.Println("Errore durante la lettura dei dati:", err)
			return
		}
	}

	// Stampa i dati ricevuti
	fmt.Println("Dati ricevuti dal client:", data)
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
