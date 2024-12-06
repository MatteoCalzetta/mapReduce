package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"mapReduce/config"
	"net"
	"os"
)

type Worker struct {
	ID      int
	Address string
}

func receiveMapperJob(conn net.Conn) {
	defer conn.Close()

	fmt.Println("Received job...")
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
	fmt.Println("Data from master: ", data) // Stampa i dati ricevuti

	dataKeyValue := keyValueFunction(data)
	fmt.Println("dataKeyValue", dataKeyValue)

	/*
		// Invia una conferma al Master
		ack := "DONE"
		_, err = conn.Write([]byte(ack))
		if err != nil {
			fmt.Println("Errore durante l'invio della conferma al Master:", err)
		}

		fmt.Println("Acknowledgment sent to Master.")
	*/

}

func keyValueFunction(data []int32) [][]int32 {

	fmt.Println("inizio a mappare")

	// Mappa per contare le occorrenze
	occurrences := make(map[int32]int32)

	// Array per tenere traccia dell'ordine di apparizione delle chiavi, la funzione map esegue in modo non ordinato
	var order []int32

	// Conta le occorrenze e registra l'ordine di input
	for _, value := range data {
		if _, exists := occurrences[value]; !exists {
			order = append(order, value) // Se il valore non è presente lo aggiunge
		}
		occurrences[value]++
	}

	// Crea il risultato come array di coppie [dato, occorrenze] con stesso ordine di input
	var dataKeyValue [][]int32
	for _, key := range order {
		dataKeyValue = append(dataKeyValue, []int32{key, occurrences[key]})
	}

	return dataKeyValue
}

func main() {

	var myWorker Worker
	id := flag.Int("ID", 0, "Worker's ID")
	flag.Parse()

	if *id < 1 || *id > 5 {
		fmt.Println("ID must be between 1 and 5, no valid worker ID")
		os.Exit(1)
	}

	Workers, err := config.GetWorkers()
	if err != nil {
		fmt.Println("Error getting workers:", err)
		os.Exit(1)
	}

	for _, worker := range Workers {
		if worker.ID == *id {
			fmt.Println("Found worker:", worker.ID)
			myWorker = Worker(worker)
		}
	}

	fmt.Println(myWorker.ID, myWorker.Address) //id corretti qui :)

	listener, err := net.Listen("tcp", myWorker.Address)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	defer listener.Close()

	for { //lasciare in ascolto worker
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		go receiveMapperJob(conn)
	}

}
