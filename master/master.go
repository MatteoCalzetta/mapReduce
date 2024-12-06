package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"mapReduce/config"
	"net"
	"os"
)

func distributeJobsRoundRobin(jobs []int32, numWorkers int) [][]int32 {
	// Crea una slice di slice per memorizzare i job per ogni worker
	workersJobs := make([][]int32, numWorkers)

	// Assegna ogni job al worker corrispondente in modo round-robin
	for i, job := range jobs {
		// Calcola a quale worker assegnare il job (round-robin)
		workerIndex := i % numWorkers
		// Aggiungi il job alla lista di job del worker
		workersJobs[workerIndex] = append(workersJobs[workerIndex], job)
	}

	return workersJobs
}

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

	workerJobs := distributeJobsRoundRobin(data, len(workers))
	fmt.Println(len(workerJobs[0]), len(workerJobs[1]), len(workerJobs[2]), len(workerJobs[3]), len(workerJobs[4]), "\n")

	sendSliceToWorkers(workers, workerJobs)

}

func sendSliceToWorkers(workers []config.Worker, jobs [][]int32) {
	i := 0
	for _, worker := range workers {
		var buf bytes.Buffer //dati in bytes per comunicazione

		lenght := int32(len(jobs[i]))
		err := binary.Write(&buf, binary.LittleEndian, lenght)
		if err != nil {
			fmt.Println("Error writing data to buffer:", err)
			os.Exit(1)
		}

		// Scrivi i dati reali (i numeri casuali)
		for _, val := range jobs[i] {
			err := binary.Write(&buf, binary.LittleEndian, int32(val)) // Scrivi ogni intero come 4 byte
			if err != nil {
				fmt.Println("Error writing data to buffer:", err)
				os.Exit(1)
			}
		}

		conn, err := net.Dial("tcp", worker.Address) //init connessione col server
		if err != nil {
			fmt.Println("Error connecting to worker:", err)
			os.Exit(1)
		}
		fmt.Println("Connected to worker on " + conn.LocalAddr().String())
		defer conn.Close()

		_, err = conn.Write(buf.Bytes())
		if err != nil {
			fmt.Println("Error sending data to worker:", err)
			os.Exit(1)
		}
		fmt.Println("data to master has been sent", worker.Address)
		i += 1
	}
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
