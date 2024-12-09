package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"time"
)

// Argomenti per l'RPC Client-to-Master
type ClientArgs struct {
	Data []int32
}

// Risposta per l'RPC Client-to-Master
type ClientReply struct {
	Ack string
}

// Funzione per generare input casuali
func generateRandomInput(size, min, max int) []int32 {
	rand.Seed(time.Now().UnixNano())
	input := make([]int32, size)
	for i := 0; i < size; i++ {
		input[i] = int32(rand.Intn(max-min+1) + min)
	}
	return input
}

func main() {
	size := 100
	min := 1
	max := 30
	masterAddress := "127.0.0.1:8080"

	// Genera i dati casuali
	dataToProcess := generateRandomInput(size, min, max)

	// Connessione al Master tramite RPC
	client, err := rpc.Dial("tcp", masterAddress)
	if err != nil {
		log.Fatalf("Errore durante la connessione al Master: %v", err)
	}
	defer client.Close()

	args := &ClientArgs{Data: dataToProcess}
	reply := &ClientReply{}

	// Chiama il metodo RPC del Master
	err = client.Call("Master.ReceiveData", args, reply)
	if err != nil {
		log.Fatalf("Errore durante la chiamata RPC al Master: %v", err)
	}

	// Stampa la risposta del Master
	fmt.Println("Risposta dal Master:", reply.Ack)
}
