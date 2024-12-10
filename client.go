package main

import (
	"fmt"
	"log"
	"mapReduce/utils"
	"math/rand"
	"net/rpc"
	"time"
)

// Funzione per generare input casuali
func generateRandomInput(size, min, max int) []int32 {
	rand.Seed(time.Now().UnixNano())
	input := make([]int32, size)
	for i := 0; i < size; i++ {
		input[i] = int32(rand.Intn(max-min+1) + min)
	}
	return input
}

// Client struttura con il metodo per ricevere i dati finali dal Master
type Client struct{}

// Funzione che riceve i dati finali dal master
func (c *Client) ReceiveFinalData(args *utils.ClientRequest, reply *utils.ClientResponse) error {
	// Stampa i dati finali ricevuti dal master
	fmt.Println("Dati finali ricevuti dal Master:", reply.FinalData)

	// Risposta di conferma
	reply.Ack = "Dati ricevuti correttamente dal client"
	return nil
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

	args := &utils.ClientArgs{Data: dataToProcess}
	reply := &utils.ClientReply{}

	// Chiama il metodo RPC del Master
	err = client.Call("Master.ReceiveData", args, reply)
	if err != nil {
		log.Fatalf("Errore durante la chiamata RPC al Master: %v", err)
	}

	// Stampa la risposta del Master
	fmt.Println("Risposta dal Master:", reply.Ack)

}
