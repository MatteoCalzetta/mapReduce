package main

import (
	"fmt"
	"log"
	"mapReduce/utils"
	"math/rand"
	"net/rpc"
	"time"
)

// Genero input casuali, array con 100 valori compresi tra 1 e 30
func generateRandomInput(size, min, max int) []int32 {
	rand.Seed(time.Now().UnixNano())
	input := make([]int32, size)
	for i := 0; i < size; i++ {
		input[i] = int32(rand.Intn(max-min+1) + min)
	}
	return input
}

// Struct per ricevere dati da master
type Client struct{}

// Ricevo dati finali dal master
func (c *Client) ReceiveFinalData(args *utils.ClientRequest, reply *utils.ClientResponse) error {

	fmt.Println("Dati finali ricevuti dal Master:", reply.FinalData)

	reply.Ack = "Dati ricevuti correttamente dal client"
	return nil
}

func main() {
	size := 100
	min := 1
	max := 30
	masterAddress := "master:8080"

	//Chiamata a funz per generare numeri casuali in base ai parametri
	dataToProcess := generateRandomInput(size, min, max)

	//Connessione al Master tramite RPC
	client, err := rpc.Dial("tcp", masterAddress)
	if err != nil {
		log.Fatalf("Errore durante la connessione al Master: %v", err)
	}
	defer client.Close()

	args := &utils.ClientArgs{Data: dataToProcess}
	reply := &utils.ClientReply{}

	err = client.Call("Master.ReceiveData", args, reply)
	if err != nil {
		log.Fatalf("Errore durante la chiamata RPC al Master: %v", err)
	}

	fmt.Println("Risposta dal Master:", reply.Ack)
	fmt.Printf("Array ordinato: %v\n", reply.FinalData)

}
