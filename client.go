package main

import (
	"fmt"
	"log"
	"mapReduce/utils"
	"math/rand"
	"net"
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
	clientAddr := "127.0.0.1:8086" // Indirizzo del client

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

	// Avvia il server RPC per il client (ascoltando sulla porta 8086)
	clientServer := new(Client)
	server := rpc.NewServer()
	err = server.Register(clientServer)
	if err != nil {
		log.Fatalf("Errore durante la registrazione del Client: %v", err)
	}

	listener, err := net.Listen("tcp", clientAddr)
	if err != nil {
		log.Fatalf("Errore durante l'ascolto del Client su %s: %v", clientAddr, err)
	}
	defer listener.Close()

	fmt.Printf("Client in ascolto su %s\n", clientAddr)

	// Ora il client è in ascolto sulla porta 8086 per ricevere i dati finali
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("Errore durante l'accettazione della connessione: %v", err)
				continue
			}
			go server.ServeConn(conn)
		}
	}()

	// Connessione al client per ricevere i dati finali
	finalDataArgs := &utils.ClientRequest{}
	finalDataReply := &utils.ClientResponse{}

	// Connessione al client per ricevere i dati finali
	client, err = rpc.Dial("tcp", clientAddr)
	if err != nil {
		log.Fatalf("Errore durante la connessione al client per ricevere i dati finali: %v", err)
	}
	defer client.Close()

	// Chiama il metodo RPC del client per ricevere i dati finali dal master
	err = client.Call("Client.ReceiveFinalData", finalDataArgs, finalDataReply)
	if err != nil {
		log.Fatalf("Errore durante la chiamata RPC al client per ricevere i dati finali: %v", err)
	}

	// Stampa la risposta finale del client
	fmt.Println("Dati finali ricevuti dal Master:", finalDataReply.FinalData)
	fmt.Println("Risposta finale dal Master:", finalDataReply.Ack)
}
