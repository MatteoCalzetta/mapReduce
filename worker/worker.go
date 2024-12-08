package main

import (
	"flag"
	"fmt"
	"log"
	"mapReduce/utils"
	"net"
	"net/rpc"
	"os"
)

// Struct per il Worker RPC
type Worker struct{}

// Funzione per processare i job ricevuti dal Master
func (w *Worker) ProcessJob(args *utils.WorkerArgs, reply *utils.WorkerReply) error {
	fmt.Printf("Worker %d ricevuto job: %v\n", os.Getpid(), args.Job)

	// Calcola la coppia chiave-valore
	result := make(map[int32]int32)
	for _, value := range args.Job {
		result[value]++
	}

	reply.Ack = fmt.Sprintf("Job completato con %d valori unici", len(result))
	fmt.Printf("Worker %d completato job: %v\n", os.Getpid(), result)
	return nil
}

func main() {
	// Leggi l'ID e la porta iniziale da linea di comando
	id := flag.Int("ID", 0, "ID del Worker")
	port := flag.Int("port", 5000, "Porta base per il Worker")
	flag.Parse()

	if *id <= 0 {
		fmt.Println("L'ID deve essere maggiore di 0")
		os.Exit(1)
	}

	// Calcola l'indirizzo basato sull'ID e la porta iniziale
	address := fmt.Sprintf("127.0.0.1:%d", *port+*id)
	fmt.Printf("Avvio Worker %d su %s\n", *id, address)

	// Crea un'istanza del Worker
	worker := new(Worker)
	server := rpc.NewServer()
	err := server.Register(worker)
	if err != nil {
		log.Fatalf("Errore durante la registrazione del Worker %d: %v", *id, err)
	}

	// Avvia il listener per le connessioni RPC
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Errore durante l'ascolto del Worker %d: %v", *id, err)
	}
	defer listener.Close()

	fmt.Printf("Worker %d in ascolto su %s\n", *id, address)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Errore durante l'accettazione della connessione: %v", err)
			continue
		}
		go server.ServeConn(conn)
	}
}
