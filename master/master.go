package main

import (
	"fmt"
	"log"
	"mapReduce/utils"
	"net"
	"net/rpc"
	_ "strconv"
	"sync"
)

// Struct RPC per il Master
type Master struct{}

// Funzione per ricevere i dati dal Client
func (m *Master) ReceiveData(args *utils.ClientArgs, reply *utils.ClientReply) error {
	fmt.Println("Dati ricevuti dal Client:", args.Data)

	// Calcola gli indirizzi dei Worker dinamicamente basati sugli ID passati nella linea di comando
	workerIDs := []int{1, 2, 3, 4, 5} // ID dei Worker
	basePort := 5000                  // Porta base per il Worker
	workers := getDynamicWorkers(workerIDs, basePort)

	// Suddividi i dati tra i Worker
	workerJobs := distributeJobsRoundRobin(args.Data, len(workers))
	fmt.Println("Jobs distribuiti ai Worker:", workerJobs)

	// Distribuisci i job ai Worker in parallelo
	var wg sync.WaitGroup
	for i, workerAddr := range workers {
		wg.Add(1)
		go func(i int, workerAddr string) {
			defer wg.Done()
			client, err := rpc.Dial("tcp", workerAddr)
			if err != nil {
				log.Printf("Errore nella connessione al Worker %d su %s: %v", i+1, workerAddr, err)
				return
			}
			defer client.Close()

			workerArgs := utils.WorkerArgs{Job: workerJobs[i]}
			workerReply := utils.WorkerReply{}
			asyncCall := client.Go("Worker.ProcessJob", workerArgs, &workerReply, nil)
			<-asyncCall.Done
			if asyncCall.Error != nil {
				log.Printf("Errore durante l'invocazione RPC al Worker %d: %v", i+1, asyncCall.Error)
				return
			}
			fmt.Printf("Worker %d su %s ha completato il job: %v\n", i+1, workerAddr, workerReply.Ack)
		}(i, workerAddr)
	}

	wg.Wait()
	reply.Ack = "Dati elaborati e inviati ai Worker"
	return nil
}

// Funzione per generare dinamicamente gli indirizzi dei Worker
func getDynamicWorkers(ids []int, basePort int) []string {
	workers := make([]string, len(ids))
	for i, id := range ids {
		workers[i] = fmt.Sprintf("127.0.0.1:%d", basePort+id)
	}
	return workers
}

// Funzione per suddividere i job in modalità round-robin
func distributeJobsRoundRobin(jobs []int32, numWorkers int) [][]int32 {
	workerJobs := make([][]int32, numWorkers)
	for i, job := range jobs {
		workerIndex := i % numWorkers
		workerJobs[workerIndex] = append(workerJobs[workerIndex], job)
	}
	return workerJobs
}

func main() {
	// Crea un'istanza del Master
	master := new(Master)

	// Registra il Master come un servizio RPC
	server := rpc.NewServer()
	err := server.Register(master)
	if err != nil {
		log.Fatalf("Errore durante la registrazione del Master: %v", err)
	}

	// Avvia il listener per le connessioni RPC
	address := "127.0.0.1:8080"
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Errore durante l'ascolto del Master su %s: %v", address, err)
	}
	defer listener.Close()

	fmt.Printf("Master in ascolto su %s\n", address)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Errore durante l'accettazione della connessione: %v", err)
			continue
		}
		go server.ServeConn(conn)
	}
}
