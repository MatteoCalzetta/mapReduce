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

	// Configura i Worker dinamicamente
	workerIDs := []int{1, 2, 3, 4, 5} // ID dei Worker
	basePort := 5000                  // Porta base per i Worker
	workers := getDynamicWorkers(workerIDs, basePort)

	/*
		// Calcola la dimensione del range di numeri per ciascun Worker
		numWorkers := len(workers)
		rangeSize := len(args.Data) / numWorkers
		remaining := len(args.Data) % numWorkers
	*/

	// Assegna un range a ciascun Worker e invia le informazioni
	workerRanges := getWorkerRanges(workerIDs, args.Data)
	var wg sync.WaitGroup
	for i, workerAddr := range workers {
		wg.Add(1)
		go func(i int, workerAddr string) {
			defer wg.Done()

			workerID := i + 1
			rangeToProcess := workerRanges[workerID]

			// Crea l'argomento per il Worker, includendo il proprio range e gli altri range
			workerArgs := utils.WorkerArgs{
				Job:          createKeyValuePairs(rangeToProcess), // Funzione per creare coppie chiave-valore
				WorkerID:     workerID,
				WorkerRanges: workerRanges,
			}

			client, err := rpc.Dial("tcp", workerAddr)
			if err != nil {
				log.Printf("Errore nella connessione al Worker %d su %s: %v", workerID, workerAddr, err)
				return
			}
			defer client.Close()

			workerReply := utils.WorkerReply{}
			asyncCall := client.Go("Worker.ProcessJob", workerArgs, &workerReply, nil)
			<-asyncCall.Done
			if asyncCall.Error != nil {
				log.Printf("Errore durante l'invocazione RPC al Worker %d: %v", workerID, asyncCall.Error)
				return
			}
			fmt.Printf("Worker %d su %s ha completato la mappatura: %v\n", workerID, workerAddr, workerReply.Ack)
		}(i, workerAddr)
	}

	wg.Wait()

	// Avvia la fase di riduzione: raccogli i risultati e chiedi ai Worker di ridurre i dati
	startReducePhase(workers)

	reply.Ack = "Dati elaborati e inviati ai Worker per la fase di mappatura"
	return nil
}

// Funzione per creare le coppie chiave-valore da un range
func createKeyValuePairs(data []int32) map[int32]int32 {
	result := make(map[int32]int32)
	for _, value := range data {
		result[value]++
	}
	return result
}

// Funzione per ottenere i range di computazione di tutti i Worker
func getWorkerRanges(ids []int, data []int32) map[int][]int32 {
	workerRanges := make(map[int][]int32)
	numWorkers := len(ids)
	rangeSize := len(data) / numWorkers
	remaining := len(data) % numWorkers

	for i, id := range ids {
		start := i * rangeSize
		end := start + rangeSize
		if i == numWorkers-1 {
			end += remaining
		}
		workerRanges[id] = data[start:end]
	}
	return workerRanges
}

// Funzione per avviare la fase di riduzione
func startReducePhase(workers []string) {
	var wg sync.WaitGroup
	for _, workerAddr := range workers {
		wg.Add(1)
		go func(workerAddr string) {
			defer wg.Done()
			client, err := rpc.Dial("tcp", workerAddr)
			if err != nil {
				log.Printf("Errore nella connessione al Worker per la fase di riduzione su %s: %v", workerAddr, err)
				return
			}
			defer client.Close()

			// Invia un comando di riduzione ai Worker
			reduceArgs := utils.ReduceArgs{}
			reduceReply := utils.ReduceReply{}
			asyncCall := client.Go("Worker.ReduceJob", reduceArgs, &reduceReply, nil)
			<-asyncCall.Done
			if asyncCall.Error != nil {
				log.Printf("Errore durante la chiamata RPC per la riduzione al Worker su %s: %v", workerAddr, asyncCall.Error)
				return
			}
			fmt.Printf("Worker su %s ha completato la riduzione: %v\n", workerAddr, reduceReply.Ack)
		}(workerAddr)
	}

	wg.Wait()
}

// Funzione per generare dinamicamente gli indirizzi dei Worker
func getDynamicWorkers(ids []int, basePort int) []string {
	workers := make([]string, len(ids))
	for i, id := range ids {
		workers[i] = fmt.Sprintf("127.0.0.1:%d", basePort+id)
	}
	return workers
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
