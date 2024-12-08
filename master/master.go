package main

import (
	"fmt"
	"log"
	"mapReduce/utils"
	"net"
	"net/rpc"
	"sync"
)

type Master struct{}

func (m *Master) ReceiveData(args *utils.ClientArgs, reply *utils.ClientReply) error {
	fmt.Println("Dati ricevuti dal Client:", args.Data)

	// Numero di Worker e distribuzione round-robin dei dati
	numWorkers := 5
	workerRanges := make(map[int][]int32)
	var wg sync.WaitGroup

	// Distribuzione round-robin dei dati tra i Worker
	for i, value := range args.Data {
		workerID := (i % numWorkers) + 1
		workerRanges[workerID] = append(workerRanges[workerID], value)
	}

	// Invia i dati ai Worker
	for workerID, data := range workerRanges {
		wg.Add(1)
		go func(workerID int, data []int32) {
			defer wg.Done()

			// Connessione al Worker
			workerAddr := fmt.Sprintf("127.0.0.1:%d", 5000+workerID)
			client, err := rpc.Dial("tcp", workerAddr)
			if err != nil {
				log.Printf("Errore nella connessione al Worker %d: %v", workerID, err)
				return
			}
			defer client.Close()

			// Crea l'argomento per il Worker
			workerArgs := utils.WorkerArgs{
				Job:          createKeyValuePairs(data),
				WorkerID:     workerID,
				WorkerRanges: workerRanges,
			}
			var workerReply utils.WorkerReply
			err = client.Call("Worker.ProcessJob", &workerArgs, &workerReply)
			if err != nil {
				log.Printf("Errore durante l'invocazione RPC al Worker %d: %v", workerID, err)
				return
			}

			fmt.Printf("Master ha inviato i dati al Worker %d: %v\n", workerID, workerReply.Ack)
		}(workerID, data)
	}

	wg.Wait()

	// Avvia la fase di riduzione
	startReducePhase(workerRanges)

	reply.Ack = "Dati elaborati e inviati ai Worker per la fase di mappatura"
	return nil
}

// Crea coppie chiave-valore da un array di dati
func createKeyValuePairs(data []int32) map[int32]int32 {
	result := make(map[int32]int32)
	for _, value := range data {
		result[value]++
	}
	return result
}

// Funzione per avviare la fase di riduzione
func startReducePhase(workerRanges map[int][]int32) {
	var wg sync.WaitGroup
	for workerID := range workerRanges {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			workerAddr := fmt.Sprintf("127.0.0.1:%d", 5000+workerID)
			client, err := rpc.Dial("tcp", workerAddr)
			if err != nil {
				log.Printf("Errore nella connessione al Worker %d per la fase di riduzione: %v", workerID, err)
				return
			}
			defer client.Close()

			// Invio della richiesta di riduzione al Worker
			reduceArgs := utils.ReduceArgs{}
			reduceReply := utils.ReduceReply{}
			err = client.Call("Worker.ReduceJob", &reduceArgs, &reduceReply)
			if err != nil {
				log.Printf("Errore durante la chiamata RPC per la riduzione al Worker %d: %v", workerID, err)
				return
			}

			fmt.Printf("Worker %d ha completato la fase di riduzione: %v\n", workerID, reduceReply.Ack)
		}(workerID)
	}

	wg.Wait()
}

func main() {
	master := new(Master)
	server := rpc.NewServer()
	err := server.Register(master)
	if err != nil {
		log.Fatalf("Errore durante la registrazione del Master: %v", err)
	}

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
