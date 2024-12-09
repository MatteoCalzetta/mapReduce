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

func calculateRanges(totalItems, totalWorkers int) map[int][]int32 {
	// Mappa per memorizzare i range dei worker
	workerRanges := make(map[int][]int32)
	rangeSize := totalItems / totalWorkers      // Calcola la dimensione di base del range
	remainingItems := totalItems % totalWorkers // Calcola gli item rimanenti

	start := 1 // Il primo range inizia da 1

	for i := 1; i <= totalWorkers; i++ {
		// Determina l'elemento finale per questo worker
		end := start + rangeSize - 1
		if i <= remainingItems {
			end++ // I primi 'remainingItems' workers ricevono uno in più
		}

		// Se siamo all'ultimo worker, assicuriamoci che prenda tutti i valori rimanenti
		if i == totalWorkers {
			end = totalItems
		}

		// Crea il range per il worker
		rangeList := make([]int32, 0, end-start+1)
		for j := start; j <= end; j++ {
			rangeList = append(rangeList, int32(j))
		}

		// Aggiungi il range al worker
		workerRanges[i] = rangeList

		// Prepara l'inizio del range per il prossimo worker
		start = end + 1
	}

	return workerRanges
}

func findMax(arr []int32) int32 {
	max := arr[0] // inizializza max con il primo elemento
	for _, value := range arr {
		if value > max {
			max = value // aggiorna max se trovi un valore maggiore
		}
	}
	return max
}

func (m *Master) ReceiveData(args *utils.ClientArgs, reply *utils.ClientReply) error {
	fmt.Println("Dati ricevuti dal Client:", args.Data)

	// Numero di Worker e distribuzione dei dati
	numWorkers := 5
	maxData := findMax(args.Data)
	workerRanges := calculateRanges(int(maxData), numWorkers)
	fmt.Println("Workers ranges are:", workerRanges)

	// Prepara la distribuzione dei dati round-robin senza alterare workerRanges
	var workerData = make(map[int][]int32)

	// Distribuzione round-robin dei dati tra i Workers
	for i, value := range args.Data {
		workerID := (i % numWorkers) + 1
		workerData[workerID] = append(workerData[workerID], value)
	}

	// Invia i dati ai Worker
	var wg sync.WaitGroup
	for workerID, data := range workerData {
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
				Job:          createKeyValuePairs(data), // Crea le coppie chiave-valore
				WorkerID:     workerID,
				WorkerRanges: workerRanges, // Passa i ranges già calcolati
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

	fmt.Println("result dentro master è ", result)
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
