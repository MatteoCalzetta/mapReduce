package main

import (
	"fmt"
	"log"
	"mapReduce/utils"
	"net"
	"net/rpc"
	"sort"
	"sync"
)

type Master struct {
	CollectedData []utils.WorkerData // Slice per raccogliere i dati da ciascun worker
	mu            sync.Mutex         // Mutex per proteggere l'accesso ai dati raccolti
	FinalData     map[int32]int32
}

func (m *Master) ReceiveDataFromWorker(args *utils.WorkerArgs, reply *utils.WorkerReply) error {
	m.mu.Lock() //lock mutex to append data thread safely
	defer m.mu.Unlock()

	workerData := utils.WorkerData{
		WorkerID: args.WorkerID,
		Data:     args.Job,
	}
	m.CollectedData = append(m.CollectedData, workerData)
	reply.Ack = "Data received from worker"
	fmt.Println("i dati in collected data sono: ", m.CollectedData)
	SendClientResponse(m.CollectedData)
	return nil
}

// Ordina i dati per WorkerID
func sortData(data []utils.WorkerData) {
	sort.Slice(data, func(i, j int) bool {
		return data[i].WorkerID < data[j].WorkerID
	})
}

// Trasforma i dati da struct a un array espanso e ordinato
func transformDataToArray(data []utils.WorkerData) []int32 {
	var result []int32

	// Itera su ciascun WorkerData
	for _, worker := range data {
		for key, value := range worker.Data {
			// Aggiungi la chiave al risultato per "value" volte
			for i := 0; i < int(value); i++ {
				result = append(result, key)
			}
		}
	}

	// Ordina il risultato finale
	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})

	return result
}

// Invia la risposta al client
// Invia la risposta al client
func SendClientResponse(data []utils.WorkerData) {
	// Ordina i dati per WorkerID
	sortData(data)

	// Trasforma i dati in un array di interi espanso e ordinato
	finalArray := transformDataToArray(data)

	// Stampa l'array risultante
	fmt.Printf("Dati finali da inviare al client: %v\n", finalArray)

	// Connessione al client tramite RPC
	clientAddr := "127.0.0.1:8086" // Indirizzo del client
	client, err := rpc.Dial("tcp", clientAddr)
	if err != nil {
		log.Printf("Errore nella connessione al Client: %v", err)
		return
	}
	defer client.Close()

	// Prepara la risposta da inviare al client
	clientResponse := utils.ClientResponse{
		FinalData: finalArray,
		Ack:       "Dati finali inviati al client con successo",
	}

	// Prepara una richiesta vuota (a meno che non abbia dei parametri specifici da inviare)
	clientRequest := utils.ClientRequest{}

	// Effettua la chiamata RPC al client
	err = client.Call("Client.ReceiveFinalData", &clientRequest, &clientResponse)
	if err != nil {
		log.Printf("Errore durante l'invio dei dati al Client: %v", err)
		return
	}

	fmt.Printf("Risposta del client: %s\n", clientResponse.Ack)
}

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
