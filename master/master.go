package main

import (
	"fmt"
	"log"
	"mapReduce/utils"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"sync"
)

type Master struct {
	CollectedData []utils.WorkerData //Slice raccogliere i dati da ciascun worker
	mu            sync.Mutex         //Mutex sincronizzare l'accesso ai dati raccolti
	FinalData     map[int32]int32
}

// gestisco la ricezione dati dai worker, per ogni nuovo dato in arrivo viene fatto l'append su CollectedData
func (m *Master) ReceiveDataFromWorker(args *utils.WorkerArgs, reply *utils.WorkerReply) error {
	m.mu.Lock() //lock mutex
	defer m.mu.Unlock()

	workerData := utils.WorkerData{
		WorkerID: args.WorkerID,
		Data:     args.Job,
	}
	m.CollectedData = append(m.CollectedData, workerData)
	reply.Ack = "Data received from worker"
	//fmt.Println("i dati in collected data sono: ", m.CollectedData)
	return nil
}

// Ordina i dati per WorkerID, la mappa non mantiene una posizione reale per gli elementi contenuti
func sortData(data []utils.WorkerData) {
	sort.Slice(data, func(i, j int) bool {
		return data[i].WorkerID < data[j].WorkerID
	})
}

// Trasforma i dati da struct a un array espanso e ordinato per rispedire nello stesso formato in cui il client ha inviato la richiesta
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

	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})

	return result
}

func calculateRanges(totalItems, totalWorkers int) map[int][]int32 {
	//Mappa per memorizzare i range dei worker
	workerRanges := make(map[int][]int32)
	rangeSize := totalItems / totalWorkers      //Calcolo dimensione base del range
	remainingItems := totalItems % totalWorkers //Calcolo item rimanenti

	start := 1

	for i := 1; i <= totalWorkers; i++ {
		//Determina l'elemento finale per il worker i-esimo
		end := start + rangeSize - 1
		if i <= remainingItems {
			end++ //I primi 'remainingItems' workers ricevono uno in più
		}

		//Se siamo all'ultimo worker, assicuriamoci che prenda tutti i valori rimanenti
		if i == totalWorkers {
			end = totalItems
		}

		//Creo range per worker
		rangeList := make([]int32, 0, end-start+1)
		for j := start; j <= end; j++ {
			rangeList = append(rangeList, int32(j))
		}

		workerRanges[i] = rangeList
		start = end + 1
	}

	return workerRanges
}

func findMax(arr []int32) int32 {
	max := arr[0] //inizializzo max con primo elemento
	for _, value := range arr {
		if value > max {
			max = value //aggiorno max se trovi valore maggiore
		}
	}
	return max
}

func (m *Master) ReceiveData(args *utils.ClientArgs, reply *utils.ClientReply) error {
	fmt.Println()
	fmt.Println("Dati ricevuti dal Client:", args.Data)

	//Numero di Worker e distribuzione dei dati
	numWorkers := 5
	maxData := findMax(args.Data)
	workerRanges := calculateRanges(int(maxData), numWorkers)

	fmt.Println()
	fmt.Println("Workers ranges are:", workerRanges)
	fmt.Println()

	//Prepara la distribuzione dei dati round-robin senza alterare workerRanges
	var workerData = make(map[int][]int32)

	for i, value := range args.Data {
		workerID := (i % numWorkers) + 1
		workerData[workerID] = append(workerData[workerID], value)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex //Mutex per operazione atomica su m.CollectedData durante le modifiche

	for workerID, data := range workerData {
		wg.Add(1)
		go func(workerID int, data []int32) {
			defer wg.Done()

			workerAddr := fmt.Sprintf("worker-%d:%d", workerID, 5000+workerID)
			workerConn, err := rpc.Dial("tcp", workerAddr)
			if err != nil {
				log.Printf("Errore nella connessione al Worker %d: %v", workerID, err)
				return
			}
			defer workerConn.Close()

			workerArgs := utils.WorkerArgs{
				JobTodo:      data,
				WorkerID:     workerID,
				WorkerRanges: workerRanges,
			}

			var workerReply utils.WorkerReply
			err = workerConn.Call("Worker.ProcessJob", &workerArgs, &workerReply)
			if err != nil {
				log.Printf("Errore durante l'invocazione RPC al Worker %d: %v", workerID, err)
				return
			}

			//Aggiorna CollectedData in modo sicuro usando mutex
			mu.Lock()
			m.CollectedData = append(m.CollectedData, utils.WorkerData{
				WorkerID: workerID,
				Data:     workerReply.Data,
			})
			mu.Unlock()

			fmt.Printf("Worker %d ha completato la fase di mapping con: %v\n", workerID, workerReply.Ack)
		}(workerID, data)
	}

	wg.Wait() //Aspetta che tutte le goroutine terminino

	//Avvio la fase reduce
	startReducePhase(workerRanges)

	//Risultato per il client
	finalArray := transformDataToArray(m.CollectedData)
	fmt.Println()
	fmt.Printf("Numeri ordinati da restituire al client: %v\n", finalArray)
	reply.FinalData = finalArray
	reply.Ack = "Dati elaborati con successo!"

	file, err := os.Create("result.txt")
	if err != nil {
		return fmt.Errorf("errore nella creazione del file: %v", err)
	}
	defer file.Close()

	//Concateno numeri della slice in singola stringa separata da spazi
	var stringSlice []string
	for _, num := range finalArray {
		stringSlice = append(stringSlice, fmt.Sprintf("%d", num))
	}
	line := strings.Join(stringSlice, " ") // Unisce tutti i numeri separandoli con spazi

	//Scrivo stringa nel file
	_, err = file.WriteString(line)
	if err != nil {
		return fmt.Errorf("errore nella scrittura del file: %v", err)
	}

	return nil
}

// Funzione per avviare la fase di riduzione, viene chiamato un worker con una go function per ogni worker.
func startReducePhase(workerRanges map[int][]int32) {
	fmt.Println()
	fmt.Println("Inizio fase di reduce")
	fmt.Println()
	var wg sync.WaitGroup
	for workerID := range workerRanges {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			//scelta dell'address dei worker scalabile, workerID si trova nella mappa
			workerAddr := fmt.Sprintf("worker-%d:%d", workerID, 5000+workerID)
			client, err := rpc.Dial("tcp", workerAddr)
			if err != nil {
				log.Printf("Errore nella connessione al Worker %d per la fase di riduzione: %v", workerID, err)
				return
			}
			defer client.Close()

			reduceArgs := utils.ReduceArgs{}
			reduceReply := utils.ReduceReply{}
			err = client.Call("Worker.ReduceJob", &reduceArgs, &reduceReply)
			if err != nil {
				log.Printf("Errore durante la chiamata RPC per la riduzione al Worker %d: %v", workerID, err)
				return
			}

			fmt.Printf("Worker %d ha completato la fase di riduzione\n", workerID)
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

	value := os.Getenv("MASTER_NAME")

	address := fmt.Sprintf(value + ":" + "8080")
	fmt.Println("Master address:", address)

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
