package main

import (
	"flag"
	"fmt"
	"log"
	"mapReduce/utils"
	"net"
	"net/rpc"
	"os"
	"sync"
)

type Worker struct {
	WorkerID     int
	WorkerRanges map[int][]int32 // Mappa dei range di lavoro di tutti i Worker
	Intermediate map[int32]int32 // Mappa per le coppie chiave-valore intermediari
	mu           sync.Mutex      // Mutex per sincronizzare l'accesso alla mappa Intermediate
	wg           sync.WaitGroup  // WaitGroup per la sincronizzazione
}

func (w *Worker) ProcessJob(args *utils.WorkerArgs, reply *utils.WorkerReply) error {

	w.WorkerID = args.WorkerID
	w.WorkerRanges = args.WorkerRanges
	w.Intermediate = args.Job

	fmt.Println("i range dei worker sono ", w.WorkerRanges)
	fmt.Println("i job dek worker sono ", w.Intermediate)

	reply.Ack = fmt.Sprintf("Job completato con %d valori unici", len(w.Intermediate))
	//fmt.Printf("Worker %d completato job: %v\n", w.WorkerID, w.Intermediate)
	return nil
}

// Funzione per avviare la fase di riduzione e scambio di dati tra Worker
func (w *Worker) ReduceJob(args *utils.ReduceArgs, reply *utils.ReduceReply) error {
	fmt.Printf("Worker %d avvia la fase di riduzione\n", w.WorkerID)
	var wg sync.WaitGroup

	// Itera su tutti gli altri Worker e scambia le coppie chiave-valore non pertinenti
	for otherID, otherRange := range w.WorkerRanges {
		if otherID == w.WorkerID {
			continue
		}

		wg.Add(1)
		go func(otherID int, otherRange []int32) {
			defer wg.Done()

			// Connessione al Worker di destinazione per inviare i dati
			otherWorkerAddr := fmt.Sprintf("127.0.0.1:%d", 5000+otherID)
			client, err := rpc.Dial("tcp", otherWorkerAddr)
			if err != nil {
				log.Printf("Errore nella connessione al Worker %d: %v", otherID, err)
				return
			}
			defer client.Close()

			// Crea una mappa temporanea per i dati da inviare
			tempPairs := make(map[int32]int32)
			w.mu.Lock()
			for key, value := range w.Intermediate {
				// Invia solo le coppie chiave-valore non nel proprio range
				if !isInRange(key, w.WorkerRanges[w.WorkerID]) && isInRange(key, otherRange) {
					tempPairs[key] += value
					delete(w.Intermediate, key)
				}
			}
			w.mu.Unlock()

			// Invia i dati al Worker di destinazione
			if len(tempPairs) > 0 {
				sendArgs := utils.WorkerArgs{
					Job:          tempPairs,
					WorkerID:     w.WorkerID,
					WorkerRanges: w.WorkerRanges,
				}
				var sendReply utils.WorkerReply
				err = client.Call("Worker.ReceiveData", &sendArgs, &sendReply)
				if err != nil {
					log.Printf("Errore durante la chiamata RPC per l'invio dei dati al Worker %d: %v", otherID, err)
					return
				}
				fmt.Printf("Worker %d ha inviato i dati a Worker %d: %v\n", w.WorkerID, otherID, tempPairs)
			}
		}(otherID, otherRange)
	}

	// Aspetta che tutti i dati siano stati inviati
	wg.Wait()

	// Stampa i risultati finali per il Worker
	fmt.Printf("Worker %d ha completato la fase di riduzione con i dati finali: %v\n", w.WorkerID, w.Intermediate)
	reply.Ack = "Fase di riduzione completata"
	return nil
}

func (w *Worker) ReceiveData(args *utils.WorkerArgs, reply *utils.WorkerReply) error {
	fmt.Printf("Worker %d ha ricevuto dati per la riduzione: %v\n", w.WorkerID, args.Job)

	// Aggiunge le coppie chiave-valore ricevute alla propria mappa
	w.mu.Lock()
	defer w.mu.Unlock()
	for key, value := range args.Job {
		if isInRange(key, w.WorkerRanges[w.WorkerID]) {
			w.Intermediate[key] += value
		}
	}

	// Stampa i dati ricevuti
	fmt.Printf("Worker %d, dati dopo l'integrazione: %v\n", w.WorkerID, w.Intermediate)
	reply.Ack = "Dati ricevuti e integrati"
	return nil
}

// Verifica se una chiave è nel range specificato
func isInRange(key int32, rangeList []int32) bool {
	for _, val := range rangeList {
		if key == val {
			return true
		}
	}
	return false
}

func main() {
	id := flag.Int("ID", 0, "ID del Worker")
	port := flag.Int("port", 5000, "Porta base per il Worker")
	flag.Parse()

	if *id <= 0 {
		fmt.Println("L'ID deve essere maggiore di 0")
		os.Exit(1)
	}

	address := fmt.Sprintf("127.0.0.1:%d", *port+*id)
	fmt.Printf("Avvio Worker %d su %s\n", *id, address)

	worker := new(Worker)
	server := rpc.NewServer()
	err := server.Register(worker)
	if err != nil {
		log.Fatalf("Errore durante la registrazione del Worker %d: %v", *id, err)
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Errore durante l'ascolto del Worker %d su %s: %v", *id, address, err)
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
