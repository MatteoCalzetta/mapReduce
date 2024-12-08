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

// Struct per il Worker RPC
type Worker struct {
	WorkerID     int
	WorkerRanges map[int][]int32 // Mappa dei range di lavoro di tutti i Worker
	Intermediate map[int32]int32 // Mappa per le coppie chiave-valore intermediari
	// Canale per notificare la conclusione della fase di riduzione
	ackChan chan int
	wg      sync.WaitGroup // WaitGroup per la sincronizzazione
}

// Funzione per processare i job ricevuti dal Master (fase di mappatura)
func (w *Worker) ProcessJob(args *utils.WorkerArgs, reply *utils.WorkerReply) error {
	w.WorkerID = args.WorkerID
	w.WorkerRanges = args.WorkerRanges
	w.Intermediate = make(map[int32]int32)

	fmt.Printf("Worker %d ricevuto job: %v\n", w.WorkerID, args.Job)

	// Calcola la coppia chiave-valore per il proprio range di dati
	for key, value := range args.Job {
		w.Intermediate[key] += value
	}

	// Mostra i range degli altri Worker per il debug
	fmt.Printf("Worker %d, range degli altri Worker: %v\n", w.WorkerID, w.WorkerRanges)

	reply.Ack = fmt.Sprintf("Job completato con %d valori unici", len(w.Intermediate))
	fmt.Printf("Worker %d completato job: %v\n", w.WorkerID, w.Intermediate)
	return nil
}

// Funzione per avviare la fase di riduzione e scambio di dati tra Worker
func (w *Worker) ReduceJob(args *utils.ReduceArgs, reply *utils.ReduceReply) error {
	fmt.Printf("Worker %d avvia la fase di riduzione\n", w.WorkerID)
	var wg sync.WaitGroup

	// Inizializza il canale per ricevere gli ack
	w.ackChan = make(chan int, len(w.WorkerRanges)-1)

	// Itera su tutti gli altri Worker e invia le coppie chiave-valore non presenti nel proprio range
	for otherID, otherRange := range w.WorkerRanges {
		if otherID == w.WorkerID {
			continue
		}

		wg.Add(1)
		go func(otherID int, otherRange []int32) {
			defer wg.Done()

			// Connessione al Worker di destinazione
			otherWorkerAddr := fmt.Sprintf("127.0.0.1:%d", 5000+otherID)
			client, err := rpc.Dial("tcp", otherWorkerAddr)
			if err != nil {
				log.Printf("Errore nella connessione al Worker %d: %v", otherID, err)
				return
			}
			defer client.Close()

			// Crea una mappa temporanea per le coppie chiave-valore da inviare
			tempPairs := make(map[int32]int32)
			for key, value := range w.Intermediate {
				if !contains(otherRange, key) {
					// Aggiungi solo le coppie chiave-valore che non fanno parte del proprio range
					tempPairs[key] += value
				}
			}

			// Invia solo le coppie chiave-valore che non sono nel proprio range
			if len(tempPairs) > 0 {
				sendArgs := utils.WorkerArgs{
					Job:          tempPairs,
					WorkerID:     w.WorkerID,
					WorkerRanges: w.WorkerRanges,
				}
				var sendReply utils.WorkerReply
				err = client.Call("Worker.ReceiveData", sendArgs, &sendReply)
				if err != nil {
					log.Printf("Errore durante la chiamata RPC per l'invio dei dati al Worker %d: %v", otherID, err)
					return
				}

				fmt.Printf("Worker %d ha inviato le coppie chiave-valore a Worker %d: %v\n", w.WorkerID, otherID, tempPairs)
			}

			// Notifica l'ack della fase di invio completata
			w.ackChan <- otherID
		}(otherID, otherRange)
	}

	// Aspetta che tutti i dati siano stati scambiati
	wg.Wait()

	// Aspetta di ricevere un ack da ciascun Worker
	expectedAcks := len(w.WorkerRanges) - 1
	receivedAcks := 0
	for receivedAcks < expectedAcks {
		<-w.ackChan
		receivedAcks++
	}

	// Stampa i risultati finali per il Worker
	fmt.Printf("Worker %d ha completato la fase di riduzione con i dati finali: %v\n", w.WorkerID, w.Intermediate)
	reply.Ack = "Fase di riduzione completata"
	return nil
}

// Funzione per ricevere i dati da un altro Worker (fase di riduzione)
func (w *Worker) ReceiveData(args *utils.WorkerArgs, reply *utils.WorkerReply) error {
	fmt.Printf("Worker %d ha ricevuto dati da Worker %d: %v\n", w.WorkerID, args.WorkerID, args.Job)

	// Aggiunge le coppie chiave-valore ricevute alla propria mappa
	for key, value := range args.Job {
		w.Intermediate[key] += value
	}

	// Stampa i dati ricevuti
	fmt.Printf("Worker %d, dati dopo l'integrazione: %v\n", w.WorkerID, w.Intermediate)
	reply.Ack = "Dati ricevuti e integrati"
	return nil
}

// Funzione per verificare se una chiave è presente in un array di int32
func contains(arr []int32, key int32) bool {
	for _, val := range arr {
		if val == key {
			return true
		}
	}
	return false
}

func main() {
	// Leggi l'ID e la porta base da linea di comando
	id := flag.Int("ID", 0, "ID del Worker")
	port := flag.Int("port", 5000, "Porta base per il Worker")
	flag.Parse()

	if *id <= 0 {
		fmt.Println("L'ID deve essere maggiore di 0")
		os.Exit(1)
	}

	// Calcola l'indirizzo del Worker basato sull'ID e la porta base
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
