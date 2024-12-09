package utils

// ClientArgs rappresenta gli argomenti inviati dal Client al Master.
type ClientArgs struct {
	Data []int32 // Lista di numeri ricevuti dal Client
}

// ClientReply rappresenta la risposta del Master al Client.
type ClientReply struct {
	Ack string // Messaggio di conferma
}

// WorkerArgs rappresenta gli argomenti inviati dai Worker al Master e viceversa.
type WorkerArgs struct {
	Job          map[int32]int32 // Mappa delle coppie chiave-valore per la mappatura
	WorkerID     int             // ID del Worker
	WorkerRanges map[int][]int32 // Mappa dei range di lavoro di tutti i Worker
}

// WorkerReply rappresenta la risposta di un Worker al Master o a un altro Worker.
type WorkerReply struct {
	Ack string          // Messaggio di conferma
	Job map[int32]int32 // Mappa delle coppie chiave-valore ricevute
}

// ReduceArgs rappresenta gli argomenti per la fase di riduzione dei Worker.
type ReduceArgs struct {
	WorkerID     int             // ID del Worker
	WorkerRanges map[int][]int32 // Mappa dei range di lavoro di tutti i Worker
}

// ReduceReply rappresenta la risposta della fase di riduzione di un Worker.
type ReduceReply struct {
	Ack string // Messaggio di conferma della fase di riduzione completata
}

type WorkerData struct {
	WorkerID int
	Data     map[int32]int32
}
