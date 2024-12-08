package utils

// Argomenti per l'RPC Master-to-Worker (fase di mappatura)
type WorkerArgs struct {
	Job          map[int32]int32
	WorkerID     int
	WorkerRanges map[int][]int32 // Mappa dei range di lavoro degli altri Worker
}

// Risposta per l'RPC Master-to-Worker (fase di mappatura)
type WorkerReply struct {
	Ack string
}

// Argomenti per l'RPC Worker-to-Worker (fase di riduzione)
type ReduceArgs struct{}

// Risposta per l'RPC Worker-to-Worker (fase di riduzione)
type ReduceReply struct {
	Ack string
}

// Argomenti per l'RPC Client-to-Master (fase di ricezione dei dati)
type ClientArgs struct {
	Data []int32
}

// Risposta per l'RPC Client-to-Master (fase di ricezione dei dati)
type ClientReply struct {
	Ack string
}
