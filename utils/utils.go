package utils

import "log"

// Richieste dal Client al Master
type ClientArgs struct {
	Data []int32
}

// Risposte dal Master al Client
type ClientReply struct {
	Ack string
}

// Richieste dal Master ai Worker
type WorkerArgs struct {
	Job []int32
}

// Risposte dal Worker al Master
type WorkerReply struct {
	Ack string
}

// Funzione per controllare errori
func CheckError(err error) {
	if err != nil {
		log.Fatalf("Errore: %v", err)
	}
}
