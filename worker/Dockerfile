FROM golang:1.23.4

WORKDIR /app

# Copia il file go.mod e scarica le dipendenze
COPY go.mod ./
RUN go mod download

# Copia tutto il codice sorgente nella directory del container
COPY . .

RUN chmod +x worker

# Compila il file master.go
RUN go build -v -o worker worker/worker.go

# Comando per avviare il Master
CMD ["./worker/worker"]


