FROM golang:1.23.4

WORKDIR /app

# Copia il file go.mod e scarica le dipendenze
COPY go.mod ./
RUN go mod download

# Copia tutto il codice sorgente nella directory del container
COPY . .

RUN chmod +x client

# Compila il file master.go
RUN go build -v -o client client/client.go

# Comando per avviare il Master
CMD ["./client/client"]


