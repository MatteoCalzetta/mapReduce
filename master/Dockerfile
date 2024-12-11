FROM golang:1.23.4

WORKDIR /app

# Copia il file go.mod e scarica le dipendenze
COPY go.mod ./
RUN go mod download

# Copia tutto il codice sorgente nella directory del container
COPY . .

RUN chmod +x master

# Compila il file master.go
RUN go build -v -o master master/master.go

# Comando per avviare il Master
CMD ["./master/master"]

# Espone la porta necessaria
EXPOSE 8080

