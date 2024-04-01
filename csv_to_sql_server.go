package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

var (
	CONEXAO       = ""
	IP            = ""
	DATABASE      = ""
	TABELA        = ""
	ARQUIVO       = ""
	DELIMITADOR   = ','
	MAX_SQL_CONEX = 100
	USUARIO       = ""
	SENHA         = ""
)

func main() {

	parseArgsDoCMD()
	file, err := os.Open(ARQUIVO)
	if err != nil {
		log.Fatal(err.Error())
	}
	reader := csv.NewReader(file)
	reader.Comma = DELIMITADOR
	reader.LazyQuotes = true
	CONEXAO = "server=" + IP + ";user id=" + USUARIO + ";password=" + SENHA + ";database=" + DATABASE
	db, err := sql.Open("sqlserver", CONEXAO)
	if err != nil {
		log.Fatal(err.Error())
		return
	}

	err = db.Ping()
	if err != nil {
		log.Fatal(err.Error())
		return
	}

	db.SetMaxIdleConns(MAX_SQL_CONEX)
	defer db.Close()

	start := time.Now()

	query := ""
	retorno := make(chan int)
	conexoes := 0
	insercoes := 0
	disponibilidade := make(chan bool, MAX_SQL_CONEX)
	for i := 0; i < MAX_SQL_CONEX; i++ {
		disponibilidade <- true
	}

	// log
	iniciaLog(&insercoes, &conexoes)
	conexaoController(&insercoes, &conexoes, retorno, disponibilidade)

	var x sync.WaitGroup
	id := 1
	primeiraLinha := true

	for {
		linhas, err := reader.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatal(err.Error())
		}

		if primeiraLinha {
			parseColunas(linhas, &query)
			primeiraLinha = false

		} else if <-disponibilidade {
			conexoes += 1
			id += 1
			x.Add(1)
			go insert(id, query, db, retorno, &conexoes, &x, converte_String_Interface(linhas))
		}
	}

	x.Wait()

	decorrido := time.Since(start)
	log.Printf("Status: %d inserções\n", insercoes)
	log.Printf("Tempo de execução: %s\n", decorrido)
}

func parseArgsDoCMD() {
	ip := 			flag.String("ip", IP, "IP de conexão")
	arq := 			flag.String("a", ARQUIVO, "Arquivo CSV.")
	base := 		flag.String("base", DATABASE, "Nome da base SQL Server.")
	tabela := 		flag.String("tabela", TABELA, "Nome da tabela SQL Server.")
	delimitador := 	flag.String("d", string(DELIMITADOR), "Delimitador do csv.")
	max_conex := 	flag.Int("c", MAX_SQL_CONEX, "Número de conexões SQL Server max: 150.")
	usuario := 		flag.String("u", USUARIO, "Usuário SQL Server.")
	senha := 		flag.String("s", SENHA, "Senha SQL Server.")
	flag.Parse()

	ARQUIVO = *arq
	if *tabela == "" {

		if strings.HasSuffix(ARQUIVO, ".csv") {
			TABELA = ARQUIVO[:len(ARQUIVO)-len(".csv")]
		} else {
			TABELA = ARQUIVO
		}

	} else {
		TABELA = *tabela
	}

	DELIMITADOR = []rune(*delimitador)[0]
	MAX_SQL_CONEX = *max_conex
	USUARIO = *usuario
	SENHA = *senha
	DATABASE = *base
	IP = *ip
}
// insere
func insert(id int, query string, db *sql.DB, callback chan<- int, conns *int, concorrencia *sync.WaitGroup, args []interface{}) {
	stmt, err := db.Prepare(query)
	//log.Printf(query)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer stmt.Close()

	_, err = stmt.Exec(args...)
	if err != nil {
		log.Printf("ID: %d (%d conexões), %s\n", id, *conns, err.Error())
	}
	callback <- id
	concorrencia.Done()
}

// finaliza
func conexaoController(inserçoes, qtdConexoes *int, callback <-chan int, disponiveis chan<- bool) {

	go func() {
		for {

			<-callback
			*inserçoes += 1
			*qtdConexoes -= 1
			disponiveis <- true
		}
	}()
}

// log 
func iniciaLog(insertions, connections *int) {
	go func() {
		c := time.Tick(time.Second)
		for {
			<-c
			log.Printf(" - %d inserções, %d conexões\n", *insertions, *connections)
		}
	}()
}

func parseColunas(columns []string, query *string) {

	*query = "INSERT INTO ["+DATABASE+"].dbo.["+TABELA+"] ("
	placeholder := "VALUES ("
	for i, c := range columns {
		if i == 0 {
			*query += " ["+c+"]"
			placeholder += "?"
		} else {
			*query += " ,\n["+c+"]"
			placeholder += ",\n ?"
		}
	} 
	placeholder += ")"
	*query += ") " + placeholder 
}

func converte_String_Interface(s []string) []interface{} {
	i := make([]interface{}, len(s))
	for k, v := range s {
		i[k] = v
	}
	return i
}
