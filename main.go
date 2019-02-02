package main

import (
	"encoding/json"
	"fmt"
	"github.com/gocql/gocql"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
)

const scheme = "http"
var generatorHost = ""
var storageHost = ""
const storageKeyspace = "apptenminutes"
const port = "8080"
const batchSize = 500
const apiKey = "SECRET42"

type info struct {
	Users []user `json:"users"`
}

type user struct {
	Id string `json:"id"`
	Name string `json:"name"`
	Email string `json:"email"`
	Country string `json:"country"`
	Company string `json:"company"`
	Position string `json:"position"`
}

func main() {
	fmt.Println(" ------------- Start -------------")

	// discover services from environment
	if generator := os.Getenv("GENERATOR_HOST"); generator != "" {
		generatorHost = generator
	}

	if storage := os.Getenv("STORAGE_HOST"); storage != "" {
        storageHost = storage
	}
	
    processAndStoreData()
	fmt.Println(" ------------- Finish -------------")
}

func processAndStoreData() {
	url := scheme + "://" + generatorHost + ":" + port + "/users/" + strconv.Itoa(batchSize) + "?token=" + apiKey
	resp, err := http.Get(url)
	defer resp.Body.Close()

	if err != nil {
		panic(err.Error())
	}

	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err.Error())
	}

	var data info
	if err := json.Unmarshal(bytes, &data); err != nil {
		panic(err.Error())
	}

    conn := startConnection()
    i := storeData(data, conn)
    fmt.Printf(" Inserted rows: %d\n", i)
    endConnection(conn)
}

func storeData(data info, session *gocql.Session) int {
	insertedRows := 0
	for _, v := range data.Users {
		query := `INSERT INTO customers (id, name, email, country, company, position) VALUES (?, ?, ?, ?, ?, ?)`
		task := session.Query(query, v.Id, v.Name, v.Email, v.Country, v.Company, v.Position)
		err := task.Exec()
		if err != nil {
			log.Fatal(err)
		}
		insertedRows++
	}

	return insertedRows
}

func startConnection() *gocql.Session {
	cluster := gocql.NewCluster(storageHost)
	cluster.Consistency = gocql.One
	cluster.Keyspace = storageKeyspace

	conn, _ := cluster.CreateSession()
	return conn
}

func endConnection(conn *gocql.Session) {
	conn.Close()
}
