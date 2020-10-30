package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"io/ioutil"
	"log"
	"os"
	"time"
)

type payload struct {
	Counter int `json:"counter"`
	Timestamp int64 `json:"time_stamp"`
}
type payloads struct {
	payloads []payload `json:"payloads"`
}


func connecttodb () (connection *sql.DB) {
	dburi :=os.Getenv("DBURI")
	connection, err:= sql.Open("mysql", dburi)
	if err !=nil {
		log.Println("Unable to connect to db", err)
		os.Exit(3)
	}

	log.Println("Succesfully connected")
	connection.SetMaxOpenConns(100)
	connection.SetMaxIdleConns(20)
	connection.SetConnMaxIdleTime(time.Second*10)
	return connection
}


func main ()  {
	_ = godotenv.Load()
	position := 0
	db := connecttodb()


	for {
		var oneHundredRecords = []payload{}
		var oneRecord payload

		log.Println("Position : ", position)
		query := "SELECT counter, time_stamp FROM q LIMIT ?, 100"

		rows, err := db.Query(query, position)
		if err != nil {
			log.Println("Unable to read from db because ", err)
			break
		}

		for rows.Next() {
			if err := rows.Scan(&oneRecord.Counter, &oneRecord.Timestamp); err != nil {
				continue
			}
			oneHundredRecords = append(oneHundredRecords, oneRecord)
		}
		if len(oneHundredRecords) <= 0 {
			log.Println("We have reached the end")
			break
		}
		log.Println("Read ", len(oneHundredRecords), " records")
		saveToJson(position, oneHundredRecords)

		position += 100
		time.Sleep(100 *time.Millisecond)
	}

}

func saveToJson(position int, data []payload) {
	fileName := fmt.Sprintf("%d.json", position)
	log.Println("Saving data to ", fileName)

	jsonFile, err := os.Open("payloads.json")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Successfully Opened payloads.json")

	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

		var payload payloads
	var payloads map[string]interface{}
	json.Unmarshal([]byte(byteValue), &payload)

	fmt.Println(payloads["we here"])

	for i:=0; i<len(payload.payloads); i++ {
		fmt.Println("Timestamp: ", payload.payloads[i].Timestamp)
		fmt.Println("counter: ", payload.payloads[i].Counter)
	}
	}
