package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

func analyzeTable(db *sql.DB, tableNames []string, wg *sync.WaitGroup) {
	defer wg.Done()

	for _, tableName := range tableNames {

		startTime := time.Now()

		_, err := db.Exec(fmt.Sprintf("ANALYZE %s", tableName))
		if err != nil {
			fmt.Println("Error ananlyzing table %s : %v", tableName, err)
		} else {
			duration := time.Since(startTime)
			fmt.Println("Analyzed table %s in %s", tableName, duration)
		}
	}
}

func main() {
	host := flag.String("host", "localhost", "Database host")
	port := flag.Int("port", 5432, "Database port")
	user := flag.String("user", "postgres", "Database user")
	password := flag.String("password", "", "Database password")
	dbname := flag.String("dbname", "postgres", "Database name")
	flag.Parse()

	//connection string
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=require", *host, *port, *user, *password, *dbname)

	//connect to DB
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	//pull the tables list
	rows, err := db.Query("SELECT table_name from information_schema.tables where table_schema = 'public'")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	/*	//Print tables
		fmt.Println("Tables list")
		for rows.Next() {
			var tableName string
			if err := rows.Scan(&tableName); err != nil {
				log.Fatal(err)
			}
			fmt.Println(tableName)
		}
	*/

	tables := []string{}
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			log.Fatal(err)
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	batchSize := 30
	tableGroups := [][]string{}

	for i := 0; i < len(tables); i += batchSize {
		end := i + batchSize
		if end > len(tables) {
			end = len(tables)
		}
		batch := tables[i:end]
		tableGroups = append(tableGroups, batch)
	}

	for i, batch := range tableGroups {
		fmt.Println("Batch %d: %v", i+1, batch)
		fmt.Println()
	}

	var wg sync.WaitGroup

	for _, batch := range tableGroups {
		wg.Add(1)
		go analyzeTable(db, batch, &wg)
	}

	wg.Wait()

}
