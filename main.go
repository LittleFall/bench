package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"sync"
)

func query(db *sql.DB, statement string, except int32, wg *sync.WaitGroup) {
	defer wg.Done()

	rows := db.QueryRow("select sleep(1)")

	var res int32
	err := rows.Scan(&res)
	if err != nil {
		return
	}
	if res != except {
		fmt.Printf("result not match, except %v, but got %v\n", except, res)
	} else {
		fmt.Printf("result match, except %v, got %v\n", except, res)
	}
}

func main() {
	// TODO: config
	server := "127.0.0.1:4000"
	dbName := "test"
	parallel := 100
	statement := "select count(*) from t"
	var res int32 = 7

	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(%v)/%v?charset=utf8&parseTime=True", server, dbName))
	if err != nil {
		fmt.Println("database open failed:", err)
		return
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			fmt.Println("database close failed:", err)
		}
	}(db)

	var wg sync.WaitGroup

	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go query(db, statement, res, &wg)
	}
	wg.Wait()
}
