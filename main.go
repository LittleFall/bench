package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"sync"
)

func query(db *sql.DB, statement string, except int32, wg *sync.WaitGroup) {
	defer wg.Done()

	for i := 0; i < 100; i++ {
		//startTime := time.Now()
		rows := db.QueryRow(statement)
		//elapsedTime := time.Since(startTime) / time.Millisecond

		var res int32
		err := rows.Scan(&res)
		if err != nil {
			fmt.Println("scan failed:", err)
			return
		}
		if res != except {
			fmt.Printf("result not match, except %v, but got %v\n", except, res)
		} else {
			//fmt.Printf("result match, except %v, got %v\n", except, res)
		}
	}

}

func main() {
	// TODO: config
	server := "172.16.4.42:23300"
	dbName := "tpch10"
	parallel := 100
	statement := "select count(*) from part where p_name like '%green%almond%' and p_name like '%read%apple%' and p_name not like '%yellow%man%' and p_name like '%still%water%' and p_name not like '%ishar%mla%' and P_SIZE not in (11, 45, 14);"
	var res int32 = 0

	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(%v)/%v?charset=utf8&parseTime=True&tidb_allow_mpp=false&tidb_allow_batch_cop=false", server, dbName))
	if err != nil {
		fmt.Println("database open failed:", err)
		return
	} else {
		fmt.Println("database open suc")
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
