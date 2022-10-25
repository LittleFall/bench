package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

var totalExecuted int32
var totalReturned int32

func query(db *sql.DB, statement string, except int32) {
	atomic.AddInt32(&totalExecuted, 1)

	//startTime := time.Now()
	log.Debug("will query", zap.String("statement", statement))

	rows := db.QueryRow(statement)
	//elapsedTime := time.Since(startTime) / time.Millisecond

	atomic.AddInt32(&totalReturned, 1)
	var res int32
	err := rows.Scan(&res)
	log.Debug("query done", zap.Int32("res", res), zap.Error(err))
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

func waitForSigterm() os.Signal {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	for {
		sig := <-ch
		if sig == syscall.SIGHUP {
			// Prevent from the program stop on SIGHUP
			continue
		}
		return sig
	}
}

func initLogger() error {
	logCfg := &log.Config{
		Level: "info",
		File:  log.FileLogConfig{Filename: ""},
	}

	logger, p, err := log.InitLogger(logCfg)
	if err != nil {
		return err
	}
	log.ReplaceGlobals(logger, p)
	return nil
}

func OpenDatabase(endpoint string, dbName string) *sql.DB {
	now := time.Now()
	log.Info("setting up database")
	defer func() {
		log.Info("init database done", zap.Duration("in", time.Since(now)))
	}()

	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(%s)/%s", endpoint, dbName))
	{
		if err != nil {
			log.Fatal("failed to open db", zap.Error(err))
		}
		sqlCtx, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()
		err = db.PingContext(sqlCtx)
		if err != nil {
			log.Fatal("failed to open db", zap.Error(err))
		}
	}
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	return db
}

func main() {
	err := initLogger()
	if err != nil {
		fmt.Println("init logger failed", err)
		return
	}

	// TODO: config
	server := "172.16.4.42:23300"
	dbName := "tpch10"
	parallel := 100
	cycle := 1 * time.Second

	db := OpenDatabase(server, dbName)

	statement := "select count(*) from part where p_name like '%green%almond%' and p_name like '%read%apple%' and p_name not like '%yellow%man%' and p_name like '%still%water%' and p_name not like '%ishar%mla%' and P_SIZE not in (11, 45, 14);"
	var res int32 = 0

	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			fmt.Println("database close failed:", err)
		}
	}(db)

	for i := 0; i < parallel; i++ {
		go func() {
			ticker := time.NewTicker(cycle)
			query(db, statement, res)
			for _ = range ticker.C {
				go query(db, statement, res)
			}
		}()
	}

	go func() {
		ticker := time.NewTicker(cycle)
		for _ = range ticker.C {
			log.Info("query status",
				zap.Int32("totalExecuted", atomic.LoadInt32(&totalExecuted)),
				zap.Int32("totalReturned", atomic.LoadInt32(&totalReturned)))
		}
	}()

	sig := waitForSigterm()
	log.Info("received signal", zap.String("sig", sig.String()))
}
