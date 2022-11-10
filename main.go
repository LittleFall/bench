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
		log.Info("query failed", zap.Error(err))
		return
	}
	if res != except {
		log.Debug("query result not match", zap.Int32("res", res), zap.Int32("except", except))
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
	//db.SetMaxOpenConns(10)
	//db.SetMaxIdleConns(10)

	return db
}

func main() {
	err := initLogger()
	if err != nil {
		fmt.Println("init logger failed", err)
		return
	}

	// TODO: config
	server := "127.0.0.1:4000"
	dbName := "test"
	parallel := 40
	cycle := 1 * time.Second
	//statement := "select * from t"
	statement := "select a+b from p"
	var res int32 = 2

	db := OpenDatabase(server, dbName)

	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			log.Error("failed to close db", zap.Error(err))
		}
	}(db)

	for i := 0; i < parallel; i++ {
		go func() {
			ticker := time.NewTicker(cycle)
			go query(db, statement, res)
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
				zap.Int32("totalReturned", atomic.LoadInt32(&totalReturned)),
				zap.Int32("queued", atomic.LoadInt32(&totalExecuted)-atomic.LoadInt32(&totalReturned)))
		}
	}()

	sig := waitForSigterm()
	log.Info("received signal", zap.String("sig", sig.String()))
}
