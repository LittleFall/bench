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
var totalError int32

func query(db *sql.DB, statement string, except int32) {
	atomic.AddInt32(&totalExecuted, 1)

	//startTime := time.Now()
	log.Debug("will query", zap.String("statement", statement))

	_ = db.QueryRow(statement)
	//elapsedTime := time.Since(startTime) / time.Millisecond

	atomic.AddInt32(&totalReturned, 1)
	//var res int32
	//err := rows.Scan(&res)
	//log.Debug("query done", zap.Int32("res", res), zap.Error(err))
	//if err != nil {
	//	atomic.AddInt32(&totalError, 1)
	//	log.Info("query failed", zap.Error(err))
	//	return
	//}
	//if res != except {
	//	atomic.AddInt32(&totalError, 1)
	//	log.Debug("query result not match", zap.Int32("res", res), zap.Int32("except", except))
	//} else {
	//
	//}
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
	server := "127.0.0.1:23500"
	dbName := "test"
	parallel := 100
	cnt := 40
	cycle := 1 * time.Second
	//statement := "select * from t"
	statement := "explain analyze select nested.primary_key, nested.secondary_key, nested.timestamp, nested.value, nested. rowAlias from ( select primary_key, secondary_key, timestamp, value, row_number() over (partition by primary_key, secondary_key order by timestamp desc) as rowAlias from test where (primary_key = 0x32 and secondary_key >= 0x31) ) as nested where rowAlias <= 10 order by secondary_key desc;"
	var res int32 = 2

	db := OpenDatabase(server, dbName)

	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			log.Error("failed to close db", zap.Error(err))
		}
	}(db)

	closeChan := make(chan int)

	for i := 0; i < parallel; i++ {
		go func(ch <-chan int) {
			ticker := time.NewTicker(time.Second)
			for {
				select {
				case <-ch:
					return
				case <-ticker.C:
					for i := 0; i < cnt; i++ {
						query(db, statement, res)
					}
				}
			}

		}(closeChan)
	}

	go func(ch <-chan int) {
		ticker := time.NewTicker(cycle)

		closed := false
		for {
			select {
			case <-ch:
				closed = true
			case <-ticker.C:
				log.Info("query status",
					zap.Int32("totalExecuted", atomic.LoadInt32(&totalExecuted)),
					zap.Int32("totalReturned", atomic.LoadInt32(&totalReturned)),
					zap.Int32("queued", atomic.LoadInt32(&totalExecuted)-atomic.LoadInt32(&totalReturned)),
					zap.Int32("totalSuccess", atomic.LoadInt32(&totalReturned)-atomic.LoadInt32(&totalError)),
					zap.Int32("totalError", atomic.LoadInt32(&totalError)))
				if closed && atomic.LoadInt32(&totalExecuted) == atomic.LoadInt32(&totalReturned) {
					log.Info("all query done")
					os.Exit(0)
				}
			}
		}
	}(closeChan)

	sig := waitForSigterm()
	log.Info("received signal", zap.String("sig", sig.String()))
	log.Warn("stop send new queries, waiting for queued query, press ctrl+c again to exit")
	close(closeChan)
	sig = waitForSigterm()
	log.Info("received signal", zap.String("sig", sig.String()))
}
