package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var db *gorm.DB

func init() {

	var err error
	db, err = gorm.Open(postgres.Open(
		"host=localhost user=postgres password=1234567890 dbname=postgres sslmode=disable port=5432"),
		&gorm.Config{Logger: logger.New(
			log.New(os.Stdout, "\r\n", log.LstdFlags),
			logger.Config{SlowThreshold: time.Second},
		)},
	)
	if err != nil {
		log.Fatalf("failed to connect database: %s", err.Error())
	}

	err = db.Exec("CREATE TABLE IF NOT EXISTS t (id SERIAL PRIMARY KEY, name VARCHAR(64) UNIQUE, num INT);").Error
	if err != nil {
		log.Fatalf("create table error: %s", err.Error())
	}

	// default num 777
	err = db.Exec("INSERT INTO t(name, num) VALUES ('ian', 777) ON CONFLICT(name) DO UPDATE SET num = 777;").Error
	if err != nil {
		log.Fatalf("insert table error: %s", err.Error())
	}

	r := gin.New()

	// curl http://127.0.0.1:8080/idle_in_transaction_timeout/
	r.GET("/idle_in_transaction_timeout/", idleInTransactionTimeout)

	// curl http://127.0.0.1:8080/lock_timeout/
	r.GET("/lock_timeout/", lockTimeout)

	// curl http://127.0.0.1:8080/statement_timeout/
	r.GET("/statement_timeout/", statementTimeout)

	srv := &http.Server{Addr: ":8080", Handler: r}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen and serve error: %s", err.Error())
	}
}

// curl http://127.0.0.1:8080/idle_in_transaction_timeout/
func idleInTransactionTimeout(c *gin.Context) {
	concurrency(2, func(tx *gorm.DB, i int) error {
		if err := tx.Exec("SET idle_in_transaction_session_timeout=1500;").Error; err != nil {
			return errors.Wrap(err, strconv.FormatInt(int64(i), 32)+": set lock timeout")
		}
		// 一定拿得到 Lock (別人拿 Lock 太久會強制釋出)
		if err := tx.Exec("UPDATE t SET num=? WHERE name='ian';", i).Error; err != nil {
			return errors.Wrap(err, strconv.FormatInt(int64(i), 32)+": update")
		}
		fmt.Printf("%d: got lock and updated\n", i)

		// 如果不用 time.Sleep，而是用 SELECT pg_sleep()，不會受 idle_in_transaction_session_timeout 影響而 commit 失敗
		// if err := tx.Exec("SELECT pg_sleep(3);").Error; err != nil {
		// 	return errors.Wrap(err, strconv.FormatInt(int64(i), 32)+": sleep")
		// }
		if i%2 == 0 {
			time.Sleep(1 * time.Second)
		} else {
			// 佔用 Lock 太久，到時 Commit 會有 FATAL: terminating connection due to idle-in-transaction timeout (SQLSTATE 25P03)
			time.Sleep(2 * time.Second)
		}

		fmt.Printf("%d: finish process\n", i)
		return nil
	})
	c.JSON(http.StatusOK, "")
}

// curl http://127.0.0.1:8080/lock_timeout/
func lockTimeout(c *gin.Context) {
	concurrency(2, func(tx *gorm.DB, i int) error {
		if err := tx.Exec("SET lock_timeout=1500;").Error; err != nil {
			return errors.Wrap(err, "set lock timeout")
		}
		// 太久拿不到 Lock，這裡會被放棄執行，然後 ERROR: canceling statement due to lock timeout (SQLSTATE 55P03)
		if err := tx.Exec("UPDATE t SET num=? WHERE name='ian';", i).Error; err != nil {
			return errors.Wrap(err, strconv.FormatInt(int64(i), 32)+": update")
		}
		fmt.Printf("%d: got lock and updated\n", i)
		time.Sleep(2 * time.Second)

		fmt.Printf("%d: finish process\n", i)
		return nil
	})

	c.JSON(http.StatusOK, "")
}

// curl http://127.0.0.1:8080/statement_timeout/
func statementTimeout(c *gin.Context) {
	concurrency(2, func(tx *gorm.DB, i int) error {
		if err := tx.Exec("SET statement_timeout=1500;").Error; err != nil {
			return errors.Wrap(err, "set lock timeout")
		}
		// 模擬某個 SELECT 太久
		if err := tx.Exec("SELECT pg_sleep(3);").Error; err != nil {
			return errors.Wrap(err, strconv.FormatInt(int64(i), 32)+": sleep")
		}

		fmt.Printf("%d: finish process\n", i)
		return nil
	})

	c.JSON(http.StatusOK, "")
}

func main() {

}

func concurrency(count int, fn func(tx *gorm.DB, i int) error) {
	var wg sync.WaitGroup
	for i := 1; i <= count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tx := db.Begin()

			if err := fn(tx, i); err != nil {
				tx.Rollback()
				fmt.Printf("%d: process has err: %s\n", i, err.Error())
				return
			}

			if err := tx.Commit().Error; err != nil {
				tx.Rollback()
				fmt.Printf("%d: commit err: %s\n", i, err.Error())
			}
			fmt.Printf("%d: done\n", i)
		}(i)
	}
	wg.Wait()
}
