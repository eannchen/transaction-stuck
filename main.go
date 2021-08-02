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

	// curl http://127.0.0.1:8080/gorm_set_max_open_conns/
	r.GET("/gorm_set_max_open_conns/", gormSetMaxOpenConns)

	// curl http://127.0.0.1:8080/gorm_set_max_idle_conns/
	r.GET("/gorm_set_max_idle_conns/", gormSetMaxIdleConns)

	// curl http://127.0.0.1:8080/gorm_conn_max_life_time/
	r.GET("/gorm_conn_max_life_time/", gormSetConnMaxLifeTime)

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

		if err := tx.Exec("UPDATE t SET num=? WHERE name='ian';", i).Error; err != nil {
			return errors.Wrap(err, strconv.FormatInt(int64(i), 32)+": update")
		}
		fmt.Printf("%d: got lock and updated\n", i)

		// tx state 'active'，不會受 idle_in_transaction_session_timeout 影響，不會釋放 connection
		// 需要使用 SET statement_timeout 將 tx 轉為 state 'idle'
		// if err := tx.Exec("SELECT pg_sleep(3);").Error; err != nil {
		// 	return errors.Wrap(err, strconv.FormatInt(int64(i), 32)+": sleep")
		// }

		// tx state 'idle in transaction' 超時，釋放 connection
		// Commit → FATAL: terminating connection due to idle-in-transaction timeout (SQLSTATE 25P03)
		time.Sleep(3 * time.Second)

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
		// 太久拿不到 Lock，這裡會被放棄執行 => tx state 'idle', tx query 'rollback'
		// ERROR: canceling statement due to lock timeout (SQLSTATE 55P03)
		if err := tx.Exec("UPDATE t SET num=? WHERE name='ian';", i).Error; err != nil {
			return errors.Wrap(err, strconv.FormatInt(int64(i), 32)+": update")
		}
		fmt.Printf("%d: got lock and updated\n", i)
		time.Sleep(3 * time.Second)

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
		// tx 'active' 超時 => tx state 'idle', tx query 'rollback'
		// ERROR: canceling statement due to statement timeout (SQLSTATE 57014)
		if err := tx.Exec("SELECT pg_sleep(3);").Error; err != nil {
			return errors.Wrap(err, strconv.FormatInt(int64(i), 32)+": sleep")
		}

		fmt.Printf("%d: finish process\n", i)
		return nil
	})

	c.JSON(http.StatusOK, "")
}

// curl http://127.0.0.1:8080/gorm_set_max_open_conns/
func gormSetMaxOpenConns(c *gin.Context) {
	sqlDB, err := db.DB()
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	sqlDB.SetMaxOpenConns(2)

	concurrency(4, func(tx *gorm.DB, i int) error {
		// 將多出的連線卡在 Begin
		fmt.Printf("%d: start\n", i)
		time.Sleep(5 * time.Second)
		return nil
	})

	sqlDB.SetMaxOpenConns(0) // <= 0 means unlimited
	c.JSON(http.StatusOK, "")
}

// curl http://127.0.0.1:8080/gorm_set_max_idle_conns/
func gormSetMaxIdleConns(c *gin.Context) {
	sqlDB, err := db.DB()
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	sqlDB.SetMaxIdleConns(5)

	fmt.Printf("connection: %d\n", countConnection())

	// 所有 goroutine 執行 commit 完，還會保有連線
	concurrency(5, func(tx *gorm.DB, i int) error { return nil })
	fmt.Printf("connection: %d\n", countConnection())

	// 再執行一次，會拿待機的連線去使用
	concurrency(5, func(tx *gorm.DB, i int) error { return nil })
	fmt.Printf("connection: %d\n", countConnection())

	sqlDB.SetMaxIdleConns(0)
	c.JSON(http.StatusOK, "")
}

// curl http://127.0.0.1:8080/gorm_conn_max_life_time/
func gormSetConnMaxLifeTime(c *gin.Context) {
	sqlDB, err := db.DB()
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}

	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetConnMaxLifetime(2 * time.Second)
	concurrency(10, func(tx *gorm.DB, i int) error { return nil })

	fmt.Printf("connection: %d\n", countConnection())

	// 超過時間後，會釋出待機的連線
	time.Sleep(5 * time.Second)
	fmt.Printf("connection: %d\n", countConnection())

	c.JSON(http.StatusOK, "")
}

func main() {}

func countConnection() (count int) {
	if err := db.Raw("SELECT count(*) FROM pg_stat_activity WHERE usename = 'postgres';").Scan(&count).Error; err != nil {
		log.Fatalf("count connection error: %s", err.Error())
	}
	return
}

func concurrency(count int, fn func(tx *gorm.DB, i int) error) {
	var wg sync.WaitGroup
	for i := 1; i <= count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tx := db.Begin()
			fmt.Printf("%d: start\n", i)

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
