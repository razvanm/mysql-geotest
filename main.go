package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	dsn         = flag.String("dsn", "root@unix(/var/lib/mysql/mysql.sock)/geotest", "MySQL data source name")
	tableName   = flag.String("table_name", "geotest", "Name of test table")
	tableSize   = flag.Uint64("table_size", 10000, "Number of records in test table")
	tableEngine = flag.String("table_engine", "MyISAM", "MySQL storage engine")
	xMin        = flag.Float64("x_min", -10000, "Min x")
	xMax        = flag.Float64("x_max", 10000, "Max x")
	yMin        = flag.Float64("y_min", -10000, "Min y")
	yMax        = flag.Float64("y_max", 10000, "Max y")
	radius      = flag.Float64("radius", 300, "Radius")
	minPoints   = flag.Int("min_points", 3, "Min number of points in a polygon")
	maxPoints   = flag.Int("max_points", 20, "Max number of points in a polygon")

	// Run flags.
	maxTime = flag.Duration("max_time", 1*time.Minute, "How long to run the test")
	//maxRequests = flag.Uint64("max_requets", 0, "How many requests to do")
	numThreads = flag.Int("num_threads", 1, "How many threads to use")

	// Debug flags.
	printSegments = flag.Bool("print_segments", false, "Print the segments of the polygons")
)

func exec(db *sql.DB, query string) (int64, int64, error) {
	r, err := db.Exec(query)
	if err != nil {
		return 0, 0, err
	}
	last, err := r.LastInsertId()
	if err != nil {
		log.Panic(err)
	}
	rows, err := r.RowsAffected()
	if err != nil {
		log.Panic(err)
	}
	return last, rows, nil
}

func numRows(db *sql.DB) uint64 {
	var rows uint64
	q := fmt.Sprintf("SELECT COUNT(*) rows FROM %s", *tableName)
	err := db.QueryRow(q).Scan(&rows)
	if err != nil {
		log.Panic(err)
	}
	return rows
}

func prepare(db *sql.DB) {
	q := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
       id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,
       poly POLYGON NOT NULL,
       c CHAR(120) DEFAULT '' NOT NULL,
       pad CHAR(60) DEFAULT '' NOT NULL,
       PRIMARY KEY(id),
       SPATIAL INDEX(poly)
) ENGINE=%s`, *tableName, *tableEngine)
	_, _, err := exec(db, q)
	if err != nil {
		log.Panic(err)
	}

	start := numRows(db)
	log.Printf("Start number of rows: %v", start)
	for i := start; i < *tableSize; i++ {
		x := *xMin + rand.Float64()*(*xMax-*xMin)
		y := *yMin + rand.Float64()*(*yMax-*yMin)
		d := rand.Float64() * *radius
		n := *minPoints + rand.Intn(*maxPoints-*minPoints)
		angles := make([]float64, n)
		for j := 0; j < n; j++ {
			angles[j] = rand.Float64() * 2 * math.Pi
		}
		sort.Float64s(angles)
		vertices := make([]string, n)
		for j := 0; j < n; j++ {
			vertices[j] = fmt.Sprintf("%v %v", x+d*math.Sin(angles[j]), y+d*math.Cos(angles[j]))
		}

		if *printSegments {
			for j := 1; j < n; j++ {
				fmt.Printf("POLY: %v %v\n", vertices[j-1], vertices[j])
			}
			fmt.Printf("POLY: %v %v\n", vertices[n-1], vertices[0])
		}

		polygon := fmt.Sprintf("POLYGON((%s, %s))", strings.Join(vertices, ","), vertices[0])
		exec(db, fmt.Sprintf("INSERT INTO %s(poly) VALUES (GeomFromText('%s'))", *tableName, polygon))
	}

	log.Printf("End number of rows: %v", numRows(db))
}

func oneRun(db *sql.DB, done chan int, stop chan bool) {
	log.Println("Start oneRun")
	q := fmt.Sprintf("SELECT COUNT(*) count FROM %s WHERE Contains(poly, Point(?,?))", *tableName)
	reqs := 0
	hits := 0
	for {
		select {
		case <-stop:
			log.Printf("Done %v requests, %v hits", reqs, hits)
			done <- reqs
			return
		default:
		}
		x := *xMin + rand.Float64()*(*xMax-*xMin)
		y := *yMin + rand.Float64()*(*yMax-*yMin)
		var count int
		err := db.QueryRow(q, x, y).Scan(&count)
		hits += count
		if err != nil {
			log.Panic(err)
		}
		reqs += 1
	}
}

func run(db *sql.DB) {
	done := make(chan int, *numThreads)
	stop := make(chan bool, *numThreads)
	for i := 0; i < *numThreads; i++ {
		go oneRun(db, done, stop)
	}
	log.Printf("Sleep %v", *maxTime)
	time.Sleep(*maxTime)
	for i := 0; i < *numThreads; i++ {
		stop <- true
	}
	total := 0
	for i := 0; i < *numThreads; i++ {
		total = total + <-done
	}
	log.Printf("Total requests: %v/s", float64(total)/maxTime.Seconds())
}

func main() {
	flag.Parse()

	if len(flag.Args()) != 1 {
		fmt.Printf("Usage: %s [options] prepare|run|cleanup]\n", os.Args[0])
		return
	}

	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	switch flag.Arg(0) {
	case "prepare":
		prepare(db)
	case "run":
		run(db)
	case "cleanup":
		exec(db, fmt.Sprintf("DROP TABLE `%s`", *tableName))
	}
}
