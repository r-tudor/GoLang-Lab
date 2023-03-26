package main

import (
    "encoding/json"
    "fmt"
    "time"
    "math"
    "sync"
    "database/sql"
    _ "github.com/lib/pq"
    "github.com/nats-io/nats.go"
)

type Sensor struct {
    Name      string  `json:"name"`
    Timestamp int64   `json:"timestamp"`
    Value     float64  `json:"value"`
}

const (
    host     = "/tmp"
    port     = 5432
    user     = "robert"
    password = "<Hogetoren123>"
    dbname   = "sensor_reads"
)

func main() {
    // init NATS connection
    nc, err := nats.Connect(nats.DefaultURL)
    CheckError(err)

    defer nc.Close()

    // init encoded NATS connection
    ec, err := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
    CheckError(err)

    defer ec.Close()
    // ---------------------------------------------------
    // init db connection
    psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

    // open database
    db, err := sql.Open("postgres", psqlconn)
    CheckError(err)

    // close database
    defer db.Close()

    // check db
    err = db.Ping()
    CheckError(err)

    fmt.Println("Connected to PostgreSQL Database!")
    // ---------------------------------------------------

    wg := sync.WaitGroup{}
    wg.Add(1)

    // subscribe to NATS channel
    nc.Subscribe("SensorReadings", func(m *nats.Msg) {
        var data []Sensor
        err := json.Unmarshal(m.Data, &data)
        CheckError(err)

        // send received readings to calc avg
        avg := calcAverage( data )

        // Insert Average into DB
        insertIntoDb := `insert into "avg_reads"("created_on", "value") values($1, $2)`
            _, e := db.Exec(insertIntoDb, time.Now().UnixMilli(), avg)
        fmt.Println("Inserting AVERAGE READING-> ", avg)
        CheckError(e)

        // Insert SensorReadings into DB
        for _, readingItem := range data {
            insertIntoDb := `insert into "sensor_reads"("sensor_name", "read_on", "value") values($1, $2, $3)`
                _, e := db.Exec(insertIntoDb, readingItem.Name, readingItem.Timestamp, readingItem.Value)
            fmt.Println("Inserting READING -> ", readingItem)
            CheckError(e)
        }

        fmt.Println("---------------------\n")
    })

    wg.Wait()
}

// calculate sensor value average
func calcAverage(data []Sensor) float64 {
	sum := 0.0
	for _, readingItem := range data {
		sum += readingItem.Value
	}
	return roundFloat(sum / float64(len(data)), 2)
}

// shorten the readings to 2 decimals
func roundFloat(val float64, precision uint) float64 {
    ratio := math.Pow(10, float64(precision))
    return math.Round(val*ratio) / ratio
}

// handle errors
func CheckError(err error) {
    if err != nil {
        panic(err)
    }
}

