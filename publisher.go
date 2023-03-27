package main

import (
    "encoding/json"
    "fmt"
    "math"
    "math/rand"
    "time"

    "github.com/nats-io/nats.go"
)

type Sensor struct {
    Name      string  `json:"name"`
    Timestamp int64   `json:"timestamp"`
    Value     float64 `json:"value"`
}

// create feed
func main() {
    // set interval time
    const sleepDuration = 2 * time.Second

    // init NATS connection
    nc, err := nats.Connect(nats.DefaultURL)
    CheckError(err)

    defer nc.Close()

    // init encoded NATS connection
    ec, err := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
    CheckError(err)

    defer ec.Close()

    fmt.Println("Connected to NATS! Ready to send data!")

    // sensor reading cycle with goroutine
    go func() {
        for {
            now := time.Now().UnixMilli()

            sensors := []Sensor{
                {Name: "Sensor1", Timestamp: now, Value: roundFloat(rand.Float64()*150, 2)},
                {Name: "Sensor2", Timestamp: now, Value: roundFloat(rand.Float64()*150, 2)},
                {Name: "Sensor3", Timestamp: now, Value: roundFloat(rand.Float64()*150, 2)},
            }

            // prepare data for publish
            data, err := json.Marshal(sensors)
            CheckError(err)

            // publish readings to NATS channel
            nc.Publish("SensorReadings", data)

            // sleep for the specified duration
            time.Sleep(sleepDuration)
        }
    }()

    // block the main Goroutine to keep the program running
    select {}
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
