package main

import (
	"os"
	"fmt"
	"log"
	"time"
	"encoding/json"
	"context"
	// "github.com/n.holmstedt/nats-scheduler/common"
	"github.com/go-co-op/gocron"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// // '{"cronsecond":"*/1 * * * * *", "payload":{"foo":"bar"}}'
type scheduledJob struct {
	CronSecond 	string			 		`json:"cronsecond"`
	Payload 	map[string]interface{} 	`json:"payload"`
}

func main() {
	ctx := context.Background()
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}
	nc, _ := nats.Connect(url)
	defer nc.Drain()
	js, _ := jetstream.New(nc)
	kv, _ := js.KeyValue(ctx, "foo")
	task(ctx, kv, nc)
} 

func addJob()  {
	
}

func task(ctx context.Context, kv jetstream.KeyValue, nc *nats.Conn) {
	s := gocron.NewScheduler(time.UTC)
	s.TagsUnique()
	fmt.Println("starting")
	w, err := kv.Watch(ctx, "*")
	defer w.Stop()
	if err != nil {
		log.Fatalln("error scheduling job", err)
		return
	}	
	s.StartAsync()
	for {                                              
        select {                                                                 
        case <-ctx.Done():
            return
		case kve := <-w.Updates():
			if kve != nil {
				switch kve.Operation() {
				case 0:
					job := scheduledJob{}
					err := json.Unmarshal(kve.Value(), &job)

					if err != nil {
						log.Fatalln("error unmarshaling job", err)
					}

					_, err = s.CronWithSeconds(job.CronSecond).Tag(string(kve.Key())).Do(func() {
						
						b, _ := json.Marshal(job.Payload)
						log.Println(string(b))

						nc.Publish("scheduled", kve.Value())
					})
					// Should update when "PUT" updates.
					if err != nil {
						log.Println("error adding job", err)
					}
				case 1:
					s.RemoveByTag(kve.Key())
				}
				//Add case for update
				//Add case for purge
				fmt.Printf("%s @ %d -> %q (op: %s)\n", kve.Key(), kve.Revision(), string(kve.Value()), kve.Operation())
				

			}


        }
    }   
}