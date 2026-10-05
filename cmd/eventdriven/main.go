// Command eventdriven runs the event-driven saga POC: an in-process pub/sub
// bus with three choreographed participants (Order → Payment → Inventory).
package main

import (
	"log"
	"net/http"

	"github.com/siamosystems/siamo-poc-event-driven/internal/api"
)

func main() {
	s := api.New()
	log.Println("siamo-poc-event-driven listening on :8095")
	log.Println("  POST /saga/start            {\"fail\":\"\" | \"payment\" | \"inventory\"}")
	log.Println("  POST /saga/inject-failure   {\"fail\":\"payment\" | \"inventory\" | \"\"}")
	log.Println("  GET  /events")
	if err := http.ListenAndServe(":8095", s.Handler()); err != nil {
		log.Fatal(err)
	}
}
