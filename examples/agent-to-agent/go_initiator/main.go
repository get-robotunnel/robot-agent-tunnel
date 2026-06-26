// go_initiator: sends a message to the python_responder through two daemon instances.
//
// Prerequisites:
//   - roboatd on /tmp/roboat-initiator.sock (ROBOAT_LISTEN_PORT=11411)
//   - roboatd on /tmp/roboat-responder.sock (ROBOAT_LISTEN_PORT=11412)
//   - python_responder.py running and listening
//
// Usage:
//
//	go run . [target_host:port]  (default: 127.0.0.1:11412)
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/get-robotunnel/roboat/clients/go/roboat"
)

func main() {
	socketPath := os.Getenv("ROBOAT_SOCKET")
	if socketPath == "" {
		socketPath = "/tmp/roboat-initiator.sock"
	}

	target := "127.0.0.1:11412"
	if len(os.Args) > 1 {
		target = os.Args[1]
	}

	rt, err := roboat.NewDaemon(socketPath)
	if err != nil {
		log.Fatalf("initiator: connect to daemon: %v", err)
	}
	defer rt.Close()

	fmt.Printf("initiator: dialing %s ...\n", target)
	stream, err := rt.Dial(target, "control")
	if err != nil {
		log.Fatalf("initiator: dial failed: %v", err)
	}
	defer stream.Close()

	msg := []byte("hello from go initiator!")
	if err := stream.Send(msg); err != nil {
		log.Fatalf("initiator: send failed: %v", err)
	}
	fmt.Printf("initiator: sent: %q\n", msg)

	reply, err := stream.Recv()
	if err != nil {
		log.Fatalf("initiator: recv failed: %v", err)
	}
	fmt.Printf("initiator: got reply: %q\n", reply)
}
