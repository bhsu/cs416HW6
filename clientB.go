// to test, run node listening on port 2001 for clients
// run clientA and watch until it waits
// run clientB

package main

// Expects kvservice.go to be in the ./kvservice/ dir, relative to
// this client.go file
import "./kvservice"

import (
	"fmt"
	"net"
	"log"
	"runtime/debug"
	"os"
	"net/rpc"
)

type CServer int

type SendSignalRequest struct {}
type SendSignalResponse struct {}

var (
	clients []string
	gotMessageChannel = make(chan bool, 10)
)

func main() {
	var nodes []string
	nodes = append(nodes, "127.0.0.1:2001")
	//nodes = append(nodes, "127.0.0.1:2003")

	clients = append(clients,
		"127.0.0.1:3001",
		"127.0.0.1:3002",
	)
	setupListening(clients[1])

	c := kvservice.NewConnection(nodes)
	fmt.Printf("NewConnection returned: %v\n", c)

	t, err := c.NewTX()
	fmt.Printf("NewTX returned: %v, %v\n", t, err)

	waitForSignal()

	success, v, err := t.Get("hello")
	fmt.Printf("Get returned: %v, %v, %v\n", success, v, err)

	fmt.Println("If we got here, the test failed since A should hold the lock for 'hello'")
}

func waitForSignal() {
	//debugLog("Waiting for signal")
	<-gotMessageChannel
}

func tellClientToGo(clientIndex int) {
	ipPort := clients[clientIndex]

	for {
		client, err := rpc.Dial("tcp", ipPort)
		//checkError(err, false)
		if err != nil {
			continue
		}

		request := SendSignalRequest{}
		var reply SendSignalResponse
		err = client.Call("CServer.SendSignal", request, &reply)
		checkError(err, true)
		if err == nil {
			break
		}
	}
}

func (*CServer) SendSignal(request SendSignalRequest, reply *SendSignalResponse) error {
	//debugLog("Got signal")
	gotMessageChannel <- true
	return nil
}

func setupListening(ipPort string) {
	//debugLog("Setting up kv server for clients at", ipPort)
	cServer := rpc.NewServer()
	p := new(CServer)
	cServer.Register(p)

	l, err := net.Listen("tcp", ipPort)
	checkError(err, true, "listening for connection")
	go func() {
		for {
			conn, err := l.Accept()
			//debugLog("Accepted kv server connection")
			checkError(err, false, "accepting connection")
			go cServer.ServeConn(conn)
		}
	}()
}

// logging that can be disabled
func debugLog(v ...interface{}) {
	fmt.Println(v...)
}

// error handling
func checkError(err error, exit bool, msgs ...interface{}) {
	msg := fmt.Sprint(msgs...)
	if err != nil {
		if len(msg) > 0 {
			msg = msg + ":"
		}

		if exit {
			log.Println(msg, err)
			debug.PrintStack()
			os.Exit(-1)
		} else {
			debugLog(msg, err)
		}
	}
}