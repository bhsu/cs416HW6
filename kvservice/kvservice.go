/*

An example stub implementation of the kvservice interface for use by a
client to access the key-value service in assignment 6 for UBC CS 416
2016 W2.

*/

package kvservice

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"runtime/debug"
	"sort"
	"time"
)

const CREATE_CLIENT_TIMEOUT = 6 * time.Second
const TIME_BETWEEN_HEARTBEATS = time.Millisecond

// Represents a key in the system.
type Key string

// Represent a value in the system.
type Value string

// Server for kvservice
type KVServer int

// KVServer.Get
type GetRequest struct {
	Key           Key
	TransactionId int
}
type GetResponse struct {
	Value   Value
	Success bool
}

// KVServer.Put
type PutRequest struct {
	Key           Key
	Value         Value
	TransactionId int
}
type PutResponse struct {
	Success bool
}

// KVServer.Commit
type CommitRequest struct {
	TransactionId int
}
type CommitResponse struct {
	Success bool
}

// KVServer.Abort
type AbortRequest struct {
	TransactionId int
}
type AbortResponse struct{}

// KVServer.CreateTransaction
type CreateTransactionRequest struct{}
type CreateTransactionResponse struct {
	TransactionId int
}

// KVServer.Ping or NServer.Ping or LibServer.Ping
type PingRequest struct{}
type PingResponse struct{}

// Server running on client for heartbeat from node
type LibServer int

var (
	client               *rpc.Client
	nodesToTry           []string
	transactionId        int
	transactionIsAborted bool
)

var abortError = errors.New("Transaction was aborted!!!")

// An interface representing a connection to the key-value store. To
// create a new connection use the NewConnection() method.
type connection interface {
	// The 'constructor' for a new logical transaction object. This is the
	// only way to create a new transaction. The returned transaction must
	// correspond to a specific, reachable, node in the k-v service. If
	// none of the nodes are reachable then tx must be nil and error must
	// be set (non-nil).
	NewTX() (newTX tx, err error)

	// Close the connection.
	Close()
}

// An interface representing a client's transaction. To create a new
// transaction use the connection.NewTX() method.
type tx interface {
	// Retrieves a value v associated with a key k as part of this
	// transaction. If success is true then v contains the value
	// associated with k and err is nil. If success is false then the
	// tx has aborted, v is nil, and err is non-nil. If success is
	// false, then all future calls on this transaction must
	// immediately return success = false (indicating an earlier
	// abort).
	Get(k Key) (success bool, v Value, err error)

	// Associates a value v with a key k as part of this
	// transaction. If success is true then put was recoded
	// successfully, otherwise the transaction has aborted (see
	// above).
	Put(k Key, v Value) (success bool, err error)

	// Commits this transaction. If success is true then commit
	// succeeded, otherwise the transaction has aborted (see above).
	// txID represents the transactions's global sequence number
	// (which determines this transaction's position in the serialized
	// sequence of all the other transactions executed by the
	// service).
	Commit() (success bool, txID int, err error)

	// Aborts this transaction. This call always succeeds.
	Abort()
}

//////////////////////////////////////////////

// The 'constructor' for a new logical connection object. This is the
// only way to create a new connection. Takes a set of k-v service
// node ip:port strings.
func NewConnection(nodes []string) connection {
	debugLog("NewConnection")

	nodesToTry = nodes

	// ensure everyone tries connecting to nodes in the same order
	// guaranteed to be subset of nodesFile on node https://piazza.com/class/ixc8krzqchx5p4?cid=440
	sort.Strings(nodesToTry)

	return &myconn{}

}

// tries to create a new rpc client for a transaction.
// sets client to nil if no nodes are alive
func createRpcClientForTransaction() {
	// making rpc connection to the first node
	client = nil

	doneChannel := make(chan bool, 1)
	var connectedNode string
	// checking dead node for 3secs timeout when establishing connection
Loop:
	for len(nodesToTry) > 0 {
		quitTryingToConnect := make(chan bool, 1)
		go func(node string) {
			for {
				select {
				case <-quitTryingToConnect:
					debugLog("Got signal to quit trying to connect to node: ", node)
					return
				default:
					// keep trying to connect to client until we get quit signal
					c, err := rpc.Dial("tcp", node)
					checkError(err, false, "dialing client")
					if err != nil {
						continue
					}

					// make a test rpc call to ensure client works
					var response PingResponse
					request := PingRequest{}
					err = c.Call("KVServer.Ping", request, &response)
					checkError(err, false, "pinging node")
					if err == nil {
						client = c
						connectedNode = node
						doneChannel <- true
						return
					}
				}
			}
		}(nodesToTry[0])

		// wait while trying to connect
		select {
		case <-doneChannel:
			debugLog("Connected to node: ", connectedNode)
			break Loop
		case <-time.After(CREATE_CLIENT_TIMEOUT):
			debugLog("Timeout connecting to node: ", connectedNode)
			quitTryingToConnect <- true
			nodesToTry = nodesToTry[1:] // pop the front node
		}
	}

}

// RPC LibServer.Ping
func (*LibServer) Ping(request PingRequest, reply *PingResponse) error {
	return nil
}

// create an rpc server for node and notify it of the location by udp
func initHeartbeatFromServer(tx int, ipPort string) error {
	serverAddr, err := net.ResolveUDPAddr("udp", ipPort)
	checkError(err, true, "resolving server addr")
	conn, err := net.DialUDP("udp", nil, serverAddr)
	checkError(err, true, "dialing server")

	// keep sending messages until we succeed
	connected := make(chan int, 1)
	go acceptServerConnection(conn.LocalAddr().String(), connected)
	//debugLog("Going to attempt to connect to server for heartbeat")

	timeout := time.After(CREATE_CLIENT_TIMEOUT)
Loop:
	for {
		foo := byte(tx)
		msg := make([]byte, 1)
		msg[0] = foo
		conn.Write(msg)
		//debugLog("Attempting to connect to server")
		select {
		case <-connected:
			//debugLog("Node connected for heartbeating tx: ", tx)
			break Loop
		case <-timeout:
			return fmt.Errorf("Took too long connecting to node, %v", ipPort)
		case <-time.After(TIME_BETWEEN_HEARTBEATS):
			// continue
		}
	}

	return nil
}

func acceptServerConnection(ipPort string, connected chan int) {
	pServer := rpc.NewServer()
	p := new(LibServer)
	pServer.Register(p)

	notConnectedToAnybody := true
	l, err := net.Listen("tcp", ipPort)
	checkError(err, true, "error listening")
	for {
		conn, err := l.Accept()
		checkError(err, false, "error accepting")
		if err != nil {
			continue
		}
		go pServer.ServeConn(conn)
		if notConnectedToAnybody {
			//debugLog("Connected to server with remote addr: ", conn.RemoteAddr(), " with local addr: ", ipPort)
			connected <- 1
			notConnectedToAnybody = false
		}
	}
}

//////////////////////////////////////////////
// Connection interface

// Concrete implementation of a connection interface.
type myconn struct{} // everything stored in global variables

// Create a new transaction.
func (conn *myconn) NewTX() (tx, error) {
	debugLog("NewTX")

	// cleanup old transaction variables
	if client != nil {
		client.Close()
		client = nil
	}
	transactionIsAborted = false
	transactionId = -1

	for len(nodesToTry) > 0 {
		createRpcClientForTransaction()
		if client == nil {
			debugLog("Client was nil after creating, so all nodes are dead")
			break // no more nodes to try
		}

		var response CreateTransactionResponse
		request := CreateTransactionRequest{}
		err := client.Call("KVServer.CreateTransaction", request, &response)
		ErrorCheck("NewTX", err, false)
		if err != nil {
			continue // node died while we were creating a transaction. try another
		}

		err = initHeartbeatFromServer(response.TransactionId, nodesToTry[0])
		checkError(err, false, "initiating heartbeat from server to client")
		if err != nil {
			continue // node died while we were setting up server to client heartbat
		}

		transactionId = response.TransactionId
		return &mytx{}, nil
	}

	return nil, errors.New("All nodes are dead")
}

// Close the connection.
func (conn *myconn) Close() {
	debugLog("Close")
	// close rpc connections
	client.Close()
}

// /Connection interface
//////////////////////////////////////////////

//////////////////////////////////////////////
// Transaction interface

// Concrete implementation of a tx interface.
type mytx struct{} // everything stored in global variables

// Retrieves a value v associated with a key k.
func (t *mytx) Get(k Key) (success bool, v Value, err error) {
	// quit when transaction is already dead
	if transactionIsAborted {
		return false, v, abortError
	}
	debugLog("Get")
	var response GetResponse
	request := GetRequest{
		Key:           k,
		TransactionId: transactionId,
	}
	err = client.Call("KVServer.Get", request, &response)
	ErrorCheck("Get", err, false)
	if err != nil || response.Success == false {
		transactionIsAborted = true
	}
	v = response.Value
	return response.Success, v, nil
}

// Associates a value v with a key k.
func (t *mytx) Put(k Key, v Value) (success bool, err error) {
	// quit when transaction is already dead
	if transactionIsAborted {
		return false, abortError
	}
	debugLog("Put")
	var response PutResponse
	request := PutRequest{
		Key:           k,
		Value:         v,
		TransactionId: transactionId,
	}
	err = client.Call("KVServer.Put", request, &response)
	ErrorCheck("Put", err, false)
	if err != nil || response.Success == false {
		transactionIsAborted = true
	}
	return response.Success, nil
}

// Commits the transaction.
func (t *mytx) Commit() (success bool, txID int, err error) {
	// quit when transaction is already dead
	if transactionIsAborted {
		return false, txID, abortError
	}
	debugLog("Commit")

	var response CommitResponse
	// keep trying until we get a response
	for client != nil {
		// try calling on another node. if it already committed, it should return true
		request := CommitRequest{TransactionId: transactionId}
		err = client.Call("KVServer.Commit", request, &response)
		ErrorCheck("Commit", err, false)
		if err != nil {
			// if we had an error, maybe our node died while we called it
			createRpcClientForTransaction()
		} else {
			break
		}

	}

	if err != nil || response.Success == false {
		transactionIsAborted = true
		err = abortError
	}
	return response.Success, transactionId, err
}

// Aborts the transaction.

func (t *mytx) Abort() {
	debugLog("Abort")
	var response AbortResponse
	request := AbortRequest{TransactionId: transactionId}
	err := client.Call("KVServer.Abort", request, &response)
	ErrorCheck("Abort", err, false)
	// set this transaction as dead forever
	transactionIsAborted = true
}

// /Transaction interface
//////////////////////////////////////////////

//// Utility functions

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

// helper function for error printing
func ErrorCheck(msg string, err error, exit bool) {
	checkError(err, exit, msg)
}
