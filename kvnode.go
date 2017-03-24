// Usage: go run kvnode.go [nodesFile] [nodeID] [listen-node-in IP:port] [listen-client-in IP:port]

package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/rpc"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const HEARTBEAT_TIMEOUT = 3 * time.Second
const TIME_BETWEEN_HEARTBEATS = time.Millisecond
const NODE_DIAL_TIMEOUT = 6 * time.Second // todo should some of these timings be changed? clients start after 4 sec

// Server for kvservice
type KVServer int

// Server for nodes
type NServer int

// Represents a key in the system.
type Key string

// Represent a value in the system.
type Value string

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

// KVServer.IsCommitted
type IsCommittedRequest struct {
	CommittedTransaction int
}
type IsCommittedResponse struct {
	IsCommitted bool
}

// KVServer.Ping or NServer.Ping
type PingRequest struct{}
type PingResponse struct{}

// NServer.UpdateLargestTxId
type UpdateLargestTxIdRequest struct {
	largestTransactionId int
}
type UpdateLargestTxIdReply struct{}

// NServer.CommitTransaction
type CommitTransactionRequest struct {
	CommittedTransaction int
	MasterKV             map[Key]Value
}
type CommitTransactionResponse struct{}

var (
	masterKV    map[Key]Value // the key-value store
	locks       map[Key]int   // Key to Transaction Id
	uncommitted map[Key]Value // uncommitted key-values
	waiting     map[int]Key   // Transaction Id to Key

	largestTransactionId int // Globally Unique Transaction Id

	operationMutex sync.Mutex // ensures only one thing happens at a time on this node

	inProgressTransactions []int
	committedTransactions  []int

	livingNodeIPs []string
	numNodes      int
)

func main() {
	if len(os.Args) != 5 {
		panic("Usage: go run kvnode.go [nodesFile] [nodeID] [listen-node-in IP:port] [listen-client-in IP:port]")
	}

	// initializing maps
	masterKV = make(map[Key]Value)
	locks = make(map[Key]int)
	uncommitted = make(map[Key]Value)
	waiting = make(map[int]Key)

	operationMutex.Lock() // don't allow any other methods to run while initializing

	selfNodeId, err := strconv.Atoi(os.Args[2])
	checkError(err, true, "Node Id wasn't an int!")
	// setup rpc servers
	go setupKVServer(os.Args[4])
	go setupNodeServer(os.Args[3])
	go listenForClientHeartbeatSetup(os.Args[4])

	// get node ips
	nodesFile, err := os.Open(os.Args[1])
	checkError(err, true, "opening nodesFile")
	bufIo := bufio.NewReader(nodesFile)
	for {
		nodeIp, err := bufIo.ReadString('\n')
		nodeIp = strings.TrimSpace(nodeIp)
		if err == io.EOF {
			break
		}
		livingNodeIPs = append(livingNodeIPs, nodeIp)
	}
	debugLog("Found node ips: ", livingNodeIPs)

	// set up values for unique transaction ids on each node
	numNodes = len(livingNodeIPs)
	largestTransactionId = selfNodeId

	// remove self from living ips
	livingNodeIPs = append(livingNodeIPs[0:selfNodeId-1], livingNodeIPs[selfNodeId:]...)

	// sort ips to get same order as client
	sort.Strings(livingNodeIPs)

	// setup heartbeat
	checkError(err, true, "parsing node id")
	for _, nodeIp := range livingNodeIPs {
		setupHeartbeatToNode(nodeIp) // todo should this be done in parallel?
	}

	// done initializing
	operationMutex.Unlock()
	debugLog("Finished initializing. Waiting forever...")
	<-make(chan int, 1)
}

// Check if transactionId is still in progress
func isInProgress(transactionId int) bool {
	for _, tid := range inProgressTransactions {
		if transactionId == tid {
			return true
		}
	}

	return false
}

// Handle a node death
// Set getOperationMutexLock to false if you've already locked it
func nodeDied(nodeIpPort string, getOperationMutexLock bool) {
	if getOperationMutexLock {
		operationMutex.Lock()
		defer operationMutex.Unlock()
	}
	debugLog("Marking node: ", nodeIpPort, " as dead")
	// remove dead node from alive nodes
	nodeIndex := -1
	for index, nip := range livingNodeIPs {
		if nip == nodeIpPort {
			nodeIndex = index
		}
	}

	if nodeIndex < 0 {
		debugLog(nodeIpPort, " was already deleted")
		return // already deleted
	}

	livingNodeIPs = append(livingNodeIPs[:nodeIndex], livingNodeIPs[nodeIndex+1:]...)
}

//// RPC Methods

// RPC KVServer.Get
func (*KVServer) Get(request GetRequest, reply *GetResponse) error {
	operationMutex.Lock()
	defer operationMutex.Unlock()

	debugLog("Calling Get for reqeust:", request)

	if !isInProgress(request.TransactionId) {
		return fmt.Errorf("Transaction %v is already done", request.TransactionId)
	}

	// 1. If locks[Key] is free, get a lock and skip to the end.
	// 2. locks[Key] isn't free. set wait[Key] to transactionId. Go to 3.
	// 3. Check if you've created a deadlock. If you haven't go to 5. If you have, go to 4.
	// 4. Resolve the deadlock. The locks on Keys of the aborted transaction are free for others. If your Key isn't free, go to 4.
	// 5. Wait until transaction that owns Key commits or aborts. Then do 1.

	ownerTx, isLocked := locks[request.Key]
	if isLocked && ownerTx != request.TransactionId {
		waiting[request.TransactionId] = request.Key

		if isDeadLock() {
			//deadlock detected
			resolveDeadLock()
		}

		if _, isLocked = locks[request.Key]; isLocked {
			// key is still locked, so wait until key is free
			operationMutex.Unlock()
			for {
				operationMutex.Lock()

				// check if transaction was aborted while we were waiting
				if !isInProgress(request.TransactionId) {
					debugLog("Transaction was aborted while waiting for Get: ", request)
					return errors.New("Transaction was aborted while waiting for lock")
				}

				// check if lock is free
				if _, isLocked = locks[request.Key]; !isLocked {
					debugLog("Got lock on key for Get:", request)
					break
				}
				operationMutex.Unlock()
			}
		}

		delete(waiting, request.TransactionId)
	}

	// we can lock it
	locks[request.Key] = request.TransactionId
	if val, exists := uncommitted[request.Key]; exists {
		reply.Value = val
	} else {
		reply.Value = masterKV[request.Key]
	}

	reply.Success = true
	return nil
}

// RPC KVServer.Put
func (*KVServer) Put(request PutRequest, reply *PutResponse) error {
	operationMutex.Lock()
	defer operationMutex.Unlock()

	debugLog("Calling Put for request:", request)

	if !isInProgress(request.TransactionId) {
		return fmt.Errorf("Transaction %v is already done", request.TransactionId)
	}

	// 1. If locks[Key] is free, get a lock and skip to the end.
	// 2. locks[Key] isn't free. set wait[Key] to transactionId. Go to 3.
	// 3. Check if you've created a deadlock. If you haven't go to 5. If you have, go to 4.
	// 4. Resolve the deadlock. The locks on Keys of the aborted transaction are free for others. If your Key isn't free, go to 4.
	// 5. Wait until transaction that owns Key commits or aborts. Then do 1.

	ownerTx, isLocked := locks[request.Key]
	if isLocked && ownerTx != request.TransactionId {
		waiting[request.TransactionId] = request.Key

		if isDeadLock() {
			//deadlock detected
			resolveDeadLock()
		}

		if _, isLocked = locks[request.Key]; isLocked {
			// key is still locked, so wait until key is free
			operationMutex.Unlock()
			for {
				operationMutex.Lock()

				// check if transaction was aborted while we were waiting
				if !isInProgress(request.TransactionId) {
					debugLog("Transaction was aborted while waiting for Get: ", request)
					return errors.New("Transaction was aborted while waiting for lock")
				}

				// check if lock is free
				if _, isLocked = locks[request.Key]; !isLocked {
					debugLog("Got lock on key for Get:", request)
					break
				}
				operationMutex.Unlock()
			}
		}

		delete(waiting, request.TransactionId)
	}

	// we can lock it
	locks[request.Key] = request.TransactionId

	uncommitted[request.Key] = request.Value

	reply.Success = true
	return nil
}

// RPC KVServer.Commit
func (*KVServer) Commit(request CommitRequest, reply *CommitResponse) error {
	operationMutex.Lock()
	defer operationMutex.Unlock()

	debugLog("Got Commit request:", request)

	// check if transaction is already committed
	for _, tid := range committedTransactions {
		if tid == request.TransactionId {
			debugLog("Transaction was already committed: ", request)
			reply.Success = true
			return nil
		}
	}

	// check if transaction is still in progress
	if !isInProgress(request.TransactionId) {
		return fmt.Errorf("Transaction %v was aborted previously: ", request.TransactionId)
	}

	// do commit so we have data to propagate
	// add uncommitted locked keys to master
	for key, tid := range locks {
		if tid == request.TransactionId {
			// if key is locked, check if there's committed data
			if val, exists := uncommitted[key]; exists {
				masterKV[key] = val
			}

			// delete lock and uncommitted data
			delete(locks, key)
			delete(uncommitted, key)
			// there should be no keys this transaction is waiting waiting for
		}
	}
	// mark transaction as committed and remove it from being in progress
	committedTransactions = append(committedTransactions, request.TransactionId)

	committingIndex := -1
	for index, tid := range inProgressTransactions {
		if request.TransactionId == tid {
			committingIndex = index
		}
	}
	inProgressTransactions = append(inProgressTransactions[:committingIndex], inProgressTransactions[committingIndex+1:]...)

	// ensure every other node commits the transaction or all nodes that know about it die
	// talk to each node in same order as client.
	// if this node fails, this ensures that next node client talks to will tell the rest or have not received the message
	for _, node := range livingNodeIPs {
		// todo can dead nodes mean this hangs forever?
		client, err := rpc.Dial("tcp", node)
		checkError(err, false)
		if err != nil {
			continue
		}

		commitRequest := CommitTransactionRequest{request.TransactionId, masterKV}
		var commitReply CommitTransactionResponse
		err = client.Call("NServer.CommitTransaction", commitRequest, &commitReply)
		checkError(err, false, "Committing trasnaction") // err should just mean node is dead
	}

	reply.Success = true

	return nil
}

// RPC NServer.CommitTransaction
func (*NServer) CommitTransaction(request CommitTransactionRequest, reply *CommitTransactionResponse) error {
	debugLog("Got CommitTransaction request: ", request)
	// check if transaction is already committed
	for _, tid := range committedTransactions {
		if tid == request.CommittedTransaction {
			return nil
		}
	}

	//debugLog("Locking for CommitTransaction request: ", request)

	// if not, lock operation mutex, mark tx as committed and flood everyone else with it
	// todo test to ensure this won't ever create a deadlock of 2+ nodes mutually blocked and waiting?
	operationMutex.Lock()
	defer operationMutex.Unlock()

	// update from request
	committedTransactions = append(committedTransactions, request.CommittedTransaction)
	masterKV = request.MasterKV

	// flood everyone else
	// if they know about the tx being committed we call, they'll return
	// if they learn about it being committed as we tell them (maybe along with other calls), they'll block on all concurrent NServer.CommitTransaction
	//   then they'll go through one at a time flooding each other node and waiting for an ack back.
	for _, node := range livingNodeIPs {
		// todo can dead nodes mean this hangs forever?
		client, err := rpc.Dial("tcp", node)
		checkError(err, false)
		if err != nil {
			continue
		}

		commitRequest := CommitTransactionRequest{request.CommittedTransaction, masterKV}
		var commitReply CommitTransactionResponse
		err = client.Call("NServer.CommitTransaction", commitRequest, &commitReply)
		checkError(err, false, "Committing trasnaction") // err should just mean node is dead
	}

	return nil
}

// RPC KVServer.Abort
func (*KVServer) Abort(request AbortRequest, reply *AbortResponse) error {
	operationMutex.Lock()
	defer operationMutex.Unlock()

	debugLog("Got Abort for request: ", request)

	if !isInProgress(request.TransactionId) {
		return fmt.Errorf("Transaction %v is already done", request.TransactionId)
	}

	abortTransaction(request.TransactionId)

	return nil
}

// helper function for aborting a transaction
// assumes that caller already has lock on operationMutex
func abortTransaction(transactionId int) {
	abortedTxIndex := -1
	for index, tid := range inProgressTransactions {
		if transactionId == tid {
			abortedTxIndex = index
		}
	}

	// transaction is done if not in progress
	if abortedTxIndex < 0 {
		debugLog("Tried to abort transaction that is already completed: ", transactionId)
		return
	}

	// remove transaction from being in progress
	inProgressTransactions = append(inProgressTransactions[:abortedTxIndex], inProgressTransactions[abortedTxIndex+1:]...)

	// remove locks and uncommitted locked keys
	for key, tid := range locks {
		if tid == transactionId {
			delete(locks, key)
			delete(uncommitted, key)
		}
	}

	// remove tid from waiting map
	delete(waiting, transactionId)
}

// RPC KVServer.CreateTransaction
func (*KVServer) CreateTransaction(request CreateTransactionRequest, reply *CreateTransactionResponse) error {
	operationMutex.Lock()
	defer operationMutex.Unlock()

	// ensure two transactions never share the same id by incrementing by numNodes
	// each node starts at a different offset (it's node id)
	largestTransactionId += numNodes
	reply.TransactionId = largestTransactionId
	inProgressTransactions = append(inProgressTransactions, largestTransactionId)

	// make sure other nodes increment largest transaction ids
	for _, nodeIpPort := range livingNodeIPs {

		// create function for updating largest tx id on other node
		doneChannel := make(chan error, 1)
		go func(nodeIpPort string, doneChannel chan error) {
			client, err := rpc.Dial("tcp", nodeIpPort)
			checkError(err, false, "Dialing node for updating largest transaction id")
			if err != nil {
				doneChannel <- err
				return
			}

			request := UpdateLargestTxIdRequest{largestTransactionId}
			var reply UpdateLargestTxIdReply
			err = client.Call("NServer.UpdateLargestTxId", request, &reply)
			checkError(err, false, "Calling update largest tx id")

			doneChannel <- err
		}(nodeIpPort, doneChannel)

		// set timeout in case node goes down as we're calling this
		select {
		case <-doneChannel: // ignore any errors
		case <-time.After(HEARTBEAT_TIMEOUT):
			debugLog("Timed out trying to update largest tx id on node: ", nodeIpPort)
		}
	}

	return nil
}

// RPC KVServer.Ping
func (*KVServer) Ping(request PingRequest, reply *PingResponse) error {
	return nil
}

// RPC NServer.Ping
func (*NServer) Ping(request PingRequest, reply *PingResponse) error {
	//debugLog("Got Ping request", PingRequest{})
	return nil
}

// RPC NServer.UpdateLargestTxId
// Used to ensure that even if sender fails, other nodes will give out larger transaction ids
func (*NServer) UpdateLargestTxId(request UpdateLargestTxIdRequest, reply *UpdateLargestTxIdReply) error {
	for request.largestTransactionId > largestTransactionId {
		largestTransactionId += numNodes
	}
	return nil
}

//// Initialization functions

// sets up a heartbeat to a node
func setupHeartbeatToNode(nodeIpPort string) {
	goodClientChannel := make(chan *rpc.Client, 1)
	quitDialingChannel := make(chan bool, 1)

	// keep trying to dial until time limit
	go func() {
		for {
			select {
			case <-quitDialingChannel:
				debugLog("Stopping dialing to node: ", nodeIpPort)
				return
			default:
				// keep trying to dial a client
				client, err := rpc.Dial("tcp", nodeIpPort)
				checkError(err, false, "Dialing to node: ", nodeIpPort, " failed while initializing")
				if err == nil {
					// made a good client
					goodClientChannel <- client
					return
				}
			}
		}
	}()

	var client *rpc.Client
	select {
	case client = <-goodClientChannel:
		debugLog("Connected to node:", nodeIpPort)
	case <-time.After(NODE_DIAL_TIMEOUT):
		debugLog("Timed out waiting for dial to node: ", nodeIpPort)
		quitDialingChannel <- true
		nodeDied(nodeIpPort, false)
		return
	}

	go func() {
		heartbeatChannel := make(chan error, 1)
		heartbeatFunc := func() {
			heartbeatChannel <- client.Call("NServer.Ping", PingRequest{}, &PingResponse{})
		}

	Loop:
		for {
			go heartbeatFunc()
			select {
			case err := <-heartbeatChannel:
				checkError(err, false, "Got error when heartbeating to ", nodeIpPort)
				if err != nil {
					// error, so assume it's down
					break Loop
				}
				// if we made it here, we want to keep repeating the heartbeat
				//debugLog("Successful heartbeat to node: ", nodeIpPort)

			case <-time.After(HEARTBEAT_TIMEOUT):
				break Loop
			}

			time.Sleep(TIME_BETWEEN_HEARTBEATS)
		}

		// got here due to reaching heartbeat timeout or error in heartbeat
		nodeDied(nodeIpPort, true)
	}()
}

// create an rpc server for clients
func setupKVServer(ipPort string) {
	debugLog("Setting up kv server for clients at", ipPort)
	kvServer := rpc.NewServer()
	p := new(KVServer)
	kvServer.Register(p)

	l, err := net.Listen("tcp", ipPort)
	checkError(err, true, "listening for connection")
	for {
		conn, err := l.Accept()
		//debugLog("Accepted kv server connection")
		checkError(err, false, "accepting connection")
		go kvServer.ServeConn(conn)
	}
}

// create an rpc server for clients
func setupNodeServer(ipPort string) {
	debugLog("Setting up node server at", ipPort)
	nServer := rpc.NewServer()
	p := new(NServer)
	nServer.Register(p)

	l, err := net.Listen("tcp", ipPort)
	checkError(err, true, "listening for connection")
	for {
		conn, err := l.Accept()
		//debugLog("Accepted node server connection")
		checkError(err, false, "accepting connection")
		go nServer.ServeConn(conn)
	}
}

// create a udp listener for clients. when client dies, abort this transaction
func listenForClientHeartbeatSetup(ipPort string) {
	udpAddr, err := net.ResolveUDPAddr("udp", ipPort)
	checkError(err, true, "create udp conn")
	udpConn, err := net.ListenUDP("udp", udpAddr)
	checkError(err, true, "listen udp")

	for {
		buf := make([]byte, 1)
		_, remoteAddr, err := udpConn.ReadFrom(buf)
		checkError(err, false, "reading from new worker")
		if err != nil {
			continue
		}

		remoteUdpAddr := remoteAddr.(*net.UDPAddr)
		go setupHeartbeatToClient(remoteUdpAddr.String(), int(buf[0]))
	}
}
func setupHeartbeatToClient(clientIpPort string, tx int) {
	debugLog("Got heartbeat setup for tx: ", tx)

	// get rpc client for heartbeating
	client, err := rpc.Dial("tcp", clientIpPort)
	checkError(err, false, "dialing client for heartbeat")
	if err != nil {
		operationMutex.Lock()
		debugLog("Could not connect to client for transaction, ", tx, ". Aborting the transaction...")
		abortTransaction(tx)
		operationMutex.Unlock()
		return
	}

	// heartbeat to client
	resultChannel := make(chan error, 1)
	heartbeatFunc := func() {
		request := PingRequest{}
		var reply PingResponse
		resultChannel <- client.Call("LibServer.Ping", request, &reply)
	}

Loop:
	for {
		heartbeatFunc()
		select {
		case err := <-resultChannel:
			checkError(err, false, "Pinging client for result")
			if err != nil {
				break Loop
			}
		case <-time.After(HEARTBEAT_TIMEOUT):
			debugLog("Heartbeat timeout to client of transaction, ", tx)
			break Loop
		}

		time.Sleep(TIME_BETWEEN_HEARTBEATS)
	}

	// client d/c means transaction should be aborted
	operationMutex.Lock()
	debugLog("Client for transaction, ", tx, ", disconnected. Aborting the transaction if it's still in progress...")
	abortTransaction(tx)
	operationMutex.Unlock()
}

//// Deadlock detection

// dead lock detection
func isDeadLock() bool {
	deadlockTransactions := findDeadlockTransactions()

	return len(deadlockTransactions) > 0
}

// finds a cycle and kills the transaction with fewest locks
func resolveDeadLock() {
	deadlockTransactions := findDeadlockTransactions()

	if len(deadlockTransactions) == 0 {
		debugLog("Called resolveDeadlock but there was no deadlock")
		return
	}

	// determine transaction to kill
	fewestLocksCount := -1
	fewestLocksTx := -1

	for _, currentTx := range deadlockTransactions {
		// count locks made by current tx
		currentTxLocks := 0
		for _, tid := range locks {
			if tid == currentTx {
				currentTxLocks++
			}
		}

		// currentTx is better if we haven't picked a transaction or if it has fewer locks than the one we picked
		if fewestLocksCount < 0 || currentTxLocks < fewestLocksCount {
			fewestLocksCount = currentTxLocks
			fewestLocksTx = currentTx
		}
	}

	// sanity check
	if fewestLocksCount <= 0 {
		debugLog("Should never happen. Found deadlock transaction with 0 locks: ", fewestLocksTx)
		return
	}

	// kill the transaction
	debugLog("Killing transaction, ", fewestLocksTx, " since it had ", fewestLocksCount, " locks and was in a deadlock")
	abortTransaction(fewestLocksTx)
}

// returns a list of transactions that form a cycle in waiting.
// if there are none, returns nil
func findDeadlockTransactions() []int {
	fmt.Println("findDeadlockTransactions called")
	for waitingTid := range waiting {
		var visitedTransactions []int
		visitedTransactions = append(visitedTransactions, waitingTid)

		currentTx := waitingTid

		// keep going until we reach the end of the chain or find a loop
		for {
			// check if currentTx is waiting on any key
			waitingKey, foundWaitingKey := waiting[currentTx]
			fmt.Println("waitingKey", waitingKey)
			fmt.Println("foundWaitingKey", foundWaitingKey)
			fmt.Println("waiting[currentTx]", waiting[currentTx])
			if !foundWaitingKey {
				debugLog("No cycle for waiting transaction: ", waitingTid)
				break
			}

			// find the owner of waiting key
			lockOwnerTx, ownerExists := locks[waitingKey]
			fmt.Println("lockOwnerTx", lockOwnerTx)
			fmt.Println("ownerExists", ownerExists)
			fmt.Println("locks[waitingKey]", locks[waitingKey])

			if !ownerExists {
				debugLog("Should never happen: ", currentTx, " is waiting on ", waitingKey, " but there's nobody who owns it")
				break
			}

			// check if we've already seen this owner tx
			for index, tid := range visitedTransactions {
				if tid == lockOwnerTx {
					debugLog("Found cycle in transactions between: ", visitedTransactions[index:])
					return visitedTransactions[index:]
				}
			}

			// we haven't seen lockOwnerTx, so keep cycling
			visitedTransactions = append(visitedTransactions, lockOwnerTx)
			currentTx = lockOwnerTx
		}
	}

	debugLog("Found no deadlock!")
	return nil
}

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
