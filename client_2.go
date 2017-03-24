/*

A trivial client to illustrate how the kvservice library can be used
from an application in assignment 6 for UBC CS 416 2016 W2.

Usage:
go run client.go
*/

package main

// Expects kvservice.go to be in the ./kvservice/ dir, relative to
// this client.go file
import "./kvservice"

import (
	"fmt"
	"os"
	"time"
)

func main() {
	var nodes []string
	//nodes = append(nodes, "127.0.0.1:2020")
	//nodes = append(nodes, "bob:2010")

	nodes = append(nodes, "52.233.40.23:2020")
	nodes = append(nodes, "52.164.227.209:6060")

	c := kvservice.NewConnection(nodes)
	fmt.Printf("NewConnection returned: %v\n", c)

	t, err := c.NewTX()
	fmt.Printf("NewTX returned: %v, %v\n", t, err)

	testcase := 3
	switch testcase {
	case 1:
		// No failures, 1-client, non-aborting txns
		// No failures, 1-client, aborting and non-aborting txns
		//  No failures, n-clients, non-conflicting txns
		success, err := t.Put("java", "world")
		fmt.Printf("Put returned: %v, %v\n", success, err)

		success1, err := t.Put("php", "world")
		fmt.Printf("Put returned: %v, %v\n", success1, err)

		success, v, err := t.Get("java")
		fmt.Printf("Get returned: %v, %v, %v\n", success, v, err)

		//t.Abort()
		//fmt.Println("Abort")

		success, txID, err := t.Commit()
		fmt.Printf("Commit returned: %v, %v, %v\n", success, txID, err)
	case 2:
		// No failures, n-clients, conflicting txns
		success, err := t.Put("hello", "world")
		fmt.Printf("Put returned: %v, %v\n", success, err)

		success1, err := t.Put("My", "name")
		fmt.Printf("Put returned: %v, %v\n", success1, err)

		success, v, err := t.Get("hello")
		fmt.Printf("Get returned: %v, %v, %v\n", success, v, err)

		success, txID, err := t.Commit()
		fmt.Printf("Commit returned: %v, %v, %v\n", success, txID, err)
	case 3:

		// No failures, n-clients, deadlocking txns progress check

		success2, err := t.Put("hello", "world")
		fmt.Printf("Put returned: %v, %v\n", success2, err)

		success, v, err := t.Get("cat")
		fmt.Printf("Get returned: %v, %v, %v\n", success, v, err)

	case 4:
		// Testing with client 1
		success, v, err := t.Get("Hi")
		fmt.Printf("Get returned: %v, %v, %v\n", success, v, err)

	case 5:
		// Client-failures, n-clients, incomplete txns abort
		success, err := t.Put("q", "foo")
		fmt.Printf("Put returned: %v, %v\n", success, err)

		os.Exit(0)

		success, v, err := t.Get("q")
		fmt.Printf("Get returned: %v, %v, %v\n", success, v, err)

	case 7:
		// Node-failures, 1-client, kv-service available
		// Node-failures, n-clients, kv-service available testing with client 1
		success, err := t.Put("Hi", "foo")
		fmt.Printf("Put returned: %v, %v\n", success, err)

		success2, err := t.Put("hello", "world")
		fmt.Printf("Put returned: %v, %v\n", success2, err)

		success1, err := t.Put("My", "name")
		fmt.Printf("Put returned: %v, %v\n", success1, err)

		time.Sleep(5*time.Second) // kill the node(manager) at this point

		success3, v, err := t.Get("hello")
		fmt.Printf("Get returned: %v, %v, %v\n", success3, v, err)

		success4, v, err := t.Get("Hi")
		fmt.Printf("Get returned: %v, %v, %v\n", success4, v, err)

	case 8:
		// Node-failures, n-clients, aborting and non-aborting txns
		success, err := t.Put("Hi", "foo")
		fmt.Printf("Put returned: %v, %v\n", success, err)

		success2, err := t.Put("hello", "world")
		fmt.Printf("Put returned: %v, %v\n", success2, err)

		success1, err := t.Put("My", "name")
		fmt.Printf("Put returned: %v, %v\n", success1, err)

		success, txID, err := t.Commit()
		fmt.Printf("Commit returned: %v, %v, %v\n", success, txID, err)

	case 9:
		// Node-failures, n-clients, aborting and non-aborting txns
		success, err := t.Put("a", "foo")
		fmt.Printf("Put returned: %v, %v\n", success, err)

		time.Sleep(5*time.Second) // kill the node(manager) at this point

		success2, err := t.Put("b", "world")
		fmt.Printf("Put returned: %v, %v\n", success2, err)


	case 10:
		// Node-failures, n-clients, conflicting txns
		// testing with client 1
		success, err := t.Put("Hi", "foo")
		fmt.Printf("Put returned: %v, %v\n", success, err)

		success1, err := t.Put("My", "name")

		fmt.Printf("Put returned: %v, %v\n", success1, err)
		success2, err := t.Put("My", "name")
		fmt.Printf("Put returned: %v, %v\n", success2, err)

		time.Sleep(5*time.Second) // kill the node(manager) at this point

		success3, err := t.Put("My", "name")
		fmt.Printf("Put returned: %v, %v\n", success3, err)

	case 11:
		// Node-failures, n-clients, deadlocking txns progress check

		success2, err := t.Put("hello", "world")
		fmt.Printf("Put returned: %v, %v\n", success2, err)

		success, v, err := t.Get("cat")
		fmt.Printf("Get returned: %v, %v, %v\n", success, v, err)


	}
	c.Close()
}
