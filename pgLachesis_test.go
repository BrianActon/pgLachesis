package pgLachesis

import (
//	"encoding/json"
	"fmt"
	"testing"
//	"time"
)
/*

//1
// Literally testing connection to local hosted postgres instance
func TestConnectPostgres(t *testing.T) {

	fmt.Println("TestConnectPostgres start ")

	v, err := ConnectPostgres()

	if err != nil {
		fmt.Println("******************************************************************")
		fmt.Println("ClientABCIQueryTests error: ", err)
	}
	
	fmt.Println("******************************************************************")
//	if v != MarshalledJson {
	if v != nil {
		t.Error(
	    	"expected", string(MarshalledJson),
	    	"got",string(v),
        )
	}

	fmt.Println("TestClientABCIInfo finished ")
}



//1
// Literally testing connection to local hosted postgres instance
func TestConnectPostgres(t *testing.T) {

	fmt.Println("TestConnectPostgres start ")

	v, err := ConnectPostgres()

	if err != nil {
		fmt.Println("******************************************************************")
		fmt.Println("ClientABCIQueryTests error: ", err)
	}
	
	fmt.Println("******************************************************************")
//	if v != MarshalledJson {
	if v != nil {
		t.Error(
	    	"expected", string(MarshalledJson),
	    	"got",string(v),
        )
	}

	fmt.Println("TestClientABCIInfo finished ")
}


*/

//2
// Literally testing connection to local hosted postgres instance
func TestWriteAccounts(t *testing.T) {

	fmt.Println("TestWriteAccounts start ")

	v, err := WriteAccounts()

	if err != nil {
		fmt.Println("******************************************************************")
		fmt.Println("WriteAccounts error: ", err)
	}
	
	fmt.Println("******************************************************************")
//	if v != MarshalledJson {
	if v != nil {
		t.Error(
	    	"expected", string(MarshalledJson),
	    	"got",string(v),
        )
	}

	fmt.Println("TestWriteAccounts finished ")
}



//1
// Literally testing connection to local hosted postgres instance
func TestDropAccounts(t *testing.T) {

	fmt.Println("TestDropAccounts start ")

//	when exists
	err := DropAccounts()

	if err != nil {
		fmt.Println("******************************************************************")
		t.Error(
	    	"expected", nil,
	    	"got", err,
        )
	}
	
//  when doesnt exist	
	err = DropAccounts()


	fmt.Println("******************************************************************")
	if err != nil {
		t.Error(
	    	"expected", nil,
	    	"got", err,
        )
	}

	fmt.Println("TestDropAccounts finished ")
}