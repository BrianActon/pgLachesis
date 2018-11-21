package pgLachesis

import (
//	"encoding/json"
	"fmt"
	"testing"
//	"time"
)
/*
	func WriteAccounts(account []byte, address []byte) error {
	func ReadAccounts(account []byte) (AccountPG, error) {
	func UpdateAccounts(account []byte, address []byte) (AccountPG, error) {

	func WriteAccountTrans(account []byte, transaction []byte) error {
	func ReadAccountsTrans(account []byte) ([][]byte, error) {
	func UpdateAccountsTrans(account []byte, address []byte) (AccountPG, error) {

	func WriteTransactions(Transactions  [][]byte) (string, error) {	
	func ReadLatestTransaction() ([]byte, error) {
	func ReadListTransactions(transactionblockid string, pageNumber int) ([]string, error) {
	func UpdateTransactions(transactionblockid string, Transactions  [][]byte) (AccountPG, error) {	

	func WriteBlock(block BlockBody) error {
	func ReadBlock(block []byte) ( PGBlockBody, error) {
	func UpdateBlock(block []byte) ( PGBlockBody, error) {

	func WriteSummary(block BlockBody) error {
	func ReadSummary(block []byte) ( PGBlockBody, error) {
	func UpdateSummary(block []byte) ( PGBlockBody, error) {

	func testTablesExist() error {

	func HelloAccounts() error {
	func HelloTransaction() error {
	func HelloAccountTrans() error {
	func HelloBlock() error {
	func HelloSummary() error {

	func CreateLachesisDB() error {
	
	func CreateAccounts() error {		
	func CreateTransaction() error {
	func CreateAccountTrans() error {
	func CreateBlock() error {
	func CreateSummary() error {

	func DropAllTables() error {
	func DropAccounts() error {
	func DropTransaction() error {
	func DropAccountTrans() error {
	func DropBlock() error {
	func DropSummary() error {

	func ConnectPostgres() *sql.DB {	

*/

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

	a := []byte("account1234test")
	b := "address1234test"
	err := WriteAccounts(a, b)

	if err != nil {
		fmt.Println("******************************************************************")
		fmt.Println("WriteAccounts error: ", err)
	}
	
	fmt.Println("******************************************************************")


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

	r := `pq: table "accounts" does not exist`
	fmt.Println("******************************************************************")
	if err.Error() != r {
		t.Error(
	    	"expected", nil,
	    	"got", err,
        )
	}

	fmt.Println("TestDropAccounts finished ")
}