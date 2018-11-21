//**********************************************************************************
//	TODO:   (Depending on what dat we are storing)
// 		- Update blocks for when additional transactions are added
//		- Update accounts with additional transactions
//		- Update transactions with new entries for a block
//		- Update accounttransactions with new transactions for an account
//
//		Delete entries?
//
//
//**********************************************************************************
//
//	TODO to cater for explorer:
//		- Summary block : Create, update, read
//		- Add "From", "To" and amount to Transaction table
//		- Add Ether transaction history  (14 days?)
//		- Add "block reward" and "mined by" to block table
//
//		-	Login?
//
//**********************************************************************************

package pgLachesis

import (
	"database/sql" 
//	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
//	"time"

	_ "github.com/lib/pq"
)

const (
  host     = "localhost"
  port     = 5432
  user     = "postgres"
  password = "P0stGr3sSU"
  dbname   = "Lachesis"
)

var	Ppgsql *sql.DB


//**********************************************************************************
//  init function
//**********************************************************************************
func init() {
	fmt.Println("init innit?  -- AAAAIIIIII")
	db := ConnectPostgres()

	// ?!?!?!   Y U no work?
	//  A-HA!! Dont db.Close() in ConnectPostgres()!!!!!!!!
	Ppgsql = db

	fmt.Println("init done", db) 
}

type AccountPG struct {
	Account 	string
	Address 	string
	PublicKey 	string 			// Should we have these here?  
	PrivateKey 	string			// They make up the 2 parts of the Address field above
}								

//**********************************************************************************
//  Write Accounts
//**********************************************************************************
func WriteAccounts(account []byte, address string) error {

	fmt.Println("WriteAccounts in: ", string(account), address)
	var apg  AccountPG

	apg.Account = string(account)
	apg.Address = address

	q := fmt.Sprintf("INSERT INTO public.accounts(account, address, account_datetime) VALUES ($1, $2, NOW());")	

	_, err := Ppgsql.Exec(q, apg.Account, apg.Address)

	if err != nil {
		fmt.Println("fail to write account ", string(account), "error: ", err)
	}

	return err

}


//**********************************************************************************
//  Read Accounts
//**********************************************************************************
func ReadAccounts(account []byte) ([]AccountPG, error) {

	fmt.Println("ReadAccounts in: ", string(account))

	var err error
	var apg  AccountPG
	var apgs  []AccountPG

	q := ` SELECT account, address from accounts WHERE account = $1;`

	rows, err := Ppgsql.Query(q, account)

	if err != nil {
		fmt.Println("Error reading accounts : ", err)
		return apgs, err
	} 
	defer rows.Close()


	for rows.Next() {
		err = rows.Scan(&apg.Account, &apg.Address)
		if err != nil {
			fmt.Println("Error reading accounts : ", err)
		} 

		apgs = append(apgs, apg)
	}	
	
	return apgs, err
}

//**********************************************************************************
//  Update Accounts
//**********************************************************************************
//func UpdateAccounts(account []byte, address []byte) (AccountPG, error) {

//}



//**********************************************************************************
//  Write Account Transactions
//**********************************************************************************
func WriteAccountTrans(account []byte, transaction []byte) error {

	fmt.Println("WriteAccountTrans in: ", string(account), string(transaction))
		
	q := fmt.Sprintf("INSERT INTO public.accounttransactions(account, transaction, at_datetime) VALUES ($1, $2, NOW());")	

	_, err := Ppgsql.Exec(q, string(account), string(transaction))
	
	if err != nil {
		fmt.Println("fail to write accounttransactions ", string(account) , string(transaction), "error: ", err)
	}

	return err
}

//**********************************************************************************
//  Read Account Transactions
//**********************************************************************************
func ReadAccountTrans(account []byte) ([][]byte, error) {

	fmt.Println("ReadAccountTrans in: ", string(account))

	var trans [][]byte
	var tran  string
	var err error

	q := ` SELECT transaction from accounttransactions WHERE account = $1;`

	rows, err := Ppgsql.Query(q, string(account))

	if err != nil {
		fmt.Println("Error reading accounts : ", err)
		return trans, err
	} 
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&tran)
		if err != nil {
			fmt.Println("Error looping through rows", err)
		}
		trans = append(trans, []byte(tran))
	}	

	return trans, err

}

//**********************************************************************************
//  Update Accounts
//**********************************************************************************
//func UpdateAccountsTrans(account []byte, address []byte) (AccountPG, error) {

//}



//**********************************************************************************
// Write Transactions
//**********************************************************************************
	//  need to cater for pages.
	//  Can either 
func WriteTransactions(Transactions  [][]byte) (string, error) {

	fmt.Println("WriteTransactions in: ", string(Transactions[0][:]))

	transactionblockid := rand.Int63()
	var err error


	q := `INSERT INTO transactions(transaction, transactionblockid, transaction_datetime) VALUES ($1, $2, NOW());	`

	
	for i := 0; i < len(Transactions); i++ {
		
		_, err := Ppgsql.Exec(q, string(Transactions[i]), transactionblockid)
		if err != nil {
			fmt.Println("fail to write transaction ", string(Transactions[i]), "error: ", err)
			break
		}
	}

	return strconv.FormatInt(transactionblockid, 10), err
}



//**********************************************************************************
// Write Transactions
//**********************************************************************************
// get single most recent transaction
//**********************************************************************************
func ReadLatestTransaction() ([]byte, error) {

	fmt.Println("ReadLatestTransaction in: ")

	var Transaction string

	row, err := Ppgsql.Query("SELECT transaction from transactions ORDER BY transaction_datetime DESC;")
	if err != nil {
		fmt.Println("Error reading transactions db: ", err)
	} 
	defer row.Close()

	err = row.Scan(&Transaction)

 	return []byte(Transaction), err
}


//**********************************************************************************
//  TODO - Need to understand what we tryign to achieve with Pagination
//
//	- Consider for an alternative function: 
//  func ReadListTransactions(transactionblockid string, TranStart int, TranPerPage int) ([]string, error) {
//
//**********************************************************************************
func ReadListTransactions(transactionblockid string, pageNumber int) ([]string, error) {

	fmt.Println("ReadListTransactions in: ",  transactionblockid, pageNumber)

	rows, err := Ppgsql.Query("SELECT transaction from transactions WHERE transactionblockid = ? ;", transactionblockid)
	if err != nil {
		fmt.Println("Error reading transactions db: ", err)
	} 

	defer rows.Close()

	var Transactions []string
	var Trans  string

	for rows.Next() {
		err = rows.Scan(&Trans)
		if err != nil {
			fmt.Println("Error looping through rows", err)
		}
		Transactions = append(Transactions, Trans)
	}	

	return Transactions, err
}

//**********************************************************************************
//  Update Transactions for a specific block
// um...?  Unless we know we are only getting a complete block
//**********************************************************************************
//func UpdateTransactions(transactionblockid string, Transactions  [][]byte) (AccountPG, error) {

//}



// StateHash as taken from block.go:
//StateHash is the hash of the current state of transactions, if you have one
//node talking to an app, and another set of nodes talking to inmem, the
//stateHash will be different
//statehash should be ignored for validator checking
//   .. therefore StateHash will be ignored for now
type BlockBody struct {
	Index         int64
	RoundReceived int64
	StateHash     []byte
	FrameHash     []byte
	Transactions  [][]byte
}

type PGBlockBody struct {
	Index         		string   
	RoundReceived 		string
	StateHash     		string
	FrameHash     		string
	TransactionsBlockID string		
	TransactionsBlockCnt int 		//   <<<<---- RoundReceived? 			
}

//**********************************************************************************
// Write Block, includes the writing of all transaction to the transaction table
//**********************************************************************************
func WriteBlock(block BlockBody) error {

	fmt.Println("WriteBlock in: [0][:]", string(block.Transactions[0][:]))
	fmt.Println("WriteBlock in: ", string(block.Transactions[:][0]))

	indexStr := strconv.FormatInt(block.Index, 10)
	rrStr := strconv.FormatInt(block.RoundReceived, 10)

	var pbblock PGBlockBody
	
	pbblock.Index 			= indexStr
	pbblock.RoundReceived 	= rrStr

	pbblock.StateHash 		= string(block.StateHash)
	pbblock.FrameHash 		= string(block.FrameHash)

	transactionBlockID, err :=  WriteTransactions(block.Transactions)

	if err != nil {
		fmt.Println("Error: ", err)
	} else {

		q := `INSERT INTO blocks(
								blockIndex, 
								framehash, 
								transactionblockid, 
								transactionblockcount, 
								block_datetime) 
				VALUES ($1, $2, $3, $4, NOW());	`

	   // check if can use pbblock.RoundReceived or if we need the count returned from
	 	_, err = Ppgsql.Exec(q, pbblock.Index, pbblock.FrameHash, transactionBlockID, pbblock.RoundReceived )  
		if err != nil {
			fmt.Println("fail to write blocks : error: ", err)
		}
	}
	return err
}
			

//**********************************************************************************
// Reads th eblock table only, and a separate call must be made to retrieve all
// relevant transaction for this block
//**********************************************************************************
func ReadBlock(block int) ( PGBlockBody, error) {

	fmt.Println("ReadBlock in: ", block)

	var pbblock PGBlockBody

	q := `SELECT blockIndex, framehash, transactionblockid, transactionblockcount
 			FROM blocks  WHERE blockIndex = $1;	`

	row, err := Ppgsql.Query(q, block)

	if err != nil {
		fmt.Println("Error reading blocks: ", err)
		return pbblock, err
	} 
	defer row.Close()

	for row.Next() {
		err = row.Scan(&pbblock.Index, &pbblock.FrameHash, 
					&pbblock.TransactionsBlockID, &pbblock.TransactionsBlockCnt )
		if err != nil {
			fmt.Println("Error looping through rows", err)
		} else {
			fmt.Println("Block: ", pbblock)
		}
	}	
	
		

	return pbblock, err
}

//**********************************************************************************
//  Update block
//**********************************************************************************
//func UpdateBlock(account []byte, address []byte) (AccountPG, error) {

//}



//**********************************************************************************
// Write Transactions
//**********************************************************************************
	//  need to cater for pages.
	//  Can either 


/*
func WriteSummary() (string, error) {

	fmt.Println("WriteTransactions in: ", string(Transactions[0][:]))

	transactionblockid := rand.Int63()
	var err error

	for i := 0; i < len(Transactions); i++ {
		_, err = Ppgsql.Exec("INSERT INTO transactions values ( ?, ?, NOW() );", 
								string(Transactions[i]), transactionblockid)
		if err != nil {
			fmt.Println("fail to write transaction ", string(Transactions[i]), "error: ", err)
			break
		}
	}

	return strconv.FormatInt(transactionblockid, 10), err
}
*/

//**********************************************************************************
// Fetch summary details
//**********************************************************************************

/*
func ReadSummary() (Summary, error) {

	fmt.Println("ReadSummary in: ")

	var s Summary

	row, err := Ppgsql.Query("SELECT ... from summary;")
	if err != nil {
		fmt.Println("Error reading summary db: ", err)
	} 
	defer row.Close()

	err = row.Scan(&ReadSummary)

 	return s, err
}
*/

//**********************************************************************************
//  Update Transactions for a specific block
// um...?  Unless we know we are only getting a complete block
//**********************************************************************************
//func UpdateSummary(transactionblockid string, Transactions  [][]byte) (AccountPG, error) {

//}




/*
func MarshalByte(s string) ([]byte, error) {

}

func UnMarshalByte(b []byte) (string, error) {

}
*/


//*****************************************************************************************
//	Test if tables exist, if not, then create them
//*****************************************************************************************
func testTablesExist() error {

	var triedAgain bool = false

retryAccount:	
	//  test account table exists
	err := HelloAccounts()
	if err != nil && triedAgain == false {
		fmt.Println("accounts table doesnt exist")
		fmt.Println("Creating accounts table")

		err = CreateAccounts()

		if err != nil {
			if err.Error() == `pq: relation "accounts" already exists` {
				fmt.Println("accounts table: ", err)
			} else {
				fmt.Println("Create accounts error: ", err)
			}
		} else {
			triedAgain = true
			goto retryAccount
		}
	} else {
		fmt.Println("accounts exists")
	}


	triedAgain = false

retryTransaction:	
	//  test transaction table exists
	err = HelloTransaction()
	if err != nil {
		fmt.Println("transaction table doesnt exist")
		fmt.Println("Creating transaction table")

		err = CreateTransaction()

		if err != nil {
			if err.Error() == `pq: relation "transaction" already exists` {
				fmt.Println("transaction table: ", err)
			} else {
				fmt.Println("Create transaction error: ", err)
			}
		} else {
			triedAgain = true
			goto retryTransaction
		}
	} else {
		fmt.Println("transaction exists")
	}


	triedAgain = false

retryactran:	
	//  test transaction table exists
	err = HelloAccountTrans()
	if err != nil {
		fmt.Println("accounttrans table doesnt exist")
		fmt.Println("Creating accounttrans table")

		err = CreateAccountTrans()

		if err != nil {
			if err.Error() == `pq: relation "accounttrans" already exists` {
				fmt.Println("accounttrans table: ", err)
			} else {
				fmt.Println("Create accounttrans error: ", err)
			}
		} else {
			triedAgain = true
			goto retryactran
		}
	} else {
		fmt.Println("accounttrans table exists")
	}


	triedAgain = false

retryBlock:	
	//  test transaction table exists
	err = HelloBlock()
	if err != nil {
		fmt.Println("blocks table doesnt exist")
		fmt.Println("Creating blocks table")

		err = CreateBlock()

		if err != nil {
			if err.Error() == `pq: relation "blocks" already exists` {
				fmt.Println("blocks table: ", err)
			} else {
				fmt.Println("Create blocks error: ", err)
			}
		} else {
			triedAgain = true
			goto retryBlock
		}
	} else {
		fmt.Println("blocks table exists")
	}


	return nil
}



//*****************************************************************************************
//	Hello? can we read from these tables
//*****************************************************************************************

func HelloAccounts() error {

	_, err := Ppgsql.Query("SELECT account from accounts LIMIT 1")

	return err
}

func HelloTransaction() error {
	_, err := Ppgsql.Query("SELECT transaction from transactions LIMIT 1")

	return err
	
}

func HelloAccountTrans() error {
	_, err := Ppgsql.Query("SELECT account from accounttransactions LIMIT 1")

	return err
	
}

func HelloBlock() error {
	_, err := Ppgsql.Query("SELECT blockIndex from blocks LIMIT 1")

	return err
	
}


//*****************************************************************************************
//	Lets create our tables!!!
//*****************************************************************************************
//*****************************************************************************************
//	Lets create our tables!!!
//*****************************************************************************************
func CreateLachesisDB() error {
	// can we do this with PostGres????
	return nil
}

//*****************************************************************************************
//	Lets create our tables!!!
//*****************************************************************************************
func CreateAccounts() error {

	q := `	CREATE TABLE accounts (
 				account VARCHAR (70),
 				address VARCHAR (70),
 				publicKey VARCHAR (70),
 				privateKey VARCHAR (70),
 				account_datetime VARCHAR (50)
			);`
	
	_, err := Ppgsql.Exec(q)

//	fmt.Println("accounts: ", err)
	
	return err
}

//*****************************************************************************************
//	Lets create our tables!!!
//*****************************************************************************************
func CreateTransaction() error {
	
	q := `	CREATE TABLE transactions (
 				transaction VARCHAR (70),
 				transactionblockid VARCHAR (70),
 				transaction_datetime VARCHAR (50)
			);`
	
	_, err := Ppgsql.Exec(q)

//	fmt.Println("transactions: ", err)
	
	return err
}

//*****************************************************************************************
//	Lets create our tables!!!
//*****************************************************************************************
func CreateAccountTrans() error {
	
	q := `	CREATE TABLE accounttransactions (
 				account VARCHAR (70),
 				transaction VARCHAR (70),
 				at_DateTime VARCHAR (50)
			);`	
 				// address VARCHAR (50),    <--  removed as account should be sufficient


	_, err := Ppgsql.Exec(q)

//	fmt.Println("accounttransactions: ", err)
	
	return err
}

//*****************************************************************************************
//	Lets create our tables!!!
//*****************************************************************************************
func CreateBlock() error {
	
	q := `	CREATE TABLE blocks (
 				blockIndex int,
 				framehash VARCHAR (70),
 				transactionblockid VARCHAR (70),
 				transactionblockcount int,
 				block_datetime VARCHAR (50)
			);`
	
	_, err := Ppgsql.Exec(q)

//	fmt.Println("block: ", err)
	
	return err
}


//*****************************************************************************************
//	Lets create our tables!!!
//*****************************************************************************************
func CreateSummary() error {
	
	q := `	CREATE TABLE summary (
 				market_cap float?,
 				BTC_ETH float?,
 				lastblockno int64,
 				hashrate float?,
 				transactions VARCHAR (50)
 				network_difficulty VARCHAR (50)
 				lastupdate_DateTime VARCHAR (50)
 				lastread_DateTime VARCHAR (50)
			);`
	
	_, err := Ppgsql.Exec(q)

//	fmt.Println("block: ", err)
	
	return err
}


//*****************************************************************************************
//	Drop tables
//*****************************************************************************************

//*****************************************************************************************
//	Drop all tables!!!
//*****************************************************************************************
func DropAllTables() error {
	err := DropAccounts()
	fmt.Println("err", err)
	err = DropTransaction()
	fmt.Println("err", err)
	err = DropAccountTrans()
	fmt.Println("err", err)
	err = DropBlock()
	fmt.Println("err", err)
	err = DropSummary()
	fmt.Println("err", err)

	return err
}


//*****************************************************************************************
//	Drop accounts table
//*****************************************************************************************
func DropAccounts() error {

	fmt.Println("DropAccounts")

	_, err := Ppgsql.Exec("DROP TABLE accounts")

	return err
}

//*****************************************************************************************
//	Drop transactions table
//*****************************************************************************************
func DropTransaction() error {
	_, err := Ppgsql.Query("DROP TABLE transactions")

	return err
	
}

//*****************************************************************************************
//	Drop accounttrans table
//*****************************************************************************************
func DropAccountTrans() error {
	_, err := Ppgsql.Query("DROP TABLE accounttransactions")

	return err
	
}

//*****************************************************************************************
//	Drop blocks table
//*****************************************************************************************
func DropBlock() error {
	_, err := Ppgsql.Query("DROP TABLE blocks")

	return err
	
}

//*****************************************************************************************
//	Drop summary table
//*****************************************************************************************
func DropSummary() error {
	_, err := Ppgsql.Query("DROP TABLE summary")

	return err
	
}



//*****************************************************************************************
//	Connect to PostGtres instance
//*****************************************************************************************
func ConnectPostgres() *sql.DB {

	fmt.Println("ConnectPostgres innit?")
  	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
    						"password=%s dbname=%s sslmode=disable",
    						host, port, user, password, dbname)

  	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		fmt.Println("Excuse me kind person, do you have postgres loaded..?")
  		panic(err)
	} 

	Ppgsql = db
	
	err = db.Ping()
	if err != nil {
		fmt.Println("db.Ping unsuccessful", err)
  		panic(err)
	} else {
		fmt.Println("db.Ping successful", err)
	}

	err = Ppgsql.Ping()
	if err != nil {
		fmt.Println("Ppgsql.Ping unsuccessful")
  		panic(err)
	} else {
		fmt.Println("Ppgsql.Ping successful")
	}

	// Create tables if don't exist  -->  TODO: Chat to Andre to see if needed    
	err = testTablesExist()
	if err != nil {
		fmt.Println("Problem with accessing postgres tables")
		fmt.Println("Do you have postgres loaded?")
	}

	fmt.Println("ConnectPostgres done")

	return db
}
