//**********************************************************************************
//	TODO:
// 		- Update blocks for when additional transactions are added
//		- Update accounts with additional transactions
//		- Update transactions with new entries for a block
//		- Update accounttransactions with new transactions for an account
//
//		Delete entries?
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
	fmt.Println("init innit?")
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
func WriteAccounts(account []byte, address []byte) error {

	fmt.Println("WriteAccounts in: ", string(account), string(address))
	var apg  AccountPG

	apg.Account = string(account)
	apg.Address = string(address)

		
	_, err := Ppgsql.Exec("INSERT INTO accounts values ( ?, ?, ?, ?, NOW() );", 
							apg.Account, apg.Address, "", "")
	if err != nil {
		fmt.Println("fail to write account ", string(account), "error: ", err)
	}

	return err

}


//**********************************************************************************
//  Read Accounts
//**********************************************************************************
func ReadAccounts(account []byte) (AccountPG, error) {

	fmt.Println("ReadAccounts in: ", string(account))

	var err error
	var apg  AccountPG

	row, err := Ppgsql.Query("SELECT account, address from accounts WHERE account = ?;", account)

	if err != nil {
		fmt.Println("Error reading accounts : ", err)
		return apg, err
	} 
	defer row.Close()

	err = row.Scan(&apg.Account, &apg.Address)
	if err != nil {
		fmt.Println("Error reading accounts : ", err)
	} 

	return apg, err
}


//**********************************************************************************
//  Write Account Transactions
//**********************************************************************************
func WriteAccountTrans(account []byte, transaction []byte) error {

	fmt.Println("WriteAccountTrans in: ", string(account), string(transaction))
		
	_, err := Ppgsql.Exec("INSERT INTO accounttransaction values ( ?, ?, NOW() );", 
							string(account) , string(transaction))
	if err != nil {
		fmt.Println("fail to write accounttransaction ", string(account) , string(transaction), "error: ", err)
	}

	return err
}

//**********************************************************************************
//  Read Account Transactions
//**********************************************************************************
func ReadAccountsTrans(account []byte) ([][]byte, error) {

	fmt.Println("ReadAccountsTrans in: ", string(account))

	var trans [][]byte
	var tran  string
	var err error

	rows, err := Ppgsql.Query("SELECT account, transaction from accounts WHERE account = ?;", string(account))

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
// Write Transactions
//**********************************************************************************
	//  need to cater for pages.
	//  Can either 
func WriteTransactions(Transactions  [][]byte) (string, error) {

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
	Index         		string   //  Is this BlockHash?                   <<<<   Dont forget !! Index == BlockHash????
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

	   // check if can use pbblock.RoundReceived or if we need the count returned from
	 	_, err = Ppgsql.Exec("INSERT INTO blocks values ( ?, ?, ?, ?, NOW() );", pbblock.Index, pbblock.FrameHash, transactionBlockID)  
		if err != nil {
			fmt.Println("fail to write transaction : error: ", err)
		}
	}
	return err
}


//**********************************************************************************
// Reads th eblock table only, and a separate call must be made to retrieve all
// relevant transaction for this block
//**********************************************************************************
func ReadBlock(block []byte) ( PGBlockBody, error) {

	fmt.Println("ReadBlock in: ", string(block))

	var pbblock PGBlockBody

	row, err := Ppgsql.Query("SELECT blockhash, framehash, transactionblockid, " +
								"transactionblockcount " + 
								" FROM blocks " + 
								" WHERE blockhash = ?;", string(block))

	if err != nil {
		fmt.Println("Error reading blocks: ", err)
		return pbblock, err
	} 
	defer row.Close()

	err = row.Scan(&pbblock.Index, &pbblock.StateHash, &pbblock.FrameHash, 
					&pbblock.TransactionsBlockID, &pbblock.TransactionsBlockCnt )
		

	return pbblock, err
}


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
	_, err := Ppgsql.Query("SELECT blockhash from blocks LIMIT 1")

	return err
	
}


//*****************************************************************************************
//	Lets create our tables!!!
//*****************************************************************************************
func CreateLachesisDB() error {
	// can we do this with PostGres????
	return nil
}

func CreateAccounts() error {

	q := `	CREATE TABLE accounts (
 				account VARCHAR (50),
 				address VARCHAR (50),
 				publicKey VARCHAR (50),
 				privateKey VARCHAR (50),
 				account_datetime VARCHAR (50)
			);`
	
	_, err := Ppgsql.Exec(q)

//	fmt.Println("accounts: ", err)
	
	return err
}

func CreateTransaction() error {
	
	q := `	CREATE TABLE transactions (
 				transaction VARCHAR (50),
 				transactionblockid VARCHAR (50),
 				transaction_datetime VARCHAR (50)
			);`
	
	_, err := Ppgsql.Exec(q)

//	fmt.Println("transactions: ", err)
	
	return err
}

func CreateAccountTrans() error {
	
	q := `	CREATE TABLE accounttransactions (
 				account VARCHAR (50),
 				transaction VARCHAR (50),
 				at_DateTime VARCHAR (50)
			);`	
 				// address VARCHAR (50),    <--  removed as account should be sufficient


	_, err := Ppgsql.Exec(q)

//	fmt.Println("accounttransactions: ", err)
	
	return err
}

func CreateBlock() error {
	
	q := `	CREATE TABLE blocks (
 				blockhash VARCHAR (50),
 				framehash VARCHAR (50),
 				transactionblockid VARCHAR (50),
 				transactionblockcount int,
 				block_DateTime VARCHAR (50)
			);`
	
	_, err := Ppgsql.Exec(q)

//	fmt.Println("block: ", err)
	
	return err
}


//*****************************************************************************************
//	Drop tables
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

	return err
}



func DropAccounts() error {

	fmt.Println("DropAccounts")

	_, err := Ppgsql.Exec("DROP TABLE accounts")

	return err
}

func DropTransaction() error {
	_, err := Ppgsql.Query("DROP TABLE transactions")

	return err
	
}

func DropAccountTrans() error {
	_, err := Ppgsql.Query("DROP TABLE accounttransactions")

	return err
	
}

func DropBlock() error {
	_, err := Ppgsql.Query("DROP TABLE blocks")

	return err
	
}



//*****************************************************************************************
//	Drop tables
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

	//  This is a problem. Ppgsql is not maintaining a connection. Defaq?
	Ppgsql = db
	
	//  Due to the problem with maintaining the connection, I'm commenting out the db.Close()
//	defer db.Close()

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
