const { makeDb } = require('mysql-async-simple');
const mysql = require("mysql");

let config = {
    host: 'localhost',
    user: 'root',
    password: 'root',
    database: 'test'
}


const connection = mysql.createConnection(config);
const db = makeDb();


saveMessageToTables = async (message, offset = 0) => {
  let insertStatement1 = ` insert into messages_table_1 (message,offset) values ('${JSON.stringify(message)}', ${offset})`;
  let insertStatement2 = ` insert into messages_table_2 (message,offset) values ('${JSON.stringify(message)}', ${offset})`;

  db.beginTransaction(connection);
  try {
    db.query(connection, insertStatement1);
    db.query(connection, insertStatement2);
    db.commit(connection);
  } catch (error) {
    console.log('error occured in the transaction');
    db.rollback(connection)
    throw Error("error occurred in completing transaction");
  }
};

fetchLatestOffset() = async () =>{
  let getStatement = ` select offset from messages_table_1 order by created_at desc limit 1`;
  let resp = db.query(connection, getStatement);
  console.log(resp);

}

module.exports ={
    saveMessageToTables,
    fetchLatestOffset
}

