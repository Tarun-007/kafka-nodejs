const { makeDb } = require("mysql-async-simple");
const mysql = require("mysql");

let config = {
  host: "localhost",
  user: "root",
  password: "root",
  database: "test",
};

const connection = mysql.createConnection(config);
const db = makeDb();

function getConnection() {
  return connection;
}

function getDb() {
  return db;
}

async function saveMessageToTables(message, offset = 0) {
  let insertStatement1 = ` insert into messages_table_1 (message,offset) values ('${JSON.stringify(
    message
  )}', ${offset})`;
  let insertStatement2 = ` insert into messages_table_2 (message,offset) values ('${JSON.stringify(
    message
  )}', ${offset})`;

  await db.query(connection, insertStatement1);
  await db.query(connection, insertStatement2);
}

async function fetchLatestOffset(){
  let getStatement = ` select offset from messages_table_1 order by created_at desc limit 1`;
  let resp = await db.query(connection, getStatement);
  if (resp.length == 0) {
    return null;
  }
  console.log(`latest offset fetched ${resp[0].offset} \n`);
  return resp[0].offset;
};

module.exports = {
  saveMessageToTables,
  fetchLatestOffset,
  getConnection,
  getDb,
};
