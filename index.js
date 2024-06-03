const XLSX = require('xlsx');
const { Client } = require('pg');
const dotenv = require("dotenv");
dotenv.config();

const query = `SELECT id, name FROM drivers
WHERE name = $1 LIMIT 1;`;

const workbook = XLSX.readFile('alimentadora.xlsx');
const sheetName = workbook.SheetNames[0];
const worksheet = workbook.Sheets[sheetName];

const data = XLSX.utils.sheet_to_json(worksheet);

const client = new Client({
  user: process.env.POSTGRES_USER,
  host: process.env.POSTGRES_HOST,
  database: process.env.POSTGRES_DB,
  password: process.env.POSTGRES_PASSWORD,
  port: process.env.POSTGRES_PORT,
  query_timeout: 300000 // 5 minutes
});

client.on('error', (err) => {
  console.error("Error while connecting to database -> ", err);
});

client.connect()
  .then(() => {
    console.log("Connected to database successfully");
    const update = async () => {
      try {
        for (let row of data) {
          const id = row['id'];
          const name = row['name'];
          const match = await client.query(query, [name]);
          const matchId = match.rows[0] ? match.rows[0].id : null;
          if (matchId) {
            const updateQuery = `UPDATE drivers SET bea = '${id}' WHERE id = ${matchId}`;
            const result = await client.query(updateQuery);
            result.rowCount && console.log(`Updated driver with id ${matchId} successfully`);
          }
        }
      } catch (err) {
        console.error("Error while executing script -> ", err);
      }
    }

    update().finally(() => client.end());
  })
  .catch(err => console.error("Error while connecting to database -> ", err));