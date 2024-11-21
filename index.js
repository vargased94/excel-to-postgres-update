const XLSX = require('xlsx');
const { Client } = require('pg');
const dotenv = require("dotenv");
dotenv.config();

const query = `SELECT id, name FROM drivers
WHERE name = $1 LIMIT 1;`;
const unitQuery = `SELECT eco_number, field FROM units
WHERE eco_number = $1 LIMIT 1;`;

const workbook = XLSX.readFile('complementaria.xlsx');
const sheetName = workbook.SheetNames[0];
const worksheet = workbook.Sheets[sheetName];

const unitWorkbook = XLSX.readFile('fieldtps.xlsx');
const unitSheetName = unitWorkbook.SheetNames[0];
const unitWorksheet = unitWorkbook.Sheets[unitSheetName];

const data = XLSX.utils.sheet_to_json(worksheet);
const units = XLSX.utils.sheet_to_json(unitWorksheet);

const client = new Client({
  user: process.env.POSTGRES_USER,
  host: process.env.POSTGRES_HOST,
  database: process.env.POSTGRES_DB,
  password: process.env.POSTGRES_PASSWORD,
  port: process.env.POSTGRES_PORT,
  query_timeout: 300000 // 5 minutes
});

const updateDrivers = async () => {
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

client.on('error', (err) => {
  console.error("Error while connecting to database -> ", err);
});

client.connect()
  .then(() => {
    console.log("Connected to database successfully");
    // updateDrivers().finally(() => client.end());
    const updateUnits = async () => {
      try {
        // Fetch existing columns from the table
        const tableCheck = await client.query(`
          SELECT column_name 
          FROM information_schema.columns 
          WHERE table_name = 'units'
        `);
        const existingColumns = tableCheck.rows.map(r => r.column_name);
    
        for (let row of units) {
          const eco_number = row['eco_number'];
    
          // Filter valid columns
          const validColumns = Object.keys(row).filter(key => 
            existingColumns.includes(key.toLowerCase())
          );
    
          if (validColumns.length === 0) {
            console.warn(`No matching columns found for unit ${eco_number}`);
            continue;
          }
    
          const setClause = validColumns
            .map(key => `${key.toLowerCase()} = $${validColumns.indexOf(key) + 2}`)
            .join(', ');
    
          const values = [eco_number, ...validColumns.map(key => row[key])];
    
          const query = `
            UPDATE units 
            SET ${setClause}
            WHERE eco_number = $1
          `;
    
          const result = await client.query(query, values);
          if (result.rowCount) {
            console.log(`Updated unit ${eco_number}: ${result.rowCount} rows`);
          }
        }
      } catch (err) {
        console.error("Error while executing script -> ", err);
      }
    };
    updateUnits().finally(() => client.end());
  })
  .catch(err => console.error("Error while connecting to database -> ", err));