const { Pool } = require("pg");
const DROP_SCHEMA_QUERY = "drop schema public cascade; create schema public;";
class PostgresClient {
  constructor(connOptions) {
    this.connconnOptions = connOptions;
  }

  buildPool() {
    this.pool = new Pool({
      ...this.connconnOptions,
      ssl: { rejectUnauthorized: false },
    });
    return this.pool;
  }

  dropTables() {
    return this.query(DROP_SCHEMA_QUERY);
  }

  async connect() {
    if (!this.pool) {
      this.buildPool();
      const pgClient = await this.pool.connect();
      debugger;
      return pgClient;
    }
    const pgClient = await this.pool.connect();
    debugger;
    return pgClient;
  }

  async query(sql, params = []) {
    const pgClient = await this.connect();
    const resultSet = await pgClient.query(sql, params);
    const shouldDestroy = true;
    pgClient.release(shouldDestroy);
    return resultSet;
  }

  find(sql, params) {
    return this.query(sql, params).then((result) => result.rows);
  }
  findOne(sql, params) {
    return this.query(sql, params).then((result) => result.rows[0]);
  }
}

module.exports = PostgresClient;
