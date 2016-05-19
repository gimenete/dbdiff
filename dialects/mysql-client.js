var mysql = require('mysql')

class MysqlClient {
  constructor (options) {
    this.options = Object.assign({
      user: options.username,
      multipleStatements: true
    }, options)
    this.pool = mysql.createPool(this.options)
  }

  dropTables () {
    return this.findOne(`
      SELECT concat('DROP TABLE IF EXISTS ', table_name, ';') AS fullSQL
      FROM information_schema.tables
      WHERE table_schema = ?;
    `, [this.options.database])
      .then((result) => result && this.query(result.fullSQL))
  }

  query (sql, params = []) {
    return new Promise((resolve, reject) => {
      this.pool.query(sql, params, (err, rows, fields) => {
        if (err) console.log('-> ', sql, params)
        err ? reject(err) : resolve({ rows, fields })
      })
    })
  }

  find (sql, params = []) {
    return this.query(sql, params).then((result) => result.rows)
  }

  findOne (sql, params = []) {
    return this.query(sql, params).then((result) => result.rows[0])
  }
}

module.exports = MysqlClient
