var pg = require('pg')

class PostgresClient {
  constructor (conString) {
    this.client = new pg.Client(conString)
  }

  connect () {
    return new Promise((resolve, reject) => {
      if (this.transaction) return resolve()
      this.client.connect((err, transaction, done) => {
        if (err) return reject(err)
        this.transaction = transaction
        this.done = done
        resolve()
      })
    })
  }

  query (sql, params = []) {
    return this.connect()
      .then(() => {
        return new Promise((resolve, reject) => {
          this.transaction.query(sql, params, (err, result) => {
            err ? reject(err) : resolve(result)
          })
        })
      })
  }

  find (sql, params = []) {
    return this.query(sql, params).then((result) => result.rows)
  }

  findOne (sql, params = []) {
    return this.query(sql, params).then((result) => result.rows[0])
  }

  end () {
    this.transaction = null
    this.done && this.done()
  }
}

module.exports = PostgresClient
