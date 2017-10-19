var pg = require('pg')
var querystring = require('querystring')

class PostgresClient {
  constructor (conOptions) {
    this.conOptions = conOptions
  }

  dropTables () {
    return this.query('drop schema public cascade; create schema public;')
  }

  connect () {
    return new Promise((resolve, reject) => {
      if (this.client) return resolve()
      pg.connect(this.conOptions, (err, client, done) => {
        if (err) return reject(err)
        this.client = client
        this.done = done
        resolve()
      })
    })
  }

  query (sql, params = []) {
    return this.connect()
      .then(() => {
        return new Promise((resolve, reject) => {
          this.client.query(sql, params, (err, result) => {
            this.done()
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
}

module.exports = PostgresClient
