var DbDiff = require('../dbdiff')
var assert = require('assert')
var pync = require('pync')

class Utils {
  constructor (dialect, conn1, conn2) {
    var Client = require(`../dialects/${dialect}-client`)
    this.dialect = dialect
    this.conn1 = conn1
    this.conn2 = conn2
    this.client1 = new Client(conn1)
    this.client2 = new Client(conn2)
  }

  resetDatabases () {
    return Promise.all([
      this.client1.dropTables(),
      this.client2.dropTables()
    ])
  }

  runCommands (commands1, commands2) {
    return this.resetDatabases()
      .then(() => Promise.all([
        pync.series(commands1, (command) => this.client1.query(command)),
        pync.series(commands2, (command) => this.client2.query(command))
      ]))
  }

  runAndCompare (commands1, commands2, expected, levels = ['drop', 'warn', 'safe']) {
    var dbdiff = new DbDiff()
    return pync.series(levels, (level) => {
      return this.runCommands(commands1, commands2)
        .then(() => dbdiff.compare(this.conn1, this.conn2))
        .then(() => assert.equal(dbdiff.commands(level), expected))
        .then(() => this.client1.query(dbdiff.commands(level)))
        .then(() => dbdiff.compare(this.conn1, this.conn2))
        .then(() => {
          var lines = dbdiff.commands(level).split('\n')
          lines.forEach((line) => {
            if (line.length > 0 && line.substring(0, 2) !== '--') {
              assert.fail(`After running commands there is a change not executed: ${line}`)
            }
          })
        })
    })
  }
}

module.exports = (dialect, conn1, conn2) => {
  return new Utils(dialect, conn1, conn2)
}
