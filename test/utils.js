var Client = require('../dialects/postgres-client')
var DbDiff = require('../dbdiff')
var assert = require('assert')
var pync = require('pync')

var conString1 = 'postgres://postgres:postgres@localhost/db1'
var conString2 = 'postgres://postgres:postgres@localhost/db2'

var client1 = new Client(conString1)
var client2 = new Client(conString2)

exports.resetDatabases = () => {
  return Promise.all([
    client1.query('drop schema public cascade; create schema public;'),
    client2.query('drop schema public cascade; create schema public;')
  ])
}

exports.runCommands = (commands1, commands2) => {
  return exports.resetDatabases()
    .then(() => Promise.all([
      pync.series(commands1, (command) => client1.query(command)),
      pync.series(commands2, (command) => client2.query(command))
    ]))
}

exports.runAndCompare = (commands1, commands2, expected, levels = ['drop', 'warn', 'safe']) => {
  var dbdiff = new DbDiff()
  return pync.series(levels, (level) => {
    return exports.runCommands(commands1, commands2)
      .then(() => dbdiff.compare(conString1, conString2))
      .then(() => assert.equal(dbdiff.commands(level), expected))
      .then(() => client1.query(dbdiff.commands(level)))
      .then(() => dbdiff.compare(conString1, conString2))
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
