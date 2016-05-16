var Client = require('../client')
var dbdiff = require('../')
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
  return Promise.all([
    pync.series(commands1, (command) => client1.query(command)),
    pync.series(commands2, (command) => client2.query(command))
  ])
}

exports.runAndCompare = (commands1, commands2, expected) => {
  var arr = []
  dbdiff.logger = (msg) => {
    if (msg) {
      arr.push(msg)
    }
  }
  return exports.runCommands(commands1, commands2)
    .then(() => dbdiff.compareDatabases(conString1, conString2))
    .then(() => client1.query(arr.join('\n')))
    .then(() => {
      assert.deepEqual(arr, expected)
      // compare again the dbs
      arr.splice(0)
      return dbdiff.compareDatabases(conString1, conString2)
    })
    .then(() => assert.deepEqual(arr, []))
}
