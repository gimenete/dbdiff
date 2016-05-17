#!/usr/bin/env node

var DbDiff = exports.DbDiff = require('./dbdiff')

exports.describeDatabase = require('./dialects').describeDatabase

if (module.id === require.main.id) {
  var yargs = require('yargs')
  var argv = yargs
      .usage('Usage: $0 conn_string1 conn_string2')
      .example('$0 postgres://user:pass@host[:port]/dbname1 postgres://user:pass@host[:port]/dbname2',
        'compares the scheme of two databases and prints the SQL commands to modify the first one in order to match the second one')
      .demand(2)
      .wrap(yargs.terminalWidth())
      .help('h')
      .alias('h', 'help')
      .option('level', {
        alias: 'l',
        describe: 'chooses the safety of the sql',
        choices: ['safe', 'warn', 'drop']
      })
      .argv

  var conn1 = argv._[0]
  var conn2 = argv._[1]
  var dbdiff = new DbDiff()
  dbdiff.compare(conn1, conn2)
    .then(() => {
      console.log(dbdiff.commands(argv.level))
      process.exit(0)
    })
    .catch((err) => {
      console.error(err.stack)
      process.exit(1)
    })
}
