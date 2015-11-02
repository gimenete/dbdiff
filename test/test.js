var dbdiff = require('../index')

describe('dbdiff.compareSchemas', function() {

  var db1 = {
    tables: {},
    indexes: [],
    sequences: [],
  }
  
  var db2 = {
    tables: {},
    indexes: [],
    sequences: [],
  }

  it('should create SQL statements given two database schemas', function() {
    dbdiff.compareSchemas(db1, db2)
  })

})