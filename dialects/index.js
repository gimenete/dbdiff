var url = require('url')
var dialects = {}

exports.register = (dialect, clazz) => {
  dialects[dialect] = clazz
}

exports.describeDatabase = (options) => {
  return Promise.resolve()
    .then(() => {
      var dialect = options.dialect
      if (!dialect) {
        if (typeof options === 'string') {
          var info = url.parse(options)
          dialect = info.protocol
          if (dialect && dialect.length > 1) {
            dialect = info.protocol.substring(0, info.protocol.length - 1)
          }
        }
        if (!dialect) {
          return Promise.reject(new Error(`Dialect not found for options ${options}`))
        }
      }
      var clazz = dialects[dialect]
      if (!clazz) {
        return Promise.reject(new Error(`No implementation found for dialect ${dialect}`))
      }
      var obj = new (Function.prototype.bind.apply(clazz, [options]))
      return obj.describeDatabase(options)
    })
}

require('./postgres')
require('./mysql')
