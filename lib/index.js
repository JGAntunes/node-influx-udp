var dgram = require('dgram');

var _ = require('lodash');

var InfluxUdp = function influxUdp(opts) {
  opts = opts || {};
  this.host = opts.host || '127.0.0.1';
  this.port = opts.port || 4444;
  this.database = opts.database;
  this.retentionPolicy = opts.retentionPolicy;
  this.socket = dgram.createSocket('udp4');
};

function keysToValues(columns) {
  return function(values) {
    return columns.map(function(column) {
      return values[column];
    });
  };
}

InfluxUdp.prototype.send = function influxSend(seriesName, values, tags, options, callback) {
  callback = callback || options;
  if (typeof options === 'function'){
    options = {};
  }

  if(!Array.isArray(values)){
    values = [values];
  }
  var message;
  var data = {points: []};
  _.each(values, function(p){
    var dataValue = {};
    dataValue.fields = p;
    dataValue.name = p.name || seriesName;
    data.points.push(dataValue);
  });
  data.tags = tags;
  data.database = options.database || this.database;
  data.retentionPolicy = options.retentionPolicy || this.retentionPolicy;

  message = new Buffer(JSON.stringify(data));
  this.socket.send(message, 0, message.length, this.port, this.host);

  return callback(null, data);
};

module.exports = InfluxUdp;
