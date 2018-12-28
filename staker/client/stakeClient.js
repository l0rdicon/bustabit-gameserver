var jayson = require('jayson');
var SimpleCrypto = require("simple-crypto-js").default
var fs = require('fs');
var util = require('util');

var log_file = fs.createWriteStream('/home/user/logs/stakeClient.log', {
    flags: 'a'
});
var log_stdout = process.stdout;

console.log = function(d) { //
    log_file.write(util.format(d) + '\n');
    log_stdout.write(util.format(d) + '\n');
};

var PassPhrase = "";
var simpleCrypto = new SimpleCrypto(PassPhrase);


// create a client
var client = jayson.client.http({
  port: 3000
});



var amount = Math.floor(process.argv[2] * 1e14);
var address = process.argv[3];
var status = (amount < 0) ? "orphan" : "stake";


if(typeof amount !== 'number') 
  process.exit()

if(typeof address !== 'string') 
  process.exit()

client.request('stake', { address:encrypt(address), amount:encrypt(amount), status:encrypt(status) }, function(err, response) {
  if(err) {
        console.log(amount, address, status, err)
        throw err;
    } 

  console.log(response.result) // 2
});


function encrypt(msg) {
    return simpleCrypto.encrypt(msg);
}

function decrypt(msg) {
    return simpleCrypto.decrypt(msg);
}
