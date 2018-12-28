var fs = require('fs');
var util = require('util');
var jayson = require('jayson');
var SimpleCrypto = require("simple-crypto-js").default;
var pg = require('pg');

var log_file = fs.createWriteStream('/home/user/logs/stakeServer.log', {
    flags: 'a'
});
var log_stdout = process.stdout;

console.log = function(d) { //
    log_file.write(util.format(d) + '\n');
    log_stdout.write(util.format(d) + '\n');
};

var databaseUrl = 'postgres://'
var PassPhrase = "";
var simpleCrypto = new SimpleCrypto(PassPhrase);

// create a server
var server = jayson.server({
    stake: function(address, amount, status, callback) {

        var address = decrypt(address)
        var amount = decrypt(amount)
        var status = decrypt(status)

        console.log(address)
        stake(address,amount,status, function(err){ 
            if(err) {
                console.log(err) 
                return callback("error")
            }

            callback(null, "complete");
        })
        
        callback(null, "complete");
    }
},
    {
        collect: false // don't collect params in a single argument
    });

server.http().listen(3000);


function encrypt(msg) {
    return simpleCrypto.encrypt(msg);
}

function decrypt(msg) {
    return simpleCrypto.decrypt(msg);
}


pg.types.setTypeParser(20, function(val) { // parse int8 as an integer
    return val === null ? null : parseInt(val);
});

// callback is called with (err, client, done)
function connect(callback) {
    return pg.connect(databaseUrl, callback);
}

function getClient(runner, callback) {
    connect(function(err, client, done) {
        if (err) return callback(err);

        function rollback(err) {
            client.query('ROLLBACK', done);
            callback(err);
        }

        client.query('BEGIN', function(err) {
            if (err)
                return rollback(err);

            runner(client, function(err, data) {
                if (err)
                    return rollback(err);

                client.query('COMMIT', function(err) {
                    if (err)
                        return rollback(err);

                    done();
                    callback(null, data);
                });
            });
        });
    });
}

function stake(address, amount, status, callback) {

    getClient(function(client, callback) {

        client.query('INSERT into stake_history(address, amount, status) VALUES($1, $2, $3)', [address, amount, status], function(err) {
            if (err) return callback('error insert stake history orphan ' + err);

            var a = 'Stake event.. ' + '( ' + address + ' ' + amount + ' ' + status + ' )';
            console.log(a);
            return callback(null);
        });


    }, callback);

}
