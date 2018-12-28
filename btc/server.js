var async = require('async');
var assert = require('assert');
var constants = require('constants');
var fs = require('fs');
var path = require('path');
var _ = require('lodash');

var config = require('./server/config');
var socket = require('./server/socket');
var database = require('./server/database');
var Game = require('./server/game');
var Chat = require('./server/chat');
var GameHistory = require('./server/game_history');
var version = "1.0.0";

 var winston = require('winston');


 var logger =  winston.add(winston.transports.File, { filename: '/home/user/logs/gameserver-btc-info.log', level: 'info', handleExceptions: true, name: 'info-file' })
                      //.remove(winston.transports.Console);

var server;

if (config.USE_HTTPS) {
    var options = {
        key: fs.readFileSync(config.HTTPS_KEY),
        cert: fs.readFileSync(config.HTTPS_CERT),
        secureProtocol: 'SSLv23_method',
        secureOptions: constants.SSL_OP_NO_SSLv3 | constants.SSL_OP_NO_SSLv2
    };

    if (config.HTTPS_CA) {
        options.ca = fs.readFileSync(config.HTTPS_CA);
    }

    server = require('https').createServer(options).listen(config.PORT, function() {
        logger.info('Listening on port ', config.PORT, ' on HTTPS!');
    });
} else {
    server = require('http').createServer().listen(config.PORT, function() {
        logger.info('Listening on port ', config.PORT, ' with http');
    });
}


async.parallel([
    database.getGameHistory,
    database.getLastGameInfo,
    database.getBankroll,
    database.getLocalDepositAddresses,
    database.getUserList
   // database.getStakeTotals
], function(err, results) {
    console.log(err)
    if (err) {
        logger.info('[INTERNAL_ERROR] got error: ', err,
            'Unable to get table history');
        throw err;
    }

    var gameHistory = new GameHistory(results[0]);
    var info = results[1];
    var bankroll = results[2];
    var depositAddresses = results[3]
    var userIdList = results[4]


    //var stakecount = results[3][0].stake_count;
    //var staketotal = results[3][0].stake_total;
    //var orphancount = results[3][0].orphan_count;
   // var held = results[3][0].held;

    logger.info('Loading Blast Off! Btc', version, 'Have a bankroll of: ', bankroll/1e8, ' btc');

    var lastGameId = info.id;
    var lastHash = info.hash;
    assert(typeof lastGameId === 'number');

    var game = new Game(lastGameId, lastHash, bankroll, gameHistory);
    var chat = new Chat();


    socket(server, game, chat, depositAddresses, userIdList);

});

process.on('uncaughtException', function(err) {
  console.log('Caught exception: ' + err);
});
