var assert = require('better-assert');
var async = require('async');
var events = require('events');
var database = require('./database');
var util = require('util');
var lib = require('./lib');
var logger = require('winston');
var bigInt = require("big-integer");
var Decimal = require('decimal.js');

function Staker(staketotal, stakecount, orphancount, held, game) {
    var self = this;
    self.stakecount = stakecount;
    self.staketotal = new Decimal(staketotal);
    self.orphan_count = orphancount;
    self.held = new Decimal(held);

    self.state = 'RUNNING';

    //self.joined = new SortedArray(); // A list of joins, before the game is in progress


    events.EventEmitter.call(self);

    function processStakes() {


        database.getStake(function(err, result) {
            if (err) {
                logger.info(err);
                return;
            }

            self.held = new Decimal(result.held)

            database.getUnprocessedStakes(function(err, data) {
                if (err) {
                    logger.info(err);
                    return;
                }

                if (data.length === 0)
                    return setTimeout(processStakes, 30 * 1000);

                if (game.gameShuttingDown) {
                    logger.info("Game shutting down, stopping stakefinder..")
                    return;
                }

                logger.info('Unprocessed stakes found..', data.length);

                var sum = 0;
                var processed = [];

                // amount = -100000000
                for (i in data) {
                    processed.push(data[i].id);
                    sum = new Decimal(sum.toString()).plus(data[i].amount.toString());
                    // sum = -10000000

                    if (data[i].status === "orphan") {
                        self.stakecount--;
                        self.orphan_count++;
                        self.staketotal = new Decimal(self.staketotal).plus(data[i].amount);

                        var msg = {
                            "total": self.staketotal.toNumber(),
                            "amount": data[i].amount,
                            "status": "orphan"
                        };

                        logger.info('Stake processed:.. ', msg);

                    } else {
                        self.stakecount++;
                        self.staketotal = new Decimal(self.staketotal).plus(data[i].amount);

                        var msg = {
                            "total": self.staketotal.toNumber(),
                            "amount": data[i].amount,
                            "status": "stake"
                        };
                        
                        logger.info('Stake processed:.. ', msg);
                    }
                }

                if (self.staketotal.lessThan(0) || self.stakecount < 0) {
                    logger.info('STAKE TOTAL ERROR - BELOW ZERO .. should be impossible');
                    return;
                }

                if (sum.lessThan(0)) {
                    self.held = new Decimal(self.held).plus(sum);
                    sum = new Decimal(0);
                } else {
                    if (self.held.lessThan(0) && sum.greaterThanOrEqualTo(self.held.times(-1))) {
                        sum = new Decimal(sum).plus(self.held);
                        self.held = new Decimal(0);
                    } else if (self.held.lessThan(0) && self.held.times(-1).minus(sum).greaterThan(0)) {
                        self.held = new Decimal(self.held).plus(sum);
                        sum = new Decimal(0)
                    }
                }

                var dadata = {
                    sum: sum.toNumber(),
                    staketotal: self.staketotal.toNumber(),
                    held: self.held.toNumber(),
                    stakecount: self.stakecount,
                    orphan_count: self.orphan_count
                };

                database.processStakes(processed, dadata, function(err, result) {
                    if (err)
                        logger.info(err);


                    if (sum.greaterThan(0)) {
                        self.emit('msg', {
                            time: new Date(),
                            type: 'info',
                            message: "we just staked " + sum.dividedBy(1e14).toFixed(8) + " CLAM (total = " + self.staketotal.dividedBy(1e14).toFixed(8) + " )"
                        });
                    }

                    setTimeout(processStakes, 30 * 2000);

                });
            });
        });
    }

    processStakes();
}

util.inherits(Staker, events.EventEmitter);
module.exports = Staker;