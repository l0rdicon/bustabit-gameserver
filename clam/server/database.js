var assert = require('assert');
var uuid = require('uuid');

var async = require('async');
var lib = require('./lib');
var pg = require('pg');
var config = require('./config');
var _ = require('lodash');
var logger = require('winston');
var bigInt = require("big-integer");
var Decimal = require('decimal.js');
var currentWeekNumber = require('current-week-number');


if (!config.DATABASE_URL)
    throw new Error('must set DATABASE_URL environment var');


// Increase the client pool size. At the moment the most concurrent
// queries are performed when auto-bettors join a newly created
// game. (A game is ended in a single transaction). With an average
// of 25-35 players per game, an increase to 20 seems reasonable to
// ensure that most queries are submitted after around 1 round-trip
// waiting time or less.
pg.defaults.poolSize = 20;

// The default timeout is 30s, or the time from 1.00x to 6.04x.
// Considering that most of the action happens during the beginning
// of the game, this causes most clients to disconnect every ~7-9
// games only to be reconnected when lots of bets come in again during
// the next game. Bump the timeout to 2 min (or 1339.43x) to smooth
// this out.
pg.defaults.poolIdleTimeout = 120000;

var commissionPercentage = 10;
var commissionStakePercentage = 10;

pg.types.setTypeParser(20, function(val) { // parse int8 as an integer
    return val === null ? null : parseInt(val);
});

pg.types.setTypeParser(1700, function(val) { // parse numeric as a float

    return val === null ? null : bigInt(val.toString()).toString();
});

// callback is called with (err, client, done)
function connect(callback) {
    return pg.connect(config.DATABASE_URL, callback);
}

function query(query, params, callback) {
    //third parameter is optional
    if (typeof params == 'function') {
        callback = params;
        params = [];
    }

    doIt();

    function doIt() {
        connect(function(err, client, done) {
            if (err) return callback(err);
            client.query(query, params, function(err, result) {
                done();
                if (err) {
                    if (err.code === '40P01') {
                        logger.info('Warning: Retrying deadlocked transaction: ', query, params);
                        return doIt();
                    }
                    return callback(err);
                }

                callback(null, result);
            });
        });
    }
}

function getClient(runner, callback) {
    doIt();

    function doIt() {
        connect(function(err, client, done) {
            if (err) return callback(err);

            function rollback(err) {
                client.query('ROLLBACK', done);

                if (err.code === '40P01') {
                    logger.info('Warning: Retrying deadlocked transaction..');
                    return doIt();
                }

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
}


exports.query = query;

pg.on('error', function(err) {
    logger.info('POSTGRES EMITTED AN ERROR', err);
});

// runner takes (client, callback)

// callback should be called with (err, data)
// client should not be used to commit, rollback or start a new transaction

// callback takes (err, data)

exports.getLastGameInfo = function(callback) {
    query('SELECT MAX(id) id FROM games WHERE coin = \'clam\'', function(err, results) {
        if (err) {
            return callback(err);
        }
        assert(results.rows.length === 1);
        var id = results.rows[0].id;

        if (!id || id === 0) {
            return callback(null, {
                id: 2e7 - 1,
                hash: '0ffc525e68175b00749475f647ce550984d0fd757035e8d942a72421be6ea231'
            });
        }

        query('SELECT hash FROM game_hashes WHERE game_id = $1 AND coin = \'clam\'', [id], function(err, results) {
            if (err) {
                return callback(err);
            }

            assert(results.rows.length === 1);
            callback(null, {
                id: id,
                hash: results.rows[0].hash
            });
        });
    });
};

exports.getUserByName = function(username, callback) {
    assert(username);
    query('SELECT * FROM users WHERE lower(username) = lower($1)', [username], function(err, result) {
        if (err) return callback(err);
        if (result.rows.length === 0)
            return callback('USER_DOES_NOT_EXIST');

        assert(result.rows.length === 1);
        callback(null, result.rows[0]);
    });
};


/*
exports.makeWithdraw = function(userId, address, amount, isLocalUserId, speech, callback) {
    // if its a local address we can just do it all now, no need to be queued
    if (!_.isEmpty(isLocal)) {
        amount = amount * 1e6;
        getClient(function(client, callback) {
            var tasks = [
                function(callback) {
                    client.query('UPDATE users SET balance_satoshis = balance_satoshis - $1 WHERE id = $2', [amount, userId], callback);
                },
                function(callback) {
                    client.query('UPDATE users SET balance_satoshis = balance_satoshis + $1 WHERE id = $2', [amount, isLocalUserId], callback);
                },
                function(callback) {
                    client.query(
                        'INSERT INTO deposits(user_id, amount, moved, vout, address, txid, confirmations) VALUES($1, $2, $3, $4, $5, $6, $7)', [isLocalUserId, amount, 'OFFCHAIN', 9999, address, 'OFFCHAIN', 6], callback);
                }
            ];

            async.parallel(tasks, function(err, result) {
                if (err)
                    return callback(err);

                callback(null, 'offchain withdraw complete');
            });
        }, callback);
    } else {
        getClient(function(client, callback) {
            client.query("UPDATE users SET balance_satoshis = balance_satoshis - $1 WHERE id = $2", [amount, userId], function(err, response) {
                if (err) return callback(err);

                if (response.rowCount !== 1)
                    return callback(new Error('Unexpected withdrawal row count: \n' + response));

                client.query('INSERT INTO withdrawals(user_id, amount, address, withdrawal_id) ' +
                    "VALUES($1, $2, $3, $4) RETURNING id", [userId, -1 * amount, address, uuid.v4()],
                    function(err, response) {
                        if (err) return callback(err);

                        var fundingId = response.rows[0].id;
                        assert(typeof fundingId === 'number');

                        callback(null, 'offchain withdraw queued');
                    }
                );
            });
        }, callback);
    }
}
*/

exports.validateOneTimeToken = function(token, callback) {
    assert(token);

    query('WITH t as (UPDATE sessions SET expired = now() WHERE id = $1 AND ott = TRUE RETURNING *)' +
        'SELECT * FROM users WHERE id = (SELECT user_id FROM t)', [token],
        function(err, result) {
            if (err) return callback(err);
            if (result.rowCount == 0) return callback('NOT_VALID_TOKEN');
            assert(result.rows.length === 1);
            result.rows[0].balance_satoshis_clam = Math.floor(result.rows[0].balance_satoshis_clam / 1e6)
            result.rows[0].balance_satoshis_invested_clam = Math.floor(result.rows[0].balance_satoshis_invested_clam / 1e6)
            result.rows[0].balance_satoshis_btc = Math.floor(result.rows[0].balance_satoshis_btc / 1e6)
            result.rows[0].balance_satoshis_invested_btc = Math.floor(result.rows[0].balance_satoshis_invested_btc / 1e6)
            callback(null, result.rows[0]);
        }
    );
};

exports.lockUser = function(id, callback) {
    query('UPDATE users SET frozen = true WHERE id = $1', [id], function(err, result) {
        if (err) return callback(err);
        if (result.rowCount == 0) return callback('NO_USER');

        query('UPDATE sessions SET expired = now() WHERE user_id = $1 AND expired > now()', [id], function(err) {

            if (err) return callback(err);
            return callback(null)
         });
    });
}

exports.unlockUser = function(id, callback) {
    query('UPDATE users SET frozen = false WHERE id = $1', [id], function(err, result) {
        if (err) return callback(err);
        if (result.rowCount == 0) return callback('NO_USER');

        return callback(null)
    });
}

//do it here
exports.placeBet = function(amount, autoCashOut, userId, gameId, callback) {
    assert(typeof amount === 'number');
    assert(typeof autoCashOut === 'number');
    assert(typeof userId === 'number');
    assert(typeof gameId === 'number');
    assert(typeof callback === 'function');

    amount = amount * 1e6;

    getClient(function(client, callback) {
        var tasks = [
            function(callback) {
                client.query('UPDATE users SET balance_satoshis_clam = balance_satoshis_clam - $1, wagered_clam = wagered_clam + $1 WHERE id = $2', [amount, userId], callback);
            },
            function(callback) {
                client.query(
                    'INSERT INTO plays(user_id, game_id, bet, auto_cash_out) VALUES($1, $2, $3, $4) RETURNING id', [userId, gameId, amount, autoCashOut], callback);
            }
        ];

        async.parallel(tasks, function(err, result) {
            if (err)
                return callback(err);

            var playId = result[1].rows[0].id;
            assert(typeof playId === 'number');

            callback(null, playId);
        });
    }, callback);
};



exports.getUnprocessedStakes = function(callback) {
    query('SELECT * FROM stake_history WHERE processed IS NULL ORDER BY created DESC', function(err, results) {
        if (err) return callback(err);
        callback(null, results.rows);
    });
};


exports.getStakeTotals = function(callback) {
    query('SELECT * FROM staking', function(err, result) {
        if (err) return callback(err);

        var count = result.rows;
        //assert(typeof count === 'number');

        callback(null, count);
    });
}

exports.getStake = function(callback) {
    query('SELECT * FROM staking', function(err, result) {
        if (err) return callback(err);

        //assert(typeof count === 'number');

        callback(null, result.rows[0]);
    });
}

exports.getAllChatTable = function(limit, callback) {
    assert(typeof limit === 'number');
    var sql = "SELECT * FROM (SELECT chat_messages.user_id as uid, chat_messages.id as id, chat_messages.created AS time, 'say' AS type, users.username, users.userclass AS role, chat_messages.message FROM chat_messages JOIN users ON users.id = chat_messages.user_id ORDER BY chat_messages.id DESC LIMIT $1) as t ORDER BY id ASC";
    query(sql, [limit], function(err, data) {
        if (err)
            return callback(err);
        callback(null, data.rows);
    });
};

exports.addChatMessage = function(userId, created, message, callback) {
    var sql = 'INSERT INTO chat_messages (user_id, created, message) values($1, $2, $3)';
    query(sql, [userId, created, message], function(err, res) {
        if (err)
            return callback(err);

        assert(res.rowCount === 1);

        callback(null);
    });
};


exports.getLocalDepositAddresses = function(callback) {
    query('SELECT * FROM addresses', function(err, results) {
        if (err) {
            err.wtf = 'getLocalDepositAddresses'
            return callback(err);
        }
        var array = {}
        for (var i = 0; i < results.rows.length; i++) {
            array[results.rows[i].address] = results.rows[i].user_id
        }
        callback(null, array);
    });
}


var stakeProfit =
    'WITH investeduser AS ( SELECT * FROM users WHERE balance_satoshis_invested_clam > 999999 ),' +
    '    onsite AS ( SELECT COALESCE(SUM(balance_satoshis_invested_clam),0) as total FROM users )' +
    'UPDATE users SET' +
    '    balance_satoshis_invested_clam = investeduser.balance_satoshis_invested_clam + ( $1 * (   (investeduser.balance_satoshis_invested_clam ) / (onsite.total)   )),' +
    '    staking_profit = investeduser.staking_profit + ( $2 * (   (investeduser.balance_satoshis_invested_clam) / (onsite.total)))' +
    'FROM investeduser,onsite WHERE users.id = investeduser.id RETURNING users.id, users.staking_profit, investeduser.staking_profit as old_profit';


exports.processStakes = function(processed, data, callback) {

    var tasks = [];
    var comm = 0;

    tasks.push(function(callback) {
        query('UPDATE staking SET held = $1, stake_count = $2, stake_total = $3, orphan_count = $4', [data.held, data.stakecount, data.staketotal, data.orphan_count], callback);
    });

    Object.keys(processed).forEach(function(key, idx) {
        tasks.push(function(callback) {
            query('UPDATE stake_history SET processed = now() WHERE id=$1', [processed[idx]], callback);
        });
    });

    if (data.sum > 0) {
        comm = Math.floor((commissionStakePercentage * data.sum) / 100)
        var total = Math.floor(data.sum - comm);

        tasks.push(function(callback) {
            query(stakeProfit, [total, total],
                function(err, results) {
                    if (err) return callback(err);

                    callback(null);
                });
        });
    }

    if (comm > 0) {
        tasks.push(function(callback) {
            query('UPDATE users SET balance_satoshis_clam = balance_satoshis_clam + $1 WHERE id = 7', [comm], callback);
        });

        tasks.push(function(callback) {
            var uid = uuid.v4();
            query('INSERT INTO commissions(id, user_id, amount, reason) VALUES($1, $2, $3, $4)', [uid, 7, comm, "stake-commission"], callback);
        });
    }

    async.series(tasks, function(err, results) {
        if (err) return callback(err);

        return callback(null);
    });

}



exports.validateOtp = function(id, otp, callback) {
    assert(id);

    query('SELECT mfa_secret FROM users WHERE id = $1', [id], function(err, user) {
        if (err) return callback(err);

        if (user.rows.length === 0)
            return callback('NO_USER');

        if (user.mfa_secret) {
            if (!otp) return callback('INVALID_OTP'); // really, just needs one

            var expected = speakeasy.totp({
                key: user.mfa_secret,
                encoding: 'base32'
            });

            if (otp !== expected)
                return callback('INVALID_OTP');
        }

        callback(null);
    });
};


exports.getBalance = function(user, callback) {
    query('SELECT balance_satoshis_clam FROM users WHERE id = $1', [user],
        function(err, results) {
            if (err) {
                return callback(err);
            }

            return callback(null, Math.floor(results.rows[0].balance_satoshis_clam / 1e6));
        });
};




exports.getUserList = function(callback) {
    query('SELECT id, username FROM users', function(err, results) {
        if (err) return callback(err);
        var array = {}
        for (var i = 0; i < results.rows.length; i++) {
            array[results.rows[i].id] = results.rows[i].username
        }
        callback(null, array);
    });
}


exports.tipUserByIdBtc = function(from, to, amount, callback) {
    var uid = uuid.v4();
    getClient(function(client, callback) {
        var tasks = [
            function(callback) {
                client.query('UPDATE users SET balance_satoshis_btc = balance_satoshis_btc - $1 WHERE id = $2 returning id', [amount, from], callback);
            },
            function(callback) {
                client.query('UPDATE users SET balance_satoshis_btc = balance_satoshis_btc + $1 WHERE id = $2 returning id', [amount, to], callback);
            },
            function(callback) {
                client.query(
                    "INSERT INTO transfers (id, from_user_id, to_user_id, amount, coin) values($1,$2,$3,$4,$5) ", [uid, from, to, amount,'btc'], callback);
            }
        ];

        async.series(tasks, function(err, result) {
            if (err) {
                if (err.code === '23514') { // constraint violation
                    return callback('NOT_ENOUGH_BALANCE');
                }
                if (err.code === '23505') { // dupe key
                    return callback('TRANSFER_ALREADY_MADE');
                }
                return callback(err);
            }

            var fromid = result[0].rows;
            var toid = result[1].rows;

            if (fromid.length === 0)
                return callback('USER_DOES_NOT_EXIST') //this should not tbe possible
            if (toid.length === 0)
                return callback('TO_USER_DOES_NOT_EXIST') //this should not tbe possible


            callback(null);
        });
    }, callback);
}

exports.tipUserByIdClam = function(from, to, amount, callback) {
    var uid = uuid.v4();
    getClient(function(client, callback) {
        var tasks = [
            function(callback) {
                client.query('UPDATE users SET balance_satoshis_clam = balance_satoshis_clam - $1 WHERE id = $2 returning id', [amount, from], callback);
            },
            function(callback) {
                client.query('UPDATE users SET balance_satoshis_clam = balance_satoshis_clam + $1 WHERE id = $2 returning id', [amount, to], callback);
            },
            function(callback) {
                client.query(
                    "INSERT INTO transfers (id, from_user_id, to_user_id, amount, coin) values($1,$2,$3,$4,$5) ", [uid, from, to, amount,'clam'], callback);
            }
        ];

        async.series(tasks, function(err, result) {
            if (err) {
                if (err.code === '23514') { // constraint violation
                    return callback('NOT_ENOUGH_BALANCE');
                }
                if (err.code === '23505') { // dupe key
                    return callback('TRANSFER_ALREADY_MADE');
                }
                return callback(err);
            }

            var fromid = result[0].rows;
            var toid = result[1].rows;

            if (fromid.length === 0)
                return callback('USER_DOES_NOT_EXIST') //this should not tbe possible
            if (toid.length === 0)
                return callback('TO_USER_DOES_NOT_EXIST') //this should not tbe possible


            callback(null);
        });
    }, callback);
}

exports.tipUserById = function(from, to, amount, coin, callback) {
    assert(from, to, amount, coin)

    amount = amount * 1e6;
    if(coin === 'clam') { 
        this.tipUserByIdClam(from, to, amount, function(err){ 
            if(err)
                return callback(err)

            return callback(null)

        })
    } else if (coin === 'btc') { 
        this.tipUserByIdBtc(from, to, amount, function(err){ 
            if(err)
                return callback(err)

            return callback(null)
        })
    }
}

exports.tipUserByIds = function(from, to, amount, divy, coin, callback) {
    assert(from, to, amount, coin)

    amount = amount * 1e6;
    if(coin === 'clam') { 
        this.tipUserByIdsClam(from, to, amount, divy, function(err, result){ 
            if(err)
                return callback(err)

            return callback(null, result)

        })
    } else if (coin === 'btc') { 
        this.tipUserByIdsBtc(from, to, amount, divy, function(err, result){ 
            if(err)
                return callback(err)

            return callback(null, result)
        })
    }
}

exports.tipUserByIdsBtc = function(from, ids, amount, divy, callback) {
    assert(from, amount, ids, divy)
    var total = 0;
    var totals = []
    var tasks = [];

    if (divy === "split") {
        total = amount;
        for (var i = 0; i < ids.length; i++) {
            var amt = Math.floor(amount / ids.length);
            totals.push({ userId:ids[i], amount:amt});
        }
    } else if (divy === "each") {
        total = amount * ids.length;
        for (var i = 0; i < ids.length; i++) {
            totals.push({ userId:ids[i], amount:amount});
        }
    } else {
        return callback("INCORRECT_DIVY_OPTION")
    }

    if (totals.length === 0)
        return callback('NO_USERS_TO_SEND_TO')


    tasks.push(function(callback) {
        query('UPDATE users SET balance_satoshis_btc = balance_satoshis_btc - $1 WHERE id = $2 returning id', [total, from], callback);
    });

    for(var j = 0; j < totals.length; j++) { 
        var entry = totals[j]
        tasks.push(function(callback) {
            query('UPDATE users SET balance_satoshis_btc = balance_satoshis_btc + $1 WHERE id = $2 returning id', [entry.amount, entry.userId], callback);
        });
        tasks.push(function(callback) {
            var uid = uuid.v4();
            query('INSERT INTO transfers (id, from_user_id, to_user_id, amount, coin) values($1,$2,$3,$4,$5)', [uid, from, entry.userId, entry.amount, 'btc'], callback);
        });
    }

    async.parallel(tasks, function(err, results) {
        if (err) {
            console.log(err)
            if (err.code === '23514') { // constraint violation
                return callback('NOT_ENOUGH_BALANCE');
            }
            return callback(err)
        }

        return callback(null, totals);
    });
}


exports.tipUserByIdsClam = function(from, ids, amount, divy, callback) {
    assert(from, amount, ids, divy)

    var total = 0;
    var totals = []
    var tasks = [];

    if (divy === "split") {
        total = amount;
        for (var i = 0; i < ids.length; i++) {
            var amt = Math.floor(amount / ids.length);
            totals.push({ userId:ids[i], amount:amt});
        }
    } else if (divy === "each") {
        total = amount * ids.length;
        for (var i = 0; i < ids.length; i++) {
            totals.push({ userId:ids[i], amount:amount});
        }
    } else {
        return callback("INCORRECT_DIVY_OPTION")
    }

    if (totals.length === 0)
        return callback('NO_USERS_TO_SEND_TO')


    tasks.push(function(callback) {
        query('UPDATE users SET balance_satoshis_clam = balance_satoshis_clam - $1 WHERE id = $2 returning id', [total, from], callback);
    });

    for(var j = 0; j < totals.length; j++) { 
        var entry = totals[j]
        console.log(entry)
        tasks.push(function(callback) {
            query('UPDATE users SET balance_satoshis_clam = balance_satoshis_clam + $1 WHERE id = $2 returning id', [entry.amount, entry.userId], callback);
        });
        tasks.push(function(callback) {
            var uid = uuid.v4();
            query('INSERT INTO transfers (id, from_user_id, to_user_id, amount, coin) values($1,$2,$3,$4,$5)', [uid, from, entry.userId, entry.amount, 'clam'], callback);
        });
    }

    async.parallel(tasks, function(err, results) {
        if (err) return callback(err);

        return callback(null, totals);
    });
}






var timer = function(name) {
    var start = new Date();
    return {
        stop: function() {
            var end  = new Date();
            var time = end.getTime() - start.getTime();
            console.log('Socket Timer:', name, 'finished in', time, 'ms');
        }
    }
};

//do it here
exports.endGame = function(gameId, givenOut, totalBet, instaCrash, callback) {
    assert(typeof gameId === 'number');
    assert(typeof callback === 'function');

    var roundTotal = Number(bigInt(totalBet.toString()).subtract(givenOut.toString()).multiply(1e6).toString());
    var totalBetPercision =  Number(bigInt(totalBet.toString()).multiply(1e6).toString());

    getClient(function(client, callback) {
        client.query('UPDATE games SET ended = true WHERE id = $1', [gameId],
            function(err) {
                if (err) return callback(new Error('Could not end game, got: ' + err));

            if(totalBetPercision === 0)
                return callback()

            client.query('UPDATE site_stats SET wagered_clam =  wagered_clam + $1, net_profit_clam = net_profit_clam + $2', [totalBetPercision ,roundTotal],
            function(err) {
                if (err) return callback(new Error('Could not end game, got: ' + err));

                if (roundTotal == 0)
                    return callback()

                client.query('SELECT COALESCE(SUM(balance_satoshis_invested_clam),0) as total FROM users WHERE staking_only IS FALSE', function(err, result) {
                    if (err) return callback(err)

                    var onsite = result.rows[0].total
                    client.query('SELECT * FROM users WHERE balance_satoshis_invested_clam > 999999 and staking_only IS FALSE', function(err, results) {
                        if (err) return callback(err)

                        if (results.rowCount == 0)
                            return callback(null)

                        var userCount = results.rowCount 
                        var totalProfitDifference = new Decimal(0)
                        var totalBankrollDifference = new Decimal(0)

                        async.eachSeries(results.rows, function(user, callback) {

                            Decimal.set({
                                precision: 18,
                                rounding: 4
                            })

                            var ubs = new Decimal(user.balance_satoshis_invested_clam.toString()).times(1e6)
                            roundTotal = new Decimal(roundTotal.toString()).times(1e6)
                            onsite = new Decimal(onsite.toString()).times(1e6)

                            var percentage = ubs.dividedBy(onsite)
                            var totalCut = percentage.times(roundTotal).dividedBy(1e6).toNearest(10, Decimal.ROUND_DOWN)

                            var newBankrollProfit  =  new Decimal(user.bankrole_profit_clam.toString()).plus(totalCut)      
                            var newInvestedBalance =  new Decimal(user.balance_satoshis_invested_clam.toString()).plus(totalCut.toString())      

                            totalProfitDifference   = new Decimal(totalProfitDifference).plus(newInvestedBalance).minus(user.balance_satoshis_invested_clam.toString())
                            totalBankrollDifference = new Decimal(totalBankrollDifference).plus(newBankrollProfit).minus(user.bankrole_profit_clam.toString())

                            client.query('UPDATE users SET balance_satoshis_invested_clam = $1, bankrole_profit_clam = $2 WHERE id = $3', [newInvestedBalance.toNumber(), newBankrollProfit.toNumber(), user.id], function(err, results) {
                                if (err) return callback(err)

                                return callback()
                            })
                        }, function(err) {
                            if (err) return callback(err)

                            var crashString = instaCrash ? "INSTANT CRASH" : "CRASH"
                            logger.info("CLAM: GameId (" + gameId + ") " + crashString, "totalBet:", totalBet, "givenOut:", totalProfitDifference.dividedBy(1e6).round().toString(), "difference:", totalBankrollDifference.toString(), " (", userCount, " users )");
                            callback()
                        });
                    });
                });
            });
        });
    }, callback);
};

function addSatoshis(client, userId, amount, callback) {

    amount = amount * 1e6;
    client.query('UPDATE users SET balance_satoshis_clam = balance_satoshis_clam + $1 WHERE id = $2', [amount, userId], function(err, res) {
        if (err) return callback(err);
        assert(res.rowCount === 1);
        callback(null);
    });
}


//do it here
exports.cashOut = function(userId, playId, amount, callback) {
    assert(typeof userId === 'number');
    assert(typeof playId === 'number');
    assert(typeof amount === 'number');
    assert(typeof callback === 'function');

    getClient(function(client, callback) {
        addSatoshis(client, userId, amount, function(err) {
            if (err)
                return callback(err);

            client.query(
                'UPDATE plays SET cash_out = $1 WHERE id = $2 AND cash_out IS NULL RETURNING bet', [amount, playId],
                function(err, result) {
                    if (err)
                        return callback(err);

                    if (result.rowCount !== 1) {
                        logger.info('[INTERNAL_ERROR] Double cashout? ',
                            'User: ', userId, ' play: ', playId, ' amount: ', amount,
                            ' got: ', result.rowCount);

                        return callback(new Error('Double cashout'));
                    }

                    var amountPercision = new Decimal(amount).times(1e6)
                    var profit = amountPercision.minus(result.rows[0].bet.toString())
                    client.query('UPDATE users SET net_profit_clam = net_profit_clam + $1 where id = $2',[profit.toNumber(), userId]  ,function(err, result) {
                        if (err)
                            return callback(err);

                        callback(null);
                    })
                }
            );
        });
    }, callback);
};

// callback called with (err, { crashPoint: , hash: })
exports.createGame = function(gameId, callback) {
    assert(typeof gameId === 'number');
    assert(typeof callback === 'function');

    query('SELECT hash FROM game_hashes WHERE game_id = $1 and coin = \'clam\'', [gameId], function(err, results) {
        if (err) return callback(err);

        if (results.rows.length !== 1) {
            logger.info('[INTERNAL_ERROR] Could not find hash for game ', gameId);
            return callback('NO_GAME_HASH');
        }

        var hash = results.rows[0].hash;
        var gameCrash = lib.crashPointFromHash(hash);

        assert(lib.isInt(gameCrash));

        query('INSERT INTO games(id, game_crash) VALUES($1, $2)', [gameId, gameCrash], function(err) {
            if (err) return callback(err);

            return callback(null, {
                crashPoint: gameCrash,
                hash: hash
            });
        });
    });
};


exports.getBankroll = function(callback) {
    query('SELECT balance_satoshis_invested_clam FROM users WHERE staking_only IS false AND balance_satoshis_invested_clam > 999999',
        function(err, results) {
            if (err) {
                err.wtf = 'getBankroll'
                return callback(err);
            }

            var bankroll = 0;
            var min = 0;

            for (var p in results.rows) {
                bankroll += new Decimal(results.rows[p].balance_satoshis_invested_clam).dividedBy(1e6).floor().toNumber();
            }

            callback(null, Math.max(min, bankroll));
        }
    );
};

exports.getGame = function(gameId, callback) {
    assert(gameId);

    query('SELECT * FROM games WHERE id = $1 AND ended = TRUE', [gameId], function(err, result) {
        if (err) return callback(err);
        if (result.rows.length == 0) return callback('GAME_DOES_NOT_EXISTS');
        assert(result.rows.length == 1);
        callback(null, result.rows[0]);
    });
};

exports.getGamesPlays = function(gameId, callback) {
    query('SELECT u.username, p.bet, p.cash_out FROM plays p, users u ' +
        ' WHERE game_id = $1 AND p.user_id = u.id ORDER by p.cash_out DESC NULLS LAST', [gameId],
        function(err, result) {
            if (err) return callback(err);
            return callback(null, result.rows);
        }
    );
};

exports.getUserPlays = function(userId, limit, offset, callback) {
    assert(userId);

    query('SELECT p.bet, p.cash_out, p.created, p.game_id, g.game_crash FROM plays p ' +
        'LEFT JOIN (SELECT * FROM games WHERE ended = true) g ON g.id = p.game_id ' +
        'WHERE p.user_id = $1 AND coin = \'clam\' ORDER BY p.id DESC LIMIT $2 OFFSET $3', [userId, limit, offset],
        function(err, result) {
            if (err) return callback(err);
            callback(null, result.rows);
        }
    );
};


exports.getGameHistory = function(callback) {

    var sql =
        'SELECT games.id game_id, game_crash, created, ' +
        '     (SELECT hash FROM game_hashes WHERE game_id = games.id AND coin = \'clam\'), ' +
        '     (SELECT to_json(array_agg(to_json(pv))) ' +
        '        FROM (SELECT username, bet AS stopped_at ' +
        '              FROM plays JOIN users ON user_id = users.id WHERE game_id = games.id AND coin = \'clam\') pv) player_info ' +
        'FROM games ' +
        'WHERE games.ended = true AND coin = \'clam\' ' +
        'ORDER BY games.id DESC LIMIT 10';

    query(sql, function(err, data) {
        if (err) {
            err.wtf = 'getGameHistory'
            return callback(err);
        }

        data.rows.forEach(function(row) {
            // oldInfo is like: [{"username":"USER","bet":satoshis, ,..}, ..]
            var oldInfo = row.player_info || [];
            var newInfo = row.player_info = {};

            oldInfo.forEach(function(play) {
                newInfo[play.username] = {
                    bet: play.bet,
                    stopped_at: play.stopped_at
                };
            });
        });
        callback(null, data.rows);
    });
};



// Invesment stuffs
exports.getPendingInvestments = function(callback) {
    query('SELECT * FROM investments WHERE status IS NULL AND coin = \'clam\' ORDER BY created DESC', function(err, results) {
        if (err) return callback(err);
        callback(null, results.rows);
    });
};


exports.invest = function(uid, userId, amount, callback) {
    getClient(function(client, callback) {
        client.query('UPDATE users SET balance_satoshis_invested_clam = balance_satoshis_invested_clam + $1 where id = $2', [amount, userId], function(err, data) {
            if (err) {
                if (err.code === '23514') { // constraint violation
                    return callback('NOT_ENOUGH_BALANCE');
                }

                return callback(err);
            }

            client.query('UPDATE investments SET status = $1 where id = $2', ['complete', uid], function(err, data) {
                if (err)
                    return callback(err);

                callback(null);
            });

        });

    }, callback);
}

exports.queueWeeklyCommission = function(callback) {
    query('SELECT * FROM weekly_commissions WHERE completed IS NULL AND coin = \'clam\'', function(err, results) {
        if (results.rowCount === 0) {
            query('INSERT into weekly_commissions (status) values($1)', ['QUEUED'], function(err) {
                if (err) return callback

                return callback(null)
            })
        }
        return callback(null)
    })
}

exports.checkWeeklyCommission = function(callback) {
    query('SELECT * FROM weekly_commissions WHERE completed IS NULL AND coin = \'clam\'', function(err, results) {
        if (err) return callback(err);

        callback(null, results.rows[0]);
    });
}

exports.queueInvestAction = function(userId, invest, coin, callback) {
    if(coin === 'clam') { 
        this.queueInvestActionClam(userId, invest, function(err){ 
            if(err)
                return callback(err)

            return callback(null)

        })
    } else if (coin === 'btc') { 
        console.log("queue btc invest")
        this.queueInvestActionBtc(userId, invest, function(err){ 
            if(err)
                return callback(err)

            return callback(null)
        })
    }
} 

exports.queueInvestActionBtc = function(userId, invest, callback) {
    assert(typeof userId === 'number');
    assert(typeof callback === 'function');

    var uid = uuid.v4()

    getClient(function(client, callback) {
        client.query('SELECT * FROM investments WHERE user_id = $1 AND coin = \'btc\' AND status IS NULL', [userId], function(err, data) {
            if (err) {
                logger.info(err)
                return callback(err);
            }
            if (data.rowCount > 0)
                return callback('INVESTMENT_ALREADY_MADE');

            client.query('SELECT balance_satoshis_btc, balance_satoshis_invested_btc, username FROM users WHERE id = $1', [userId], function(err, result) {
                if (err) {
                    logger.info(err)
                    return callback(err);
                }
                if (result.rows.length === 0)
                    return callback('USER_DOES_NOT_EXIST');


                var balance_satoshis = new Decimal(result.rows[0].balance_satoshis_btc)
                var balance_satoshis_invested = new Decimal(result.rows[0].balance_satoshis_invested_btc)
                var username = result.rows[0].username

                var investAmount = 0;
                var allSet = false;
                if (invest === "all") {
                    allSet = true
                    investAmount = balance_satoshis
                } else {
                    investAmount = new Decimal(invest).times(1e14)
                }


                if ((investAmount).greaterThan(balance_satoshis))
                    return callback('NOT_ENOUGH_BALANCE');

                client.query('UPDATE users SET balance_satoshis_btc = balance_satoshis_btc - $1 WHERE id = $2', [investAmount.toNumber(), userId], function(err) {
                    if (err) {
                        if (err.code === '23514') // constraint violation
                            return callback('NOT_ENOUGH_BALANCE');

                        logger.info(err)
                        return callback(err);
                    }

                    client.query('WITH sitebalance AS ( SELECT (' +
                        '    ( SELECT COALESCE(SUM(balance_satoshis_invested_btc),0) FROM users )' +
                        '  )' +
                        '), userbalance AS ( SELECT (' +
                        '    ( SELECT balance_satoshis_invested_btc FROM users WHERE id=$2 )' +
                        '  )' +
                        ')' +
                        'INSERT INTO investments (id, user_id, username, amount, investment_balance_prev, site_balance, action, allset, coin)' +
                        'values($1,$2,$3,$4,(SELECT * FROM userbalance) , (SELECT * FROM sitebalance), $5, $6, $7)', [uid, userId, username, investAmount.toNumber(), 'invest', allSet, 'btc'],
                        function(err) {
                            if (err) {
                                if (err.code === '23505') { // dupe key
                                    return callback('INVESTMENT_ALREADY_MADE');
                                }

                                logger.info(err)
                                return callback(err);
                            }

                            callback(null);
                        });

                })
            });
        });
    }, callback);
};

exports.queueInvestActionClam = function(userId, invest, callback) {
    assert(typeof userId === 'number');
    assert(typeof callback === 'function');

    var uid = uuid.v4()

    getClient(function(client, callback) {
        client.query('SELECT * FROM investments WHERE user_id = $1 AND coin = \'clam\' AND status IS NULL', [userId], function(err, data) {
            if (err) {
                logger.info(err)
                return callback(err);
            }
            if (data.rowCount > 0)
                return callback('INVESTMENT_ALREADY_MADE');

            client.query('SELECT balance_satoshis_clam, balance_satoshis_invested_clam, username FROM users WHERE id = $1', [userId], function(err, result) {
                if (err) {
                    logger.info(err)
                    return callback(err);
                }
                if (result.rows.length === 0)
                    return callback('USER_DOES_NOT_EXIST');

                var balance_satoshis = new Decimal(result.rows[0].balance_satoshis_clam)
                var balance_satoshis_invested = new Decimal(result.rows[0].balance_satoshis_invested_clam)
                var username = result.rows[0].username

                var investAmount = 0;
                var allSet = false;
                if (invest === "all") {
                    allSet = true
                    investAmount = balance_satoshis
                } else {
                    investAmount = new Decimal(invest).times(1e14)
                }

                if ((investAmount).greaterThan(balance_satoshis))
                    return callback('NOT_ENOUGH_BALANCE');

                client.query('UPDATE users SET balance_satoshis_clam = balance_satoshis_clam - $1 WHERE id = $2', [investAmount.toNumber(), userId], function(err) {
                    if (err) {
                        if (err.code === '23514') // constraint violation
                            return callback('NOT_ENOUGH_BALANCE');

                        logger.info(err)
                        return callback(err);
                    }

                    client.query('WITH sitebalance AS ( SELECT (' +
                        '    ( SELECT COALESCE(SUM(balance_satoshis_invested_clam),0) FROM users )' +
                        '  )' +
                        '), userbalance AS ( SELECT (' +
                        '    ( SELECT balance_satoshis_invested_clam FROM users WHERE id=$2 )' +
                        '  )' +
                        ')' +
                        'INSERT INTO investments (id, user_id, username, amount, investment_balance_prev, site_balance, action, allset, coin)' +
                        'values($1,$2,$3,$4,(SELECT * FROM userbalance) , (SELECT * FROM sitebalance), $5, $6, $7)', [uid, userId, username, investAmount.toNumber(), 'invest', allSet, 'clam'],
                        function(err) {
                            if (err) {
                                if (err.code === '23505') { // dupe key
                                    return callback('INVESTMENT_ALREADY_MADE');
                                }

                                logger.info(err)
                                return callback(err);
                            }

                            callback(null);
                        });

                })
            });
        });
    }, callback);
};



exports.takeWeeklyCommission = function(commissionId, callback) {
    var start = new Date();

    query('SELECT bankrole_profit_clam, hightide_clam, username, id, balance_satoshis_invested_clam FROM users WHERE balance_satoshis_invested_clam > 99999999', function(err, results) { //bankrole_profit > hightide', function(err, results) {
        if (err) {
            logger.info(err)
            return callback(err);
        }

        if (results.rows.length === 0)
            return callback(null)

        var investedBalances = []
        var userList = []

        for (var n = 0; n < results.rows.length; n++) {
            var entry = results.rows[n]

            var investedBal = new Decimal(entry.balance_satoshis_invested_clam).dividedBy(1e14).toFixed(8)
            investedBalances.push(Number(investedBal))

            var br = new Decimal(entry.bankrole_profit_clam)
            var ht = new Decimal(entry.hightide_clam)
            if (br.greaterThan(ht)) {
                userList.push(entry)
            }
        }

        var totalComission = new Decimal(0);
        var totalUsers = results.rows.length
        var collectedFromUsers = 0
        var bankrollProfitDifference = 0

        async.eachSeries(userList, function(user, callback) {

            var commissionable = bigInt('' + user.bankrole_profit_clam).subtract('' + user.hightide_clam)
            var commission = Number(bigInt(commissionPercentage).multiply(commissionable).divide(100))
            var hightide = user.bankrole_profit_clam;

            totalComission = new Decimal(totalComission.toString()).plus(commission.toString())
            collectedFromUsers++
            //bankrollProfitDifference += commissionable

            getClient(function(client, callback) {
                client.query('UPDATE users SET hightide_clam = $1, balance_satoshis_invested_clam = balance_satoshis_invested_clam - $2 where id = $3', [hightide, commission, user.id], function(err, result) {
                    if (err) return callback(err)


                    client.query('INSERT into commissions (id, user_id, amount, reason, from_user) values($1, 7, $2, $3, $4)', [uuid.v4(), commission, 'weekly-commission', user.id], function(err, result) {
                        if (err) return callback(err)

                        client.query('WITH sitebalance AS ( SELECT (' +
                            '    ( SELECT COALESCE(SUM(balance_satoshis_invested_clam),0) FROM users WHERE staking_only IS false)' +
                            '  )' +
                            ')' +
                            'INSERT INTO investments (id, user_id, username, investment_balance_prev, site_balance, action, status, commission_amount, amount)' +
                            'values($1,$2,$3,$4,(SELECT * FROM sitebalance), $5, $6, $7, 0)', [uuid.v4(), user.id, user.username, user.bankrole_profit_clam, 'weekly-commission', 'complete', commission],
                            function(err, result) {
                                if (err) return callback(err)


                                return callback(null)
                            });
                    });
                });
            }, callback);
        }, function(err) {
            if (err) return callback(err)

            query('UPDATE weekly_commissions SET completed = now(), users = $1, amount = $2, status = $3 where id = $4', [totalUsers, totalComission.toNumber(), 'COMPLETE', commissionId], function(err) {
                if (err) return callback(err)

                investedBalances.sort(function(a, b) {
                    return b - a
                });

                logger.info("[Weekly-Commissions] total commission: %d collected from: %d users ( %d total invested users ) in %d seconds", totalComission.dividedBy(1e14).toFixed(8), collectedFromUsers, totalUsers, (new Date() - start) / 1000)
                logger.info("[Weekly-Commissions] Investment balances: %s", JSON.stringify(investedBalances))

                query('UPDATE users SET balance_satoshis_clam = balance_satoshis_clam + $1 WHERE id =7', [totalComission.toNumber()], function(err) {
                    if (err) return callback(err)

                    return callback(null)
                })
            });
        });
    });
}



exports.divest = function(uid, userId, amount, allSet, callback) {
    getClient(function(client, callback) {
        client.query('SELECT bankrole_profit_clam, balance_satoshis_invested_clam, hightide_clam FROM users where id = $1', [userId], function(err, data) {
            if (err) {
                logger.error(err)
                return callback(err);
            }

            client.query('SELECT * FROM investments where id = $1', [uid], function(err, investmentResult) {
                if (err) {
                    logger.error(err)
                    return callback(err);
                }

                var investmentBalancePrev = new Decimal(investmentResult.rows[0].investment_balance_prev)
                var hightide = new Decimal(data.rows[0].hightide_clam);
                var balanceInvested = new Decimal(data.rows[0].balance_satoshis_invested_clam);
                var bankroleProfit = new Decimal(data.rows[0].bankrole_profit_clam); //balance_satoshis_invested;          
                // amount is already in the correct precision here 
                amount = new Decimal(amount)

                var total;
                if (allSet) {
                    total = balanceInvested
                } else {
                    if (amount.greaterThan(balanceInvested))
                        total = balanceInvested
                    else
                        total = amount;
                }

                if(!investmentBalancePrev.equals(balanceInvested))
                    investmentBalancePrev = balanceInvested

                // first figure out if we need to take commission
                var commission = new Decimal(0);
                if (bankroleProfit.greaterThan(hightide)) {
                    var commissionable = new Decimal(bankroleProfit).minus(hightide)
                    commission = new Decimal(commissionPercentage).times(commissionable).dividedBy(100)
                    //hightide = bankroleProfit;
                }

                var investSql = 'WITH sitebalance AS ( SELECT (' +
                                '    ( SELECT COALESCE(SUM(balance_satoshis_invested_clam),0) FROM users WHERE staking_only IS false)  )' +
                                ')' +
                                'UPDATE investments SET status = $1, commission_amount = $2, amount = $3, site_balance = (SELECT * FROM sitebalance), investment_balance_prev = $5 where id = $4'

                var tasks = []
                if (commission.greaterThan(0)) {
                    hightide = bankroleProfit

                    // handle divest all
                    if (balanceInvested.minus(total).equals(0)) {
                        //take commission from total
                        if (total.minus(commission).lessThan(0)) {
                            logger.info("[Divest][Error][take commission from total] Not enough balance to cover commission and divest", userId, uid)
                            return callback('NOT_ENOUGH_BALANCE');
                        }

                        tasks.push(function(callback) {
                            client.query('UPDATE users SET hightide_clam = $1, balance_satoshis_clam = balance_satoshis_clam + $2, balance_satoshis_invested_clam = balance_satoshis_invested_clam - $3 where id = $4', [hightide.toNumber(), total.minus(commission).toNumber(), total.toNumber(), userId], callback)
                        })
                        tasks.push(function(callback) {
                            client.query(investSql, ['complete', commission.toNumber(), total.minus(commission).toNumber(), uid, investmentBalancePrev.toNumber()], callback)
                        })
                    } else if (balanceInvested.minus(commission).minus(total).lessThan(0)) { 
                        // take all of invest and the rest from total

                        var remainder = balanceInvested.minus(total)
                        var commissionLeft
                        if(commission.minus(remainder).lessThan(0)){ 
                            commissionLeft = remainder.minus(commission)
                        } else { 
                            commissionLeft = commission.minus(remainder)
                        }
                        
                        total = total.minus(commissionLeft)

                        if (total.lessThan(0)) {
                            logger.info("[Divest][Error][take all of invest and the rest from total] Not enough balance to cover commission and divest", userId, uid)
                            return callback('NOT_ENOUGH_BALANCE');
                        }

                        tasks.push(function(callback) {
                            client.query('UPDATE users SET hightide_clam = $1, balance_satoshis_invested_clam = 0,  balance_satoshis_clam = balance_satoshis_clam + $2 where id = $3', [hightide.toNumber(), total.toNumber(), userId], callback)
                        })
                        tasks.push(function(callback) {
                            client.query(investSql, ['complete', commission.toNumber(), total.toNumber(), uid, investmentBalancePrev.toNumber()], callback)
                        })

                    } else { 
                        //take whats needed from investment balance

                        balanceInvested = balanceInvested.minus(total).minus(commission)
                        if (balanceInvested.lessThan(0)) {
                            logger.info("[Divest][Error][take whats needed from investment balance] Not enough balance to cover commission and divest", userId, uid)
                            return callback('NOT_ENOUGH_BALANCE');
                        }

                        tasks.push(function(callback) {
                            client.query('UPDATE users SET hightide_clam = $1, balance_satoshis_invested_clam = $2,  balance_satoshis_clam = balance_satoshis_clam + $3 where id = $4', [hightide.toNumber(), balanceInvested.toNumber(), total.toNumber(), userId], callback)
                        })
                        tasks.push(function(callback) {
                            client.query(investSql, ['complete', commission.toNumber(), total.toNumber(), uid, investmentBalancePrev.toNumber()], callback)
                        })

                    }



                    // actually take said commission
                    tasks.push(function(callback) {
                        client.query('UPDATE users SET balance_satoshis_clam = balance_satoshis_clam + $1 WHERE id = 7', [commission.toNumber()], callback)
                    })
                    tasks.push(function(callback) {
                        var uid2 = uuid.v4();
                        client.query('INSERT INTO commissions(id, user_id, amount, reason, from_user) VALUES($1, $2, $3, $4, $5)', [uid2, 7, commission.toNumber(), "divest-gambling-commission", userId], callback)
                    })


                } else {
                    tasks.push(function(callback) {
                        client.query('UPDATE users SET  balance_satoshis_clam = balance_satoshis_clam + $1, balance_satoshis_invested_clam = balance_satoshis_invested_clam - $1 where id = $2', [total.toNumber(), userId], callback)
                    })
                    tasks.push(function(callback) {
                        client.query(investSql, ['complete', 0, total.toNumber(), uid, investmentBalancePrev.toNumber()], callback)
                    })
                }


                async.series(tasks, function(err){ 
                    if (err) {

                        if (err.code === '23514') { // constraint violation
                            return callback('NOT_ENOUGH_BALANCE');
                        }

                        logger.error(err)
                        return callback(err);
                    }

                     logger.info('[Divest][CompletedRequest] UID: %d, Total commission %d, Total divested %d', userId, commission.toNumber(), total.toNumber())
                     return callback(null)
                })

            }); 
        });
    }, callback);
}

exports.queueDivestAction = function(userId, divest, coin, callback) {
    console.log(userId, divest, coin)
    if(coin === 'clam') { 
        console.log("divestClam")
        this.queueDivestActionClam(userId, divest, function(err){ 
            if(err)
                return callback(err)

            return callback(null)

        })
    } else if (coin === 'btc') { 
        this.queueDivestActionBtc(userId, divest, function(err){ 
            if(err)
                return callback(err)

            return callback(null)
        })
    }
} 

exports.queueDivestActionBtc = function(userId, divest, callback) {
    assert(typeof userId === 'number');
    assert(typeof callback === 'function');

    var uid = uuid.v4()

    getClient(function(client, callback) {
        client.query('SELECT * FROM investments WHERE user_id = $1 AND status IS NULL AND coin = \'btc\'', [userId], function(err, data) {
            if (err) {
                logger.info(err)
                return callback(err);
            }
            if (data.rowCount > 0)
                return callback('INVESTMENT_ALREADY_MADE');

            client.query('SELECT balance_satoshis_btc, balance_satoshis_invested_btc, username FROM users WHERE id = $1', [userId], function(err, result) {
                if (err)
                    return callback(err);
                if (result.rows.length === 0)
                    return callback('USER_DOES_NOT_EXIST');

                var balance_satoshis = result.rows[0].balance_satoshis_btc
                var balance_satoshis_invested = result.rows[0].balance_satoshis_invested_btc
                var username = result.rows[0].username

                var divestAmount = 0;
                var allSet = false;
                if (divest === "all") {
                    allSet = true
                    divestAmount = balance_satoshis_invested
                } else
                    divestAmount = divest * 1e14

                if (divestAmount > balance_satoshis_invested)
                    return callback('NOT_ENOUGH_BALANCE');


                // we can't remove a user balance here, as it could be in the middle of a game...  invested balances should
                // only ever be adjusted between games.. this also means then when we do update balances, if theres an error 
                // we have to nuke the investment request entirely, theres no good way to give a error back to the user
                client.query('WITH sitebalance AS ( SELECT (' +
                    '    ( SELECT COALESCE(SUM(balance_satoshis_invested_btc),0) FROM users)' +
                    '  )' +
                    '), userbalance AS ( SELECT (' +
                    '    ( SELECT balance_satoshis_invested_btc FROM users WHERE id=$2 )' +
                    '  )' +
                    ')' +
                    'INSERT INTO investments (id, user_id, username, amount, investment_balance_prev, site_balance, action, allset, coin)' +
                    'values($1,$2,$3,$4,(SELECT * FROM userbalance) , (SELECT * FROM sitebalance), $5, $6, $7)', [uid, userId, username, divestAmount, 'divest', allSet, 'btc'],
                    function(err) {
                        if (err) {

                            if (err.code === '23505') { // dupe key
                                return callback('INVESTMENT_ALREADY_MADE');
                            }

                            logger.info(err)
                            return callback(err);
                        }

                        callback(null);
                    });
            })
        });
    }, callback);
};

exports.queueDivestActionClam = function(userId, divest, callback) {
    assert(typeof userId === 'number');
    assert(typeof callback === 'function');

    var uid = uuid.v4()

    getClient(function(client, callback) {
        client.query('SELECT * FROM investments WHERE user_id = $1 AND status IS NULL AND coin = \'clam\'', [userId], function(err, data) {
            if (err) {
                logger.info(err)
                return callback(err);
            }
            if (data.rowCount > 0)
                return callback('INVESTMENT_ALREADY_MADE');

            client.query('SELECT balance_satoshis_clam, balance_satoshis_invested_clam, username FROM users WHERE id = $1', [userId], function(err, result) {
                if (err)
                    return callback(err);
                if (result.rows.length === 0)
                    return callback('USER_DOES_NOT_EXIST');

                var balance_satoshis = result.rows[0].balance_satoshis_clam
                var balance_satoshis_invested = result.rows[0].balance_satoshis_invested_clam
                var username = result.rows[0].username

                var divestAmount = 0;
                var allSet = false;
                if (divest === "all") {
                    allSet = true
                    divestAmount = balance_satoshis_invested
                } else
                    divestAmount = divest * 1e14

                if (divestAmount > balance_satoshis_invested)
                    return callback('NOT_ENOUGH_BALANCE');


                // we can't remove a user balance here, as it could be in the middle of a game...  invested balances should
                // only ever be adjusted between games.. this also means then when we do update balances, if theres an error 
                // we have to nuke the investment request entirely, theres no good way to give a error back to the user
                client.query('WITH sitebalance AS ( SELECT (' +
                    '    ( SELECT COALESCE(SUM(balance_satoshis_invested_clam),0) FROM users)' +
                    '  )' +
                    '), userbalance AS ( SELECT (' +
                    '    ( SELECT balance_satoshis_invested_clam FROM users WHERE id=$2 )' +
                    '  )' +
                    ')' +
                    'INSERT INTO investments (id, user_id, username, amount, investment_balance_prev, site_balance, action, allset, coin)' +
                    'values($1,$2,$3,$4,(SELECT * FROM userbalance) , (SELECT * FROM sitebalance), $5, $6, $7)', [uid, userId, username, divestAmount, 'divest', allSet, 'clam'],
                    function(err) {
                        if (err) {

                            if (err.code === '23505') { // dupe key
                                return callback('INVESTMENT_ALREADY_MADE');
                            }

                            logger.info(err)
                            return callback(err);
                        }

                        callback(null);
                    });
            })
        });
    }, callback);
};


var statSiteQuery = 'SELECT (SELECT (COALESCE(SUM(bet), 0::NUMERIC))::NUMERIC AS site_wagered_btc FROM plays WHERE coin = \'btc\'),' +
    '(SELECT (COALESCE(SUM(cash_out), 0::NUMERIC) * 1000000 - COALESCE(SUM(bet), 0::NUMERIC))::NUMERIC * -1 AS site_profit_btc FROM plays WHERE coin = \'btc\'),' +
    '(SELECT (COALESCE(SUM(balance_satoshis_invested_btc), 0::NUMERIC))::NUMERIC AS site_invested_btc FROM users WHERE balance_satoshis_invested_btc > 999999 ),' +
    '(SELECT (COALESCE(SUM(bet), 0::NUMERIC))::NUMERIC AS site_wagered_clam FROM plays WHERE coin = \'clam\'),' +
    '(SELECT (COALESCE(SUM(cash_out), 0::NUMERIC) * 1000000 - COALESCE(SUM(bet), 0::NUMERIC))::NUMERIC * -1 AS site_profit_clam FROM plays WHERE coin = \'clam\'),' +
    '(SELECT (COALESCE(SUM(balance_satoshis_invested_clam), 0::NUMERIC))::NUMERIC AS site_invested_clam FROM users WHERE balance_satoshis_invested_clam > 999999 )'


exports.getStatsForSite = function(callback) {
    query(statSiteQuery,
        function(err, results) {
            if (err) {
                return callback(err);
            }
            var data = {
                userInvestedClam: 0,
                userInvestedBtc: 0,
                userBalanceClam: 0,
                userBalanceBtc: 0,
                userWageredClam: 0,
                userWageredBtc: 0,
                userProfitAmountClam: 0,
                userProfitAmountBtc: 0,
                netProfitClam: 0,
                netProfitBtc: 0,
                siteWageredClam: Math.floor(results.rows[0].site_wagered_clam / 1e6),
                siteProfitAmountClam: Math.floor(results.rows[0].site_profit_clam / 1e6),
                siteInvestedClam: Math.floor(results.rows[0].site_invested_clam / 1e6),
                siteProfitPercentageClam: isNaN(results.rows[0].site_profit_clam / results.rows[0].site_wagered_clam) ? 0 : results.rows[0].site_profit_clam / results.rows[0].site_wagered_clam * 100,
                siteWageredBtc: Math.floor(results.rows[0].site_wagered_btc / 1e6),
                siteProfitAmountBtc: Math.floor(results.rows[0].site_profit_btc / 1e6),
                siteInvestedBtc: Math.floor(results.rows[0].site_invested_btc / 1e6),
                siteProfitPercentageBtc: isNaN(results.rows[0].site_profit_btc / results.rows[0].site_wagered_btc) ? 0 : results.rows[0].site_profit_btc / results.rows[0].site_wagered_btc * 100
            }


            return callback(null, data);
        });
};

var statQuery = 'SELECT (SELECT username FROM users WHERE users.id = $1),' +
    '(SELECT balance_satoshis_invested_clam FROM users WHERE users.id = $1),' +
    '(SELECT balance_satoshis_clam FROM users WHERE users.id = $1),' +
    '(SELECT bankrole_profit_clam FROM users WHERE users.id = $1),' +
    '(SELECT balance_satoshis_invested_btc FROM users WHERE users.id = $1),' +
    '(SELECT balance_satoshis_btc FROM users WHERE users.id = $1),' +
    '(SELECT bankrole_profit_btc FROM users WHERE users.id = $1),' +
    '(SELECT wagered_btc FROM users WHERE users.id = $1),' +
    '(SELECT wagered_clam FROM users WHERE users.id = $1),' +
    '(SELECT (COALESCE(SUM(cash_out), 0::NUMERIC) * 1000000 - COALESCE(SUM(bet), 0::NUMERIC))::NUMERIC AS net_profit_clam FROM plays WHERE user_id = $1 AND coin = \'clam\'),' +
    '(SELECT (COALESCE(SUM(cash_out), 0::NUMERIC) * 1000000 - COALESCE(SUM(bet), 0::NUMERIC))::NUMERIC AS net_profit_btc FROM plays WHERE user_id = $1 AND coin = \'btc\'),' +
    //'(SELECT net_profit_clam FROM users WHERE users.id = $1),' +
    //'(SELECT net_profit_btc FROM users WHERE users.id = $1),' +
    '(SELECT wagered_btc FROM site_stats) AS site_wagered_btc ,' +
    '(SELECT wagered_clam FROM site_stats ) AS site_wagered_clam,' +
    '(SELECT net_profit_clam FROM site_stats)  AS site_profit_clam,' +
    '(SELECT net_profit_btc FROM site_stats)   AS site_profit_btc,' +
    '(SELECT (COALESCE(SUM(balance_satoshis_invested_btc), 0::NUMERIC))::NUMERIC AS site_invested_btc FROM users WHERE balance_satoshis_invested_btc > 999999 ),' +
    '(SELECT (COALESCE(SUM(balance_satoshis_invested_clam), 0::NUMERIC))::NUMERIC AS site_invested_clam FROM users WHERE balance_satoshis_invested_clam > 999999 )'

exports.getStatsForUser = function(user, force, callback) {

    query(statQuery, [user],
        function(err, results) {
            if (err) {
                console.log("getStatsForUser err", err)
                return callback(err);
            }

            var data = {
                userInvestedClam: Math.floor(results.rows[0].balance_satoshis_invested_clam / 1e6),
                userBalanceClam: Math.floor(results.rows[0].balance_satoshis_clam / 1e6),
                siteWageredClam: Math.floor(results.rows[0].site_wagered_clam / 1e6),
                userWageredClam: Math.floor(results.rows[0].wagered_clam / 1e6),
                siteProfitAmountClam: Math.floor(results.rows[0].site_profit_clam / 1e6),
                userProfitAmountClam: Math.floor(results.rows[0].bankrole_profit_clam / 1e6),
                siteInvestedClam: Math.floor(results.rows[0].site_invested_clam / 1e6),
                netProfitClam: Math.floor(results.rows[0].net_profit_clam / 1e6),
                siteProfitPercentageClam: isNaN(results.rows[0].site_profit_clam / results.rows[0].site_wagered_clam) ? 0 : results.rows[0].site_profit_clam / results.rows[0].site_wagered_clam * 100,
                userInvestedBtc: Math.floor(results.rows[0].balance_satoshis_invested_btc / 1e6),
                userBalanceBtc: Math.floor(results.rows[0].balance_satoshis_btc / 1e6),
                siteWageredBtc: Math.floor(results.rows[0].site_wagered_btc / 1e6),
                userWageredBtc: Math.floor(results.rows[0].wagered_btc / 1e6),
                siteProfitAmountBtc: Math.floor(results.rows[0].site_profit_btc / 1e6),
                userProfitAmountBtc: Math.floor(results.rows[0].bankrole_profit_btc / 1e6),
                siteInvestedBtc: Math.floor(results.rows[0].site_invested_btc / 1e6),
                netProfitBtc: Math.floor(results.rows[0].net_profit_btc / 1e6),
                siteProfitPercentageBtc: isNaN(results.rows[0].site_profit_btc / results.rows[0].site_wagered_btc) ? 0 : results.rows[0].site_profit_btc / results.rows[0].site_wagered_btc * 100
            }

            return callback(null, data);
        });
};



//function refreshView() {
//    query('REFRESH MATERIALIZED VIEW user_stats;', function(err) {
//        if (err) {
//            logger.info('err refreshing leaderboards', err);
//        } else {
//            //console.log('leaderboards refreshed');

//        }
//        setTimeout(refreshView, 1500);
//    });
//}
//refreshView();



function takeCommission(client, amount, reason, userId, callback) {

    client.query('UPDATE users SET balance_satoshis_clam = balance_satoshis_clam + $1 WHERE id = 7', [amount.toNumber()], function(err, results) {
        var uid = uuid.v4();
        client.query('INSERT INTO commissions(id, user_id, amount, reason, from_user) VALUES($1, $2, $3, $4, $5)', [uid, 7, amount.toNumber(), reason, userId], function(err, results) {
            if (err) return callback(err);

            logger.info('Sent commission', amount.toNumber(), 'to user id 7');
        });
    });

    return callback(null);
}



exports.getWeeklyDonors = function(year, week, coin, callback) {
    var sql = 'SELECT * FROM weekly_donors WHERE year = $1 AND week = $2 AND coin = $3' +
        'ORDER BY amount ASC LIMIT 10';

    query(sql, [year, week, coin], function(err, results) {
        if (err) return callback(err)


        callback(null, results.rows);
    });
}


exports.getWeeklyDonated = function(week, year, coin, callback) {
    query('SELECT (COALESCE(SUM(amount), 0::NUMERIC))::NUMERIC FROM weekly_donors WHERE week = $1 AND year = $2 AND coin = $3', [week, year, coin], function(err, results) {
        if (err) return callback(err)

        callback(null, new Decimal(results.rows[0].coalesce).dividedBy(1e14).toFixed(8));
    });
}


exports.checkPrizePoolPayout = function(callback) {
    var week = currentWeekNumber();
    var year = new Date().getFullYear();
    var previousWeek = currentWeekNumber() - 1

    getClient(function(client, callback) {
        client.query('SELECT * FROM weekly_prize_log WHERE week = $1 and year = $2 AND coin = \'clam\'', [week, year], function(err, result) {
            if (err) return callback(err)

            if (result.rows.length === 0) {
                client.query('SELECT * FROM weekly_prize_log WHERE week = $1 and year = $2 AND coin = \'clam\'', [previousWeek, year], function(err, results) {
                    if (err) return callback(err)

                    if (results.rows.length > 0)
                       return callback()

                    async.parallel({
                        net: function(done) {
                            var sql = 'SELECT * FROM weekly_leaderboard_clam WHERE year = $1 AND week = $2 ' +
                                'ORDER BY net_rank ASC LIMIT 10';

                            client.query(sql, [year, previousWeek], done);
                        },
                        wagered: function(done) {
                            var sql = 'SELECT * FROM weekly_leaderboard_clam WHERE year = $1 AND week = $2 ' +
                                'ORDER BY wagered_rank ASC LIMIT 10';

                            client.query(sql, [year, previousWeek], done);
                        },
                        prizePool: function(done){ 
                            var sql = 'SELECT (COALESCE(SUM(amount), 0::NUMERIC))::NUMERIC FROM weekly_donors WHERE year = $1 AND week = $2 AND coin = \'clam\''

                            client.query(sql, [year, previousWeek], done);
                        }
                    }, function(err, results) {
                        if (err) return callback(err);


                        var payouts = {}
                        var tasks = []
                        var total = new Decimal(0)

                        var prizePool = new Decimal(results.prizePool.rows[0].coalesce).dividedBy(2)

                        if(!_.isEmpty(results.net)) {
                            
                            for (var h = 0; h < results.net.rows.length; h++) {
                                var entry = results.net.rows[h]

                                
                                if (entry.net_rank <= 10) {
                                    var p = .5
                                    var prize = Math.round( (  ( 1 - p )  / ( 1 - Math.pow(p, results.net.rows.length) ) ) * Math.pow(p, (entry.net_rank - 1) ) * prizePool )

                                    total = new Decimal(total).plus(prize.toString())

                                    payouts[entry.user_id] = payouts[entry.user_id] || 0
                                    payouts[entry.user_id] = new Decimal(payouts[entry.user_id]).plus(prize.toString()).toNumber()

                                    var netWeeklyPrizeEntry = { userId:entry.user_id, prizeAmount: prize, week: previousWeek, year: year, type:"net" }
                                    logger.info("Weeklyprize ", entry.user_id, prize, previousWeek, year, ' net', total.toNumber(), entry.net_rank)

                                    var annon = function(weeklyPrize, callback) { 
                                        tasks.push(function(callback) {
                                            client.query('INSERT INTO weekly_prizes (user_id, amount, week, year, prize_type) values($1,$2,$3,$4,$5)', [weeklyPrize.userId, weeklyPrize.prizeAmount, weeklyPrize.week, weeklyPrize.year, weeklyPrize.type], callback)
                                        })
                                    }(netWeeklyPrizeEntry, callback)

                                }
                            } 
                        }


                        if(!_.isEmpty(results.wagered)) {
                            for (var g = 0; g < results.wagered.rows.length; g++) {
                                var entry = results.wagered.rows[g]

                                if (entry.wagered_rank <= 10) {
                                    var p = .5
                                    var prize = Math.round( (  ( 1 - p )  / ( 1 - Math.pow(p, results.wagered.rows.length) ) ) * Math.pow(p, (entry.wagered_rank - 1) ) * prizePool )

                                    total = new Decimal(total).plus(prize.toString())

                                    payouts[entry.user_id] = payouts[entry.user_id] || 0
                                    payouts[entry.user_id] = new Decimal(payouts[entry.user_id]).plus(prize.toString()).toNumber()

                                    var wageredWeeklyPrizeEntry = { userId:entry.user_id, prizeAmount: prize, week: previousWeek, year: year, type:"wagered" }
                                    logger.info("Weeklyprize ", entry.user_id, prize, previousWeek, year, ' wagered', total.toNumber(), entry.wagered_rank)
                                    
                                    var annon = function(weeklyPrize, callback) { 
                                        tasks.push(function(callback) {
                                            client.query('INSERT INTO weekly_prizes (user_id, amount, week, year, prize_type) values($1,$2,$3,$4,$5)', [weeklyPrize.userId, weeklyPrize.prizeAmount, weeklyPrize.week, weeklyPrize.year, weeklyPrize.type], callback)
                                        })
                                    }(wageredWeeklyPrizeEntry, callback)
                                }
                            }   
                        }


                        Object.keys(payouts).forEach(function(user) {
                            tasks.push(function(callback) {
                                client.query('UPDATE users SET balance_satoshis_clam = balance_satoshis_clam + $1 WHERE id = $2', [payouts[user], parseInt(user)], callback)
                            })
                        })

                        

                        if(total.greaterThan(0)) {
                            tasks.push(function(callback) {
                                client.query('INSERT INTO weekly_prize_log (week, year, status, total) values($1,$2,$3,$4)', [previousWeek, year, 'complete', total.toNumber()], callback)
                            })
                        }

                        async.parallel(tasks, function(err, results) {
                            if (err) return callback(err);

                            logger.info("[Weekly-Prize-Payout] Paid ", total.toNumber(),"for Week ", previousWeek, " Year ", year)
                            return callback(null);
                        });
                    
                        
                    });
                })
            }
        })
    }, callback)
}



exports.donateReward = function(from, amount, coin, callback) {
    amount = new Decimal(amount.toString()).times(1e6)

    var week = currentWeekNumber();
    var year = new Date().getFullYear();

    //get current week

    if(coin === "clam") {
        getClient(function(client, callback) {
                client.query('UPDATE users SET balance_satoshis_clam = balance_satoshis_clam - $1 WHERE id = $2', [amount.toNumber(), from], function(err) {
                    if (err) {
                        if (err.code === '23514') { // constraint violation
                            return callback('NOT_ENOUGH_BALANCE');
                        }
                    }

                    client.query('INSERT into weekly_donors (user_id, amount, week, year, coin) values($1,$2,$3,$4,$5)', [from, amount.toNumber(), week, year, 'clam'], function(err) {
                        if (err) return callback(err)


                        client.query('SELECT (COALESCE(SUM(amount), 0::NUMERIC))::NUMERIC FROM weekly_donors WHERE week = $1 AND year = $2 AND coin = \'clam\'', [week, year], function(err, result) {
                            if (err) return callback(err)

                            var total = new Decimal(result.rows[0].coalesce)


                            logger.info("[weekly-reward-donation] added " + amount.dividedBy(1e14).toFixed(8).toString() + " " + coin + ". New total: " + total.dividedBy(1e14).toFixed(8).toString())
                            return callback(null, total.dividedBy(1e14).toFixed(8))
                        });
                    });
                });
        }, callback);
    } else if(coin = "btc") { 
        getClient(function(client, callback) {
                client.query('UPDATE users SET balance_satoshis_btc = balance_satoshis_btc - $1 WHERE id = $2', [amount.toNumber(), from], function(err) {
                    if (err) {
                        if (err.code === '23514') { // constraint violation
                            return callback('NOT_ENOUGH_BALANCE');
                        }
                    }

                    client.query('INSERT into weekly_donors (user_id, amount, week, year, coin) values($1,$2,$3,$4,$5)', [from, amount.toNumber(), week, year, 'btc'], function(err) {
                        if (err) return callback(err)


                        client.query('SELECT (COALESCE(SUM(amount), 0::NUMERIC))::NUMERIC FROM weekly_donors WHERE week = $1 AND year = $2 AND coin = \'btc\'', [week, year], function(err, result) {
                            if (err) return callback(err)

                            var total = new Decimal(result.rows[0].coalesce)


                            logger.info("[weekly-reward-donation] added " + amount.dividedBy(1e14).toFixed(8).toString() + " " + coin + ". New total: " + total.dividedBy(1e14).toFixed(8).toString())
                            return callback(null, total.dividedBy(1e14).toFixed(8))
                        });
                    });
                });
        }, callback);
    } 
}