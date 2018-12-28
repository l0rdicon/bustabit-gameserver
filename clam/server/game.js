var assert = require('better-assert');
var async = require('async');
var db = require('./database');
var events = require('events');
var util = require('util');
var _ = require('lodash');
var lib = require('./lib');
var SortedArray = require('./sorted_array');
var config = require('./config');
var logger = require('winston');

var tickRate = 150; // ping the client every X miliseconds
var afterCrashTime = 3000; // how long from game_crash -> game_starting
var restartTime = 5000; // How long from  game_starting -> game_started

function Game(lastGameId, lastHash, bankroll, gameHistory) {
    var self = this;

    self.bankroll = bankroll;
    self.maxWin;

    self.gameShuttingDown = false;
    self.startTime; // time game started. If before game started, is an estimate...
    self.crashPoint; // when the game crashes, 0 means instant crash
    self.gameDuration; // how long till the game will crash..

    self.forcePoint = null; // The point we force terminate the game

    self.state = 'ENDED'; // 'STARTING' | 'BLOCKING' | 'IN_PROGRESS' |  'ENDED'
    self.investmentState = 'PENDING'; // PENDING | PROCESSING
    self.pending = {}; // Set of players pending a joined
    self.pending_investment = {}; // Set of players pending investment

    self.pendingCount = 0;
    self.joined = new SortedArray(); // A list of joins, before the game is in progress

    self.players = {}; // An object of userName ->  { playId: ..., autoCashOut: .... }
    self.gameId = lastGameId;
    self.gameHistory = gameHistory;

    self.roundLost = 0;
    self.roundWon = 0;

    self.lastHash = lastHash;
    self.hash = null;


    events.EventEmitter.call(self);

    function runGame() {

        async.waterfall([
            function(callback) {
                // if not in the right state just return and wait until next go
                if (self.state !== 'ENDED' || self.investmentState === 'PROCESSING') {
                    callback("WRONG_STATE");
                }

                if (Object.keys(self.pending_investment).length > 0) {

                    self.investmentState = 'PROCESSING';
                    async.eachSeries(Object.keys(self.pending_investment), function(user, callback) {

                        var investment = self.pending_investment[user];
                        if (investment.type === "divest") {
                            db.divest(investment.id, investment.userid, investment.amount, investment.allset, function(err, result) {
                                if (err) {
                                    logger.info("divestEror", err)
                                    callback(err)
                                }

                                callback();
                            });
                        } else if (investment.type === "invest") {
                            db.invest(investment.id, investment.userid, investment.amount, function(err, result) {
                                if (err) {
                                    logger.info("investEror", err)
                                    callback(err)
                                }

                                callback();
                            });
                        } else {
                            callback('INVALID_INVESTMENT_SELECTION');
                        }

                    }, function(err) {
                        if (err) {
                            if (err.code == '23514') // constraint violation
                                callback('NOT_ENOUGH_MONEY');

                            callback(err);
                        }

                        self.pending_investment = {};
                        self.investmentState = 'PENDING';
                        callback(null);
                    });
                } else {
                    callback(null)
                }
            },
            function(callback) {
                db.checkWeeklyCommission(function(err, result) {
                    if (result) {
                        db.takeWeeklyCommission(result.id, function(err) {
                            
                            callback(err)
                        })
                    } else {
                        callback(err);
                    }
                });
            },
            function(callback) {
                db.getBankroll(function(err, result) {

                    var _bankroll = result;
                    if (_bankroll < 0.0000001 * 1e14)
                        self.bankroll = 0
                    else
                        self.bankroll = _bankroll;

                    callback(err);
                });
            },
            function(callback) {
                db.createGame(self.gameId + 1, function(err, info) {

                    callback(err, info);
                });
            }
        ], function(err, info) {
            // Proceed only if the timeout handler has not yet fired.
            if (err) {
                logger.info('Could not get bankroll', err, ' retrying in 2 sec..');
                setTimeout(runGame, 2000);
                return;
            }

            //logger.info("starting game")
            self.state = 'STARTING';
            self.crashPoint = info.crashPoint;

            // if (config.CRASH_AT) {
            // assert(!config.PRODUCTION);
            // self.crashPoint = parseInt(0);
            // self.crashPoint = parseInt(201);
            // }

            self.hash = info.hash;
            self.gameId++;
            self.startTime = new Date(Date.now() + restartTime);
            self.players = {}; // An object of userName ->  { user: ..., playId: ..., autoCashOut: ...., status: ... }
            self.gameDuration = Math.ceil(inverseGrowth(self.crashPoint + 1)); // how long till the game will crash..
            self.maxWin = Math.round(self.bankroll * 0.01); // Risk .5% per game

            self.emit('game_starting', {
                game_id: self.gameId,
                max_win: self.maxWin,
                site_bankroll: self.bankroll,
                time_till_start: restartTime
            });

            setTimeout(blockGame, restartTime);
        });

    }

    function blockGame() {
        self.state = 'BLOCKING'; // we're waiting for pending bets..

        loop();

        function loop() {
            if (self.pendingCount > 0) {
                logger.info('Delaying game by 100ms for pending actions', JSON.stringify(self.pending));
                return setTimeout(loop, 100);
            }
            startGame();
        }
    }

    function startGame() {
        self.state = 'IN_PROGRESS';
        self.startTime = new Date();
        self.pending = {};
        self.pending_investment = {};
        self.pendingCount = 0;



        var bets = {};
        var arr = self.joined.getArray();
        for (var i = 0; i < arr.length; ++i) {
            var a = arr[i];
            bets[a.user.username] = a.bet;
            self.players[a.user.username] = a;
        }

        self.joined.clear();

        self.emit('game_started', bets);

        self.setForcePoint();

        callTick(0);
    }

    function callTick(elapsed) {
        var left = self.gameDuration - elapsed;
        var nextTick = Math.max(0, Math.min(left, tickRate));

        setTimeout(runTick, nextTick);
    }


    function runTick() {

        var elapsed = new Date() - self.startTime;
        var at = growthFunc(elapsed);

        self.runCashOuts(at);

        if (self.forcePoint <= at && self.forcePoint <= self.crashPoint) {
            self.cashOutAll(self.forcePoint, function(err) {
                logger.info('Just forced cashed out everyone at: ', self.forcePoint, ' got err: ', err);

                endGame(true);
            });
            return;
        }

        // and run the next

        if (at > self.crashPoint)
            endGame(false); // oh noes, we crashed!
        else
            tick(elapsed);
    }

    function endGame(forced) {
        var gameId = self.gameId;
        var crashTime = Date.now();

        assert(self.crashPoint == 0 || self.crashPoint >= 100);

        var givenOut = 0;
        var totalBet = 0;
        var instaCrash = false;

        if (self.crashPoint !== 0) {

            var givenOut = 0;
            Object.keys(self.players).forEach(function(player) {
                var record = self.players[player];

                totalBet += (record.bet);
                if (record.status === 'CASHED_OUT') {
                    var given = record.stoppedAt * (record.bet / 100);
                    assert(lib.isInt(given) && given > 0);
                    givenOut += given;
                }
            });

        } else {
            instaCrash = true;
            Object.keys(self.players).forEach(function(player) {
                var record = self.players[player];
                totalBet += (record.bet);
            });
        }

        var playerInfo = self.getInfo().player_info;

        self.lastHash = self.hash;

        // oh noes, we crashed!
        self.emit('game_crash', {
            forced: forced,
            elapsed: self.gameDuration,
            game_crash: self.crashPoint, // We send 0 to client in instant crash
            hash: self.lastHash
        });

        self.gameHistory.addCompletedGame({
            game_id: gameId,
            game_crash: self.crashPoint,
            created: self.startTime,
            player_info: playerInfo,
            hash: self.lastHash
        });

        var dbTimer;
        dbTimeout();

        function dbTimeout() {
            dbTimer = setTimeout(function() {
                console.log('Game', gameId, 'is still ending... Time since crash:',
                    ((Date.now() - crashTime) / 1000).toFixed(3) + 's');
                dbTimeout();
            }, 1000);
        }

        var endgameTimer;
        db.endGame(gameId, givenOut, totalBet, instaCrash, function(err) {
            if (err)
                console.log('ERROR could not end game id: ', gameId, ' got err: ', err);
            clearTimeout(dbTimer);

            if (self.gameShuttingDown)
                self.emit('shutdown');
            else
                endgameTimer = setTimeout(runGame, (crashTime + afterCrashTime) - Date.now());
        });

        self.checkForInvestments();
        self.state = 'ENDED';
    }


    function tick(elapsed) {
        self.emit('game_tick', elapsed);
        callTick(elapsed);
    }

    runGame();

}

util.inherits(Game, events.EventEmitter);



Game.prototype.getInfo = function() {

    var playerInfo = {};

    for (var username in this.players) {
        var record = this.players[username];

        assert(lib.isInt(record.bet));
        var info = {
            bet: record.bet
        };

        if (record.status === 'CASHED_OUT') {
            assert(lib.isInt(record.stoppedAt));
            info['stopped_at'] = record.stoppedAt;
        }

        playerInfo[username] = info;
    }


    var res = {
        state: this.state,
        player_info: playerInfo,
        game_id: this.gameId, // game_id of current game, if game hasnt' started its the last game
        last_hash: this.lastHash,
        max_win: this.maxWin,
        site_bankroll: this.bankroll,
        // if the game is pending, elapsed is how long till it starts
        // if the game is running, elapsed is how long its running for
        /// if the game is ended, elapsed is how long since the game started
        elapsed: Date.now() - this.startTime,
        created: this.startTime,
        joined: this.joined.getArray().map(function(u) {
            return u.user.username;
        })
    };

    if (this.state === 'ENDED')
        res.crashed_at = this.crashPoint;

    return res;
};


// Calls callback with (err, booleanIfAbleToJoin)
Game.prototype.placeBet = function(user, betAmount, autoCashOut, callback) {
    var self = this;

    assert(typeof user.id === 'number');
    assert(typeof user.username === 'string');
    assert(lib.isInt(betAmount));
    assert(lib.isInt(autoCashOut) && autoCashOut >= 100);

    if (self.state !== 'STARTING')
        return callback('GAME_IN_PROGRESS');

    if (lib.hasOwnProperty(self.pending, user.username) || lib.hasOwnProperty(self.players, user.username))
        return callback('ALREADY_PLACED_BET');

    if (self.maxWin < 0.00000001 * 1e14 || self.bankroll === 0)
        return callback('NOT_ENOUGH_BANKROLL');


    self.pending[user.username] = user.username;
    self.pendingCount++;

    db.placeBet(betAmount, autoCashOut, user.id, self.gameId, function(err, playId) {
        self.pendingCount--;

        if (err) {
            if (err.code == '23514') // constraint violation
                return callback('NOT_ENOUGH_MONEY');

            logger.info('[INTERNAL_ERROR] could not play game, got error: ', err);
            callback(err);
        } else {
            assert(playId > 0);

            self.bankroll += betAmount;

            var index = self.joined.insert({
                user: user,
                bet: betAmount,
                autoCashOut: autoCashOut,
                playId: playId,
                status: 'PLAYING'
            });

            self.emit('player_bet', {
                username: user.username,
                index: index
            });

            callback(null);
        }
    });
};


// Calls callback with (err, booleanIfAbleToJoin)
Game.prototype.checkForInvestments = function() {
    var self = this;

    if (this.investmentState === 'PROCESSING')
        return;

    db.getPendingInvestments(function(err, investments) {
        if (err) {
            logger.info("checkForInvestments(): %s", err);
            return;
        }

        if (investments.length === 0)
            return;

        for (var i in investments) {
            var request = investments[i];
            if (lib.hasOwnProperty(self.pending_investment, request.username))
                continue;

            self.pending_investment[request.username] = {
                id: request.id,
                userid: request.user_id,
                amount: request.amount,
                type: request.action,
                allset: request.allset
            };
        }

    });

    return;
};



Game.prototype.doCashOut = function(play, at, callback) {
    assert(typeof play.user.username === 'string');
    assert(typeof play.user.id == 'number');
    assert(typeof play.playId == 'number');
    assert(typeof at === 'number');
    assert(typeof callback === 'function');

    var self = this;

    var username = play.user.username;

    assert(self.players[username].status === 'PLAYING');
    self.players[username].status = 'CASHED_OUT';
    self.players[username].stoppedAt = at;

    var won = Math.round(((self.players[username].bet / 100) * at));
    assert(lib.isInt(won));

    self.emit('cashed_out', {
        username: username,
        stopped_at: at
    });

    db.cashOut(play.user.id, play.playId, won, function(err) {
        if (err) {
            logger.info('[INTERNAL_ERROR] could not cash out: ', username, ' at ', at, ' in ', play, ' because: ', err);
            return callback(err);
        }

        callback(null);
    });
};

Game.prototype.runCashOuts = function(at) {
    var self = this;

    var update = false;
    // Check for auto cashouts

    Object.keys(self.players).forEach(function(playerUserName) {
        var play = self.players[playerUserName];

        if (play.status === 'CASHED_OUT')
            return;

        assert(play.status === 'PLAYING');
        assert(play.autoCashOut);

        if (play.autoCashOut <= at && play.autoCashOut <= self.crashPoint && play.autoCashOut <= self.forcePoint) {

            self.doCashOut(play, play.autoCashOut, function(err) {
                if (err)
                    logger.info('[INTERNAL_ERROR] could not auto cashout ', playerUserName, ' at ', play.autoCashOut);
            });
            update = true;
        }
    });

    if (update)
        self.setForcePoint();
};

Game.prototype.setForcePoint = function() {
    var self = this;

    var totalBet = 0; // how much satoshis is still in action
    var totalCashedOut = 0; // how much satoshis has been lost

    Object.keys(self.players).forEach(function(playerName) {
        var play = self.players[playerName];

        if (play.status === 'CASHED_OUT') {
            var amount = play.bet * (play.stoppedAt - 100) / 100;
            totalCashedOut += amount;
        } else {
            assert(play.status == 'PLAYING');
            assert(lib.isInt(play.bet));
            totalBet += play.bet;
        }
    });

    if (totalBet === 0) {
        self.forcePoint = Infinity; // the game can go until it crashes, there's no end.
    } else {
        var left = self.maxWin - totalCashedOut - (totalBet * 0.01);

        var ratio = (left + totalBet) / totalBet;

        // in percent
        self.forcePoint = Math.max(Math.floor(ratio * 100), 101);
    }

};

Game.prototype.cashOut = function(user, callback) {
    var self = this;

    assert(typeof user.id === 'number');

    if (this.state !== 'IN_PROGRESS')
        return callback('GAME_NOT_IN_PROGRESS');

    var elapsed = new Date() - self.startTime;
    var at = growthFunc(elapsed);
    var play = lib.getOwnProperty(self.players, user.username);

    if (!play)
        return callback('NO_BET_PLACED');

    if (play.autoCashOut <= at)
        at = play.autoCashOut;

    if (self.forcePoint <= at)
        at = self.forcePoint;


    if (at > self.crashPoint)
        return callback('GAME_ALREADY_CRASHED');

    if (play.status === 'CASHED_OUT')
        return callback('ALREADY_CASHED_OUT');

    self.doCashOut(play, at, callback);
    self.setForcePoint();
};

Game.prototype.cashOutAll = function(at, callback) {
    var self = this;

    if (this.state !== 'IN_PROGRESS')
        return callback();

    logger.info('Cashing everyone out at: ', at);

    assert(at >= 100);

    self.runCashOuts(at);

    if (at > self.crashPoint)
        return callback(); // game already crashed, sorry guys

    var tasks = [];

    Object.keys(self.players).forEach(function(playerName) {
        var play = self.players[playerName];

        if (play.status === 'PLAYING') {
            tasks.push(function(callback) {
                if (play.status === 'PLAYING')
                    self.doCashOut(play, at, callback);
                else
                    callback();
            });
        }
    });

    logger.info('Needing to force cash out: ', tasks.length, ' players');

    async.parallelLimit(tasks, 4, function(err) {
        if (err) {
            logger.info('[INTERNAL_ERROR] unable to cash out all players in ', self.gameId, ' at ', at);
            callback(err);
            return;
        }
        logger.info('Emergency cashed out all players in gameId: ', self.gameId);

        callback();
    });
};

Game.prototype.shutDown = function() {
    var self = this;

    self.gameShuttingDown = true;
    self.emit('shuttingdown');

    logger.info("shutting down")

    // If the game has already ended, we can shutdown immediately.
    if (this.state === 'ENDED') {
        self.emit('shutdown');
    }
};

function growthFunc(ms) {
    var r = 0.00006;
    return Math.floor(100 * Math.pow(Math.E, r * ms));
}

function inverseGrowth(result) {
    var c = 16666.666667;
    return c * Math.log(0.01 * result);
}

module.exports = Game;