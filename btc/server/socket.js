var CBuffer = require('CBuffer');
var socketio = require('socket.io');
var database = require('./database');
var lib = require('./lib');
var logger = require('winston');
var Validator = require('wallet-address-validator');
var _ = require('lodash')
var Decimal = require('decimal.js');
var currentWeekNumber = require('current-week-number');
var rpc = require('node-json-rpc');

var RateLimiter = require("rolling-rate-limiter");
var redis = require("redis");
var client = redis.createClient();

var WebSocket = require('ws')


var limiter = RateLimiter({
    redis: client,
    no_ready_check: true,
    namespace: "UserStatsLimiterBtc",
    interval: 1000,
    maxInInterval: 2,
    minDifference: 100
});

module.exports = function(server, game, chat, depositAddresses, userList) {
    var io = socketio(server);
    // holders
    var connectedUsers = {};
    var lastMessage = {};
    var reconnectInterval = 1000 * 60;
    var reconnectionId; 
    var ws;

    (function() {
        function on(event) {
            game.on(event, function(data) {
                io.to('joined').emit(event, data);
            });
        }

        on('game_starting');
        on('game_started');
        on('game_tick');
        on('game_crash');
        on('cashed_out');
        on('player_bet');
        on('get_stats');
        on('anon_stats');
    })();

    var connect = function(){
        ws = new WebSocket("ws://127.0.0.1:someport")
        ws.on('open', function() {
            if(reconnectionId){
                console.log("clear interval")
                clearTimeout(reconnectionId);
            }
        });
        ws.on('error', function() {
            //console.log('socket error');
        });
        ws.on('close', function() {
            //console.log('socket close');
            reconnectionId = setTimeout(connect, reconnectInterval);
        });
         ws.on('message', function(msg) {
            try { 
                console.log("got message")
                var msgParsed = JSON.parse(msg)
                if(msgParsed.type === 'chat')
                    io.to('joined').emit('msg', msgParsed.msg);
                else if (msgParsed.type === 'mod')
                    io.to('moderators').emit('msg', msgParsed.msg);
                else if (msgParsed.type === 'private')
                    relayPrivateMsg(msgParsed.msg)
            } catch (e) { 
                console.log("Chat relay parse error", e)
            }
        });
    };
    connect();
  //48152
    // Forward chat messages to clients.
    chat.on('msg', function(msg) {
        if (ws.readyState == ws.OPEN)
             ws.send(JSON.stringify({type:"chat", msg:msg}));

        io.to('joined').emit('msg', msg);
    });

    chat.on('modmsg', function(msg) {
        if (ws.readyState == ws.OPEN)
            ws.send(JSON.stringify({type:"mod", msg:msg}));

        io.to('moderators').emit('msg', msg);
    });

    io.on('connection', onConnection);

    // get a fresh list of user ids and usernames every 30 seconds
    // so we don't have to verify if user exists on every request
    function refreshUserIds() {
        database.getUserList(function(err, result) {
            if (err) {
                logger.info(err)
            } else {
                userList = result
            }
        })
    }
    setInterval(refreshUserIds, 10000);

    function onConnection(socket) {

        socket.once('join', function(info, ack) {
            if (typeof ack !== 'function')
                return sendError(socket, '[join] No ack function');

            if (typeof info !== 'object')
                return sendError(socket, '[join] Invalid info');

            var ott = info.ott;
            if (ott) {
                if (!lib.isUUIDv4(ott))
                    return sendError(socket, '[join] ott not valid');

                database.validateOneTimeToken(ott, function(err, user) {
                    if (err) {
                        if (err == 'NOT_VALID_TOKEN')
                            return ack(err);
                        return internalError(socket, err, 'Unable to validate ott');
                    }
                    cont(user);
                });
            } else {
                cont(null);
            }

            function cont(loggedIn) {
                if (loggedIn) {
                    loggedIn.admin = loggedIn.userclass === 'admin';
                    loggedIn.moderator = loggedIn.userclass === 'admin' ||
                        loggedIn.userclass === 'moderator';
                    loggedIn.require2fa = loggedIn.mfa_secret ? true : false

                    connectedUsers[loggedIn.id] = socket;
                    lastMessage[loggedIn.id] = new Date().getTime();
                }

                var res = game.getInfo();
                res['table_history'] = game.gameHistory.getHistory();
                res['username'] = loggedIn ? loggedIn.username : null;
                res['frozen'] = loggedIn ? loggedIn.frozen : null;
                res['site_bankroll'] = loggedIn ? game.bankroll : null
                res['balance_satoshis_btc'] = loggedIn ? loggedIn.balance_satoshis_btc : null;
                res['balance_satoshis_invested_btc'] = loggedIn ? loggedIn.balance_satoshis_invested_btc : null;
                res['balance_satoshis_clam'] = loggedIn ? loggedIn.balance_satoshis_clam : null;
                res['balance_satoshis_invested_clam'] = loggedIn ? loggedIn.balance_satoshis_invested_clam : null;


                getChatHistory(function(err, history) {

                    //If we couldn't reach the history send an empty history to the user, the error was already logged
                    if (err)
                        history = [];

                    //Append the history to the response
                    res['chat'] = history;

                    ack(null, res);
                    joined(socket, loggedIn);

                });

            }
        });

    }

    var clientCount = 0;

    function joined(socket, loggedIn) {
        ++clientCount;
        logger.info('Client joined: ', clientCount, ' - ', loggedIn ? loggedIn.username : '~guest~');

        socket.join('joined');
        if (loggedIn && loggedIn.moderator) {
            socket.join('moderators');
        }

        socket.on('disconnect', function() {
            --clientCount;
            logger.info('Client disconnect, left: ', clientCount);

            if (loggedIn) {
                game.cashOut(loggedIn, function(err) {
                    if (err && typeof err !== 'string')
                        logger.info('Error: auto cashing out got: ', err);

                    if (!err)
                        logger.info('Disconnect cashed out ', loggedIn.username, ' in game ', game.gameId);
                });
                delete connectedUsers[loggedIn.id];
            }
        });


        socket.on('get_stats', function() {
            if (loggedIn) {
                limiter(loggedIn.id, function(err, timeLeft) {
                    if (err) {

                    } else if (timeLeft > 0) {

                        // limit was exceeded, action should not be allowed
                        // timeLeft is the number of ms until the next action will be allowed
                        // note that this can be treated as a boolean, since 0 is falsy

                    } else {

                        database.getStatsForUser(loggedIn.id, true, function(err, info) {
                            //console.log(err)

                            socket.emit('stats_update', info);
                        });


                    }
                })
            } else {
                database.getStatsForSite(function(err, info) {
                    //console.log(err)

                    socket.emit('stats_update', info);
                });
            }

        });


        if (loggedIn) {

            socket.on('place_bet', function(amount, autoCashOut, ack) {

                if (loggedIn.frozen === true)
                    return sendError(socket, 'Account is locked, please contact us at https://freebitcoins.supportsystem.com/');

                if (!lib.isInt(amount)) {
                    return sendError(socket, '[place_bet] No place bet amount: ' + amount);
                }
                if (amount <= 0 || !lib.isInt(amount / 100)) {
                    return sendError(socket, '[place_bet] Must place a bet in multiples of 100, got: ' + amount);
                }

                //amount = amount / 1e8;
                //if (amount > 1e8 * 100) // 1 BTC limit
                //    return sendError(socket, '[place_bet] Max bet size is 100 CLAM got: ' + amount);

                if (!autoCashOut)
                    return sendError(socket, '[place_bet] Must Send an autocashout with a bet');

                else if (!lib.isInt(autoCashOut) || autoCashOut < 100)
                    return sendError(socket, '[place_bet] auto_cashout problem');

                if (typeof ack !== 'function')
                    return sendError(socket, '[place_bet] No ack');

                game.placeBet(loggedIn, amount, autoCashOut, function(err) {
                    if (err) {
                        if (typeof err === 'string')
                            ack(err);
                        else {
                            logger.info('[INTERNAL_ERROR] unable to place bet, got: ', err);
                            ack('INTERNAL_ERROR');
                        }
                        return;
                    }

                    ack(null); // TODO: ... deprecate
                });
            });

            socket.on('islocal', function(address, callback) {
                var isLocalAddress = depositAddresses[address] ? true : false
                var tf = (isLocalAddress) ? "true" : "false"
                var retMsg = "islocal " + address + " " + tf

                var msg = {
                    time: new Date(),
                    type: 'private',
                    message: retMsg
                }

                if (typeof callback === "function") {
                    return callback(retMsg)
                } else {
                    connectedUsers[loggedIn.id].emit('msg', msg);
                }
            })

            socket.on('cash_out', function(ack) {
                if (!loggedIn)
                    return sendError(socket, '[cash_out] not logged in');

                if (typeof ack !== 'function')
                    return sendError(socket, '[cash_out] No ack');

                game.cashOut(loggedIn, function(err) {
                    if (err) {
                        if (typeof err === 'string')
                            return ack(err);
                        else
                            return logger.info('[INTERNAL_ERROR] unable to cash out: ', err); // TODO: should we notify the user?
                    }

                    ack(null);
                });
            });

            socket.on('say', function(message) {
                if (game.gameShuttingDown === true) return;
                if (new Date().getTime() - lastMessage[loggedIn.id] > 1000) {
                    lastMessage[loggedIn.id] = new Date().getTime();

                    if (!loggedIn)
                        return sendError(socket, '[say] not logged in');

                    if (typeof message !== 'string' || /^\s*$/.test(message))
                        return sendError(socket, '[say] no message');

                    if (message.length == 0 || message.length > 500)
                        return sendError(socket, '[say] invalid message side');

                    message = message.replace(/[^\x00-\x7F]/g, "");

                    var cmdReg = /^\s*\/([a-zA-z]*)\s*(.*)$/;
                    var cmdMatch = message.match(cmdReg);

                    if (cmdMatch) {
                        var cmd = cmdMatch[1];
                        var rest = cmdMatch[2];

                        switch (cmd) {
                            case 'say':
                                if (loggedIn.frozen === true)
                                    return 
                                chat.say(socket, loggedIn, rest);
                                //ws.send(JSON.stringify({loggedIn:loggedIn, rest:rest}));
                                break;
                            case 'mod':
                            case 'mods':
                                chat.sayMod(socket, loggedIn, rest)
                                break;
                            case 'msg':
                                return sendErrorChat(socket, 'Private messages are currently disabled.')

                                var uidReg = /^([0-9]+)\s*(.*)$/;
                                var uidMatch = rest.match(uidReg);

                                if (loggedIn.locked === true)
                                    return sendErrorChat(socket, 'Account is locked, please contact us at https://freebitcoins.supportsystem.com/')

                                if (uidMatch) {
                                    var uid = uidMatch[1];
                                    var message = uidMatch[2];

                                    // send to other chat here before we do anything with it
                                    if (uid == loggedIn.id) {
                                        var msg = {
                                            time: new Date(),
                                            type: 'info',
                                            message: "are you talking to yourself?"
                                        };

                                        if (!_.isEmpty(connectedUsers[parseInt(loggedIn.id)]))
                                            connectedUsers[loggedIn.id].emit('msg', msg);
                                    } else if (!userList[uid]) {
                                        var msg = {
                                            time: new Date(),
                                            type: 'info',
                                            message: "User does not exist"
                                        };

                                        if (!_.isEmpty(connectedUsers[parseInt(loggedIn.id)]))
                                            connectedUsers[loggedIn.id].emit('msg', msg);
                                    } else {

                                        var msg = {
                                            time: new Date(),
                                            type: 'private',
                                            uid: loggedIn.id,
                                            to: uid,
                                            to_username: userList[uid],
                                            username: loggedIn.username,
                                            role: loggedIn.userclass,
                                            message: message.trim()
                                        };

                                        if (connectedUsers.hasOwnProperty(uid)) {
                                            if (!_.isEmpty(connectedUsers[parseInt(uid)]))
                                                connectedUsers[parseInt(uid)].emit('msg', msg);
                                            if (!_.isEmpty(connectedUsers[parseInt(loggedIn.id)]))
                                                connectedUsers[loggedIn.id].emit('msg', msg);
                                        } else {
                                            if (!_.isEmpty(connectedUsers[parseInt(loggedIn.id)]))
                                                connectedUsers[loggedIn.id].emit('msg', msg);
                                        }
                                    }
                                }
                                break;
                            case 'robinvestors':
                                if (loggedIn.admin) {
                                    database.queueWeeklyCommission(function(err) {
                                        if (err) {
                                            logger.info(err)
                                        } else {
                                            socket.emit('msg', {
                                                time: new Date(),
                                                type: 'info',
                                                message: 'Weekly bitcoin commissions have been queued'
                                            });
                                        }
                                    })
                                }
                                break;
                            case 'reward':
                                var rewardReg = /^(?:(noconf)?\s?)\s*(btc|BTC|clam|CLAM)\s+([0-9]*[1-9][0-9]*(?:\.[0-9]{1,8})?|[0]*\.[0-9]*[1-9][0-9]*)\s*([0-9]{6})?.*$/
                                var rewardMatch = rest.match(rewardReg);

                                var rewardStatsReg = /^\s?(stats|stat)\s+(btc|BTC|clam|CLAM)\s*$/
                                var rewardStatsMatch = rest.match(rewardStatsReg);

                                var donorsReg = /^\s?(donors|donor)\s+(btc|BTC|clam|CLAM)\s*$/
                                var donorsMatch = rest.match(donorsReg);

                                if (loggedIn.locked === true)
                                    return sendErrorChat(socket, 'Account is locked, please contact us at https://freebitcoins.supportsystem.com/')

                                if (rewardMatch) {

                                    var data = {
                                        noconf: rewardMatch[1] ? rewardMatch[1] : undefined,
                                        coin:  rewardMatch[2].toLowerCase(),
                                        amount: rewardMatch[3] ? new Decimal(rewardMatch[3]).times(1e8).toNumber() : undefined,
                                        otp: rewardMatch[4] ? rewardMatch[4] : undefined
                                    }

                                    if (!data.amount)
                                        return sendTipInfo(socket, 'Invalid reward command');

                                    if (!data.coin)
                                        return sendTipInfo(socket, 'No currency given');

                                    if (data.amount < 0)
                                        return sendTipInfo(socket, 'Invalid reward amount');

                                    if (loggedIn.require2fa === true && !data.otp)
                                        return sendTipHelp(socket, '[tip] 2fa code required for tipping');

                                    database.validateOtp(loggedIn.id, data.otp, function(err) {
                                        if (err === 'INVALID_OTP')
                                            return sendErrorChat(socket, '[tip] Invalid one-time password');

                                        database.donateReward(loggedIn.id, data.amount, data.coin, function(err, currentTotal) {
                                            if (err) {
                                                if (err === 'NOT_ENOUGH_BALANCE')
                                                    return sendErrorChat(socket, 'You do not have enough balance');

                                                logger.info("[Donate Error]", err)
                                                return sendErrorChat(socket, 'Unknown reward donation error');
                                            }


                                            sendStat(loggedIn.id);
                                            sendTipInfo("(" + loggedIn.id + ") " + loggedIn.username + " donated " + data.amount / 1e8 + ' ' + data.coin + ' to the weekly leaderboard reward ( TOTAL:' + currentTotal + ' )');
                                            tipEvent(loggedIn.id, "You donated " + data.amount / 1e8 + " " + data.coin + " to the weekly leaderboard reward");
                                        })
                                    })
                                } else if (rewardStatsMatch) {
                                    var week = currentWeekNumber();
                                    var year = new Date().getFullYear();

                                    database.getWeeklyDonated(week, year, rewardStatsMatch[2], function(err, currentTotal) {
                                        if (err)
                                            return sendErrorChat(socket, 'Unknown reward stats error');


                                        tipEvent(loggedIn.id, "The current prize pool for the weekly leaderboard is " + currentTotal + " " + rewardStatsMatch[2].toUpperCase());
                                    })
                                } else if (donorsMatch) {
                                    var week = currentWeekNumber();
                                    var year = new Date().getFullYear();

                                    database.getWeeklyDonors(year, week, donorsMatch[2], function(err, donorList) {

                                        var msg = "Weekly Donor List: "

                                        for (var i = 0; i < donorList.length; i++) {
                                            msg += " (" + donorList[i].user_id + ")" + " " + userList[donorList[i].user_id] + " " + new Decimal(donorList[i].amount).dividedBy(1e14).toFixed(8) + " " + donorsMatch[2].toUpperCase()

                                            if (i + 1 !== donorList.length)
                                                msg += " | "
                                        }


                                        tipEvent(loggedIn.id, msg);
                                    })
                                }
                                break;
                            case 'giveprizes':
                                if (loggedIn.admin) {
                                    database.checkPrizePoolPayout(function(err) {
                                        if (err) console.log(err)

                                        socket.emit('msg', {
                                            time: new Date(),
                                            type: 'info',
                                            message: 'Prizes given out'
                                        });

                                    })
                                }
                                break;
                            case 'tip':
                                if (loggedIn.frozen === true)
                                    return sendErrorChat(socket, 'Account is locked, please contact us at https://freebitcoins.supportsystem.com/');

                                var tipReg = /^(?:(noconf)?\s?)(?:(private|priv)?\s?)((?:\d+(?:,\d+)*))\s+(btc|BTC|clam|CLAM)\s+([0-9]*[1-9][0-9]*(\.[0-9]{1,8})?|[0]*\.[0-9]*[1-9][0-9]*)\s*(each|split)?\s*([0-9]{6})?.*$/;
                                var tipMatch = rest.match(tipReg);

                                if (tipMatch) {

                                    var data = {
                                        noconf: tipMatch[1] ? tipMatch[1] : undefined,
                                        privatetip: tipMatch[2] ? tipMatch[2] : undefined,
                                        sendto: tipMatch[3] ? tipMatch[3] : undefined,
                                        coin:   tipMatch[4] ? tipMatch[4].toLowerCase() : undefined,
                                        amount: new Decimal(tipMatch[5]).times(1e8).toNumber(),
                                        divy: tipMatch[7] ? tipMatch[7] : undefined,
                                        otp: tipMatch[8] ? tipMatch[8] : undefined
                                    }

                                    if(!data.coin)
                                        return sendTipHelp(socket, 'Must pick coin to tip');

                                    if (Number(data.sendto) === loggedIn.id) {
                                        if (!_.isEmpty(connectedUsers[parseInt(loggedIn.id)])) {
                                            connectedUsers[loggedIn.id].emit('msg', {
                                                time: new Date(),
                                                type: 'info',
                                                message: "No need to tip yourself"
                                            });
                                        }
                                    } else {
                                        if (!data.sendto || !data.amount)
                                            return sendTipHelp(socket, 'Invalid tip command');

                                        if (loggedIn.require2fa === true && !data.otp)
                                            return sendTipHelp(socket, '[tip] 2fa code required for tipping');

                                        database.validateOtp(loggedIn.id, data.otp, function(err) {
                                            if (err === 'INVALID_OTP') {
                                                socket.emit('msg', {
                                                    time: new Date(),
                                                    type: 'error',
                                                    message: "Invalid one-time password"
                                                });
                                                return;
                                            }
                                            switch (data.sendto) {
                                                case (data.sendto.match(/^\d+$/) || {}).input:
                                                    if (data.divy) {
                                                        sendTipHelp(socket, "Single uid detected. Should not be used with each or split")
                                                    } else if (data.sendto === loggedIn.id) {
                                                        sendPrivateTipInfo(loggedIn.id, "No reason to tip yourself!");
                                                    } else {

                                                        database.tipUserById(loggedIn.id, data.sendto, data.amount, data.coin, function(err) {
                                                            if (err) {
                                                                 console.log("e1")
                                                                //send back the user id
                                                                if (err === 'TO_USER_DOES_NOT_EXIST')
                                                                    return sendErrorChat(socket, 'error: no user with id ' + data.sendto);
                                                                if (err === 'NOT_ENOUGH_BALANCE')
                                                                    return sendErrorChat(socket, 'You do not have enough ' + data.coin);
                                                                console.log(err)
                                                                return sendErrorChat(socket, 'Unknown tiping error');
                                                            }

                                                            sendStat(data.sendto);
                                                            sendStat(loggedIn.id);
                                                            tipEvent(loggedIn.id, "You sent " + data.amount / 1e8 + " " + data.coin +" to (" + data.sendto + ")" + " " + userList[data.sendto]);

                                                            if (data.privatetip === "private")
                                                                sendPrivateTipInfo(loggedIn.id, "(" + loggedIn.id + ") " + loggedIn.username + " sent " + data.amount / 1e8 + " " + data.coin + " to " + "(" + data.sendto + ") " + userList[data.sendto]);
                                                            else
                                                                sendTipInfo("(" + loggedIn.id + ") " + loggedIn.username + " sent " + data.amount / 1e8 + ' ' + data.coin + ' to user ' + "(" + data.sendto + ") " + userList[data.sendto]);

                                                            tipEvent(data.sendto, "You received a " + data.amount / 1e8 + " " + data.coin +" tip from (" + loggedIn.id + ")" + " " + userList[loggedIn.id]);
                                                        });
                                                    }
                                                    break;
                                                case (data.sendto.match(/^(?:\d+(?:,\d+)*)?$/) || {}).input:
                                                    if (!data.divy) {
                                                        sendTipHelp(socket, "Multi uids detected. You need to specify each or split")
                                                    } else {
                                                        var ids = data.sendto.split(',');
                                                        for (var i = 0; i < ids.length; i++) {
                                                            if (_.isEmpty(userList[ids[i]])) return sendPrivateTipInfo(loggedIn.id, "UID: " + ids[i] + " does not exist");
                                                        }
   
                                                        database.tipUserByIds(loggedIn.id, ids, data.amount, data.divy, data.coin, function(err, result) {
                                                            if (err) {
                                                                if (err === 'INCORRECT_DIVY_OPTION')
                                                                    return sendTipHelp(socket, 'You did not supply either each or split to determine how to divy your tip');
                                                                if (err === 'USER_NOT_FOUND')
                                                                    return sendErrorChat(socket, 'error: user not found');
                                                                if (err === 'NOT_ENOUGH_BALANCE')
                                                                    return sendErrorChat(socket, 'You do not have enough balance');
                                                                console.log(err)
                                                            }

                                                            var tipStr = "";
                                                            sendStat(loggedIn.id);

                                                            for(var j = 0; j < result.length; j++) { 
                                                                var entry = result[j]
                                                                tipStr += "( " + entry.userId + " ) " + entry.amount / 1e14 + " " + data.coin + ", "
                                                                tipEvent(entry.userId, "You received a " + entry.userId / 1e14 + " " + data.coin + " tip from (" + loggedIn.id + ")" + " " + userList[loggedIn.id]);
                                                                sendStat(entry.userId);
                                                            }

                                                            var total = 0
                                                            if (data.divy === "each") {
                                                                total = data.amount / 1e8 * ids.length
                                                            } else if (data.divy = "split") {
                                                                total = (data.amount / 1e8)
                                                            }

                                                            if (data.privatetip === "private")
                                                                sendPrivateTipInfo(loggedIn.id, "(" + loggedIn.id + ") " + loggedIn.username + " sent " + tipStr + "for a total of " + total + " " + data.coin);
                                                            else
                                                                sendTipInfo("(" + loggedIn.id + ") " + loggedIn.username + " sent " + tipStr + "for a total of " + total + " " + data.coin);

                                                            tipEvent(loggedIn.id, "You sent " + tipStr + "for a total of " + total + " " + data.coin);
                                                        });
                                                    }

                                                    break;
                                                default:
                                                    sendTipHelp(socket);
                                                    break;
                                            }
                                        });
                                    }
                                } else
                                    return sendTipHelp(socket);
                                break;
                        case 'invest':
                                if (loggedIn.frozen === true)
                                    return sendErrorChat(socket, 'Account is locked, please contact us at https://freebitcoins.supportsystem.com/');

                                var investReg = /(btc|BTC|clam|CLAM)\s+((?:\d*\.)?\d+\s*$|(all)\s?)/;
                                var investMatch = rest.match(investReg);

                                if (!_.isEmpty(investMatch)) {
                                    database.queueInvestAction(loggedIn.id, investMatch[2], investMatch[1].toLowerCase(), function(err) {
                                        if (err) {
                                            if (err === "NOT_ENOUGH_BALANCE")
                                                return sendErrorChat(socket, "Not Enough Balance")
                                            if (err === "USER_DOES_NOT_EXIST")
                                                return sendErrorChat(socket, "User Does Not Exist")
                                            if (err === "INVESTMENT_ALREADY_MADE")
                                                return sendErrorChat(socket, "Pending Investment")
                                            return sendErrorChat(socket, 'Unkown error investing, please try again later')
                                        }

                                        sendStat(loggedIn.id);
                                        socket.emit('msg', {
                                            time: new Date(),
                                            type: 'info',
                                            message: investMatch[1].toUpperCase() + ' investment request queued.. will be completed at the end of the current round'
                                        });
                                    })
                                } else { 
                                    return sendErrorChat(socket, '/invest [coin] [amount|all]')
                                }
                                break
                            case 'divest':
                                if (loggedIn.frozen === true)
                                    return sendErrorChat(socket, 'Account is locked, please contact us at https://freebitcoins.supportsystem.com/');

                                var divestReg = /(btc|BTC|clam|CLAM)\s+((?:\d*\.)?\d+\s*$|(all)\s?)/;
                                var divestMatch = rest.match(divestReg);

                                if (!_.isEmpty(divestMatch)) {
                                    console.log(divestMatch)
                                    database.queueDivestAction(loggedIn.id, divestMatch[2], divestMatch[1].toLowerCase(), function(err) {
                                        if (err) {
                                            if (err === "NOT_ENOUGH_BALANCE")
                                                return sendErrorChat(socket, err)
                                            if (err === "USER_DOES_NOT_EXIST")
                                                return sendErrorChat(socket, err)
                                            if (err === "INVESTMENT_ALREADY_MADE")
                                                return sendErrorChat(socket, err)
                                            return sendErrorChat(socket, 'Unkown error divesting, please try again later')
                                        }

                                        socket.emit('msg', {
                                            time: new Date(),
                                            type: 'info',
                                            message:  divestMatch[1].toUpperCase() + ' divestment request queued.. will be completed at the end of the current round'
                                        });

                                    })
                                } else {  
                                    var divestReg2 = /((?:\d*\.)?\d+$|(all)\s?)/;
                                    var divestMatch2 = rest.match(divestReg);
                                    if (!_.isEmpty(divestMatch)) {
                                        return sendErrorChat(socket, 'Divest error: must include coin you want to divest')
                                    } else { 
                                        return sendErrorChat(socket, '/divest [coin] [amount|all]')
                                    }
                                }
                                break
                            case 'shutdown':
                                if (loggedIn.admin) {
                                    game.shutDown();
                                    chat.emit('msg', {
                                        time: new Date(),
                                        type: 'info',
                                        message: " Btc Server shutting down..."
                                    });
                                } else {
                                    return sendErrorChat(socket, 'Not an admin.');
                                }
                                break;
                            case 'mute':
                            case 'shadowmute':
                                if (loggedIn.moderator) {
                                    var muteReg = /^\s*([a-zA-Z0-9_\-]+)\s*([1-9]\d*[dhms])?\s*$/;
                                    var muteMatch = rest.match(muteReg);

                                    if (!muteMatch)
                                        return sendErrorChat(socket, 'Usage: /mute <user> [time]');

                                    var username = muteMatch[1];
                                    var timespec = muteMatch[2] ? muteMatch[2] : "30m";
                                    var shadow = cmd === 'shadowmute';

                                    chat.mute(shadow, loggedIn, username, timespec,
                                        function(err) {
                                            if (err)
                                                return sendErrorChat(socket, err);
                                        });
                                } else {
                                    return sendErrorChat(socket, 'Not a moderator.');
                                }
                                break;

                            case 'unmute':
                                if (loggedIn.moderator) {
                                    var unmuteReg = /^\s*([a-zA-Z0-9_\-]+)\s*$/;
                                    var unmuteMatch = rest.match(unmuteReg);

                                    if (!unmuteMatch)
                                        return sendErrorChat(socket, 'Usage: /unmute <user>');

                                    var username = unmuteMatch[1];
                                    chat.unmute(
                                        loggedIn, username,
                                        function(err) {
                                            if (err) return sendErrorChat(socket, err);
                                        });
                                }
                                break;
                            case 'freeze':
                                if (loggedIn.admin) {
                                    var lockReg = /^\s*([0-9]+)\s*$/;
                                    var lockMatch = rest.match(lockReg);

                                    if (!lockMatch)
                                        return sendErrorChat(socket, 'Usage: /freeze <user>');

                                    database.lockUser(lockMatch[1], function(err) {

                                        socket.emit('msg', {
                                            time: new Date(),
                                            type: 'info',
                                            message: 'User: ' + lockMatch[1] + ' account has been locked.'
                                        });

                                        if (!_.isEmpty(connectedUsers[parseInt(lockMatch[1])])) {
                                            connectedUsers[lockMatch[1]].disconnect();
                                            delete connectedUsers[lockMatch[1]];
                                        }
                                    });
                                } else {
                                    socket.emit('msg', {
                                        time: new Date(),
                                        type: 'error',
                                        message: 'Unknown command ' + cmd
                                    });
                                }
                                break;
                            case 'unfreeze':
                                if (loggedIn.admin) {
                                    var lockReg = /^\s*([0-9]+)\s*$/;
                                    var lockMatch = rest.match(lockReg);

                                    if (!lockMatch)
                                        return sendErrorChat(socket, 'Usage: /unfreeze <user>');

                                    database.unlockUser(lockMatch[1], function(err) {

                                        socket.emit('msg', {
                                            time: new Date(),
                                            type: 'info',
                                            message: 'User: ' + lockMatch[1] + ' account has been unlocked'
                                        });

                                        if (!_.isEmpty(connectedUsers[parseInt(lockMatch[1])])) {
                                            connectedUsers[lockMatch[1]].disconnect();
                                            delete connectedUsers[lockMatch[1]];
                                        }
                                    });

                                } else {
                                    socket.emit('msg', {
                                        time: new Date(),
                                        type: 'error',
                                        message: 'Unknown command ' + cmd
                                    });
                                }
                                break;
                            default:
                                socket.emit('msg', {
                                    time: new Date(),
                                    type: 'error',
                                    message: 'Unknown command ' + cmd
                                });
                                break;
                        }
                        return;
                    }

                    chat.say(socket, loggedIn, message);
                }
            });
        }

    }

    function sendErrorChat(socket, message) {
        logger.info('Warning: sending client: ', message);
        socket.emit('msg', {
            time: new Date(),
            type: 'error',
            message: message
        });
    }

    function sendStat(id) {
        if (!_.isEmpty(connectedUsers[id])) {
            database.getStatsForUser(id, true, function(err, info) {
                if (err) console.log(err)

                connectedUsers[id].emit('stats_update', info);
            });
        }
    }

    function tipEvent(id, message) {
        if (!_.isEmpty(connectedUsers[id])) {
            connectedUsers[id].emit('msg', {
                time: new Date(),
                type: 'info',
                message: message
            });
        }
    }

    function sendPrivateTipInfo(id, message) {
        if (!_.isEmpty(connectedUsers[id])) {
            connectedUsers[id].emit('msg', {
                time: new Date(),
                type: 'info',
                message: message
            });
        }
    }

    function sendTipInfo(message) {
        chat.emit('msg', {
            time: new Date(),
            type: 'info',
            message: message
        });
    }

    function sendError(socket, description) {
        logger.info('Warning: sending client: ', description);
        socket.emit('err', description);
    }

    function internalError(socket, err, description) {
        logger.info('[INTERNAL_ERROR] got error: ', err, description);
        socket.emit('err', 'INTERNAL_ERROR');
    }

    function sendTipHelp(socket, message) {

        var helpmsg1 = "if 'noconf' is specified the tip won't require you to click to confirm the details; use with caution!"
        var helpmsg2 = "if 'priv' or 'private' is specified the tip will not be public to other users in chat; tips of less than 0.01 CLAM are always private"
        var helpmsg3 = "you can specify multiple uids, separated by commas (and no spaces), e.g. 1,2,3,4, you must specify 'each' or 'split' after the amount to say whether you want to send the specified amount to each, or split a single amount equally between the recipients"
        var helpmsg4 = "the 2FA code is required for tipping if it is set up to be required for withdrawals."
        var helpmsg5 = "you must include the coin you wish to tip."
        var helpmsg6 = "example: \"\/tip 123 clam 4.56\" will send 4.56 CLAMs to user 123."
        if (message)
            socket.emit('msg', {
                time: new Date(),
                type: 'info',
                message: message
            });
        socket.emit('msg', {
            time: new Date(),
            type: 'info',
            message: helpmsg1
        });
        socket.emit('msg', {
            time: new Date(),
            type: 'info',
            message: helpmsg2
        });
        socket.emit('msg', {
            time: new Date(),
            type: 'info',
            message: helpmsg3
        });
        socket.emit('msg', {
            time: new Date(),
            type: 'info',
            message: helpmsg4
        });
        socket.emit('msg', {
            time: new Date(),
            type: 'info',
            message: helpmsg5
        });
         socket.emit('msg', {
            time: new Date(),
            type: 'info',
            message: helpmsg6
        });
    }

    function sendInfo(socket, message) {
        if (message)
            socket.emit('msg', {
                time: new Date(),
                type: 'info',
                message: message
            });
    }

    function getChatHistory(callback) {

        database.getAllChatTable(100, fn);

        function fn(err, history) {
            if (err) {
                logger.info('[INTERNAL_ERROR] got error ', err, ' loading chat table');
                return callback(err);
            }

            callback(null, history);
        }
    };
};