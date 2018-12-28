var assert   =  require('assert');
var CBuffer  =  require('CBuffer');
var events   =  require('events');
var util     =  require('util');
var _        =  require('lodash');

var db       =  require('./database');
var lib      =  require('./lib');
var logger = require('winston');

var CHAT_HISTORY_SIZE = 150;

function Chat() {
    var self = this;

    // History of chat messages.
    self.chatTable = new CBuffer(CHAT_HISTORY_SIZE);
    // History of mod only messages.
    self.modTable  = new CBuffer(CHAT_HISTORY_SIZE);

    /*
      Collection of muted users.
        key:   Username
        value: Object with the following fields
                  time:       Date object when the user was muted
                  moderator:  The username of the moderator that muted the user
                  timespec:   Initial (nominal diff) time the user has been muted
                  end:        Until when the user is muted
                  shadow:     If the mute is a shadow mute
    */
    self.muted = {};

    events.EventEmitter.call(self);
}

util.inherits(Chat, events.EventEmitter);


Chat.prototype.say = function(socket, userInfo, message) {
    var self = this;
    var now = new Date();

    var msg = {
        time:      now,
        type:      'say',
        uid:       userInfo.id,
        username:  userInfo.username,
        role:      userInfo.userclass,
        message:   message.trim()
    };

    if (lib.hasOwnProperty(self.muted, userInfo.username)) {
        var muted = self.muted[userInfo.username];
        if (muted.end < now) {
            // User has been muted before, but enough time has passed.
            delete self.muted[userInfo.username];
        } else if (muted.shadow) {
            // User is shadow muted. Echo the message b ack to the
            // user but don't broadcast.
            socket.emit('msg', msg);
            return;
        } else {
            // Inform the user that he is still muted.
            socket.emit('msg',
                        { time: now,
                          type: 'info',
                          message: 'You\'re muted. ' +
                            lib.printTimeString(muted.end - now) +
                            ' remaining'
                        });
            return;
        }
    }
    self.chatTable.push(msg);
    self.emit('msg', msg);
    this.saveChatMessage( userInfo.id, msg.time, msg.message );
};

Chat.prototype.sayMod = function(socket, userInfo, message) {
    var self = this;
    var now = new Date();

    var msg = {
        time:      now,
        type:      'mod',
        uid:       userInfo.id,
        username:  userInfo.username,
        role:      userInfo.userclass,
        message:   message.trim()
    };

    if (lib.hasOwnProperty(self.muted, userInfo.username)) {
        var muted = self.muted[userInfo.username];
        if (muted.end < now) {
            // User has been muted before, but enough time has passed.
            delete self.muted[userInfo.username];
        } else if (muted.shadow) {
            // User is shadow muted. Echo the message b ack to the
            // user but don't broadcast.
            socket.emit('msg', msg);
            return;
        } else {
            // Inform the user that he is still muted.
            socket.emit('msg',
                        { time: now,
                          type: 'info',
                          message: 'You\'re muted. ' +
                            lib.printTimeString(muted.end - now) +
                            ' remaining'
                        });
            return;
        }
    }

    self.modTable.push(msg);
    self.emit('modmsg', msg);
};

Chat.prototype.saveChatMessage = function(userId, date, message ) {
    db.addChatMessage(userId, date, message, function(err) {
       if(err)
        logger.info('[INTERNAL_ERROR] got error ', err, ' saving chat message of user id ', userId);
    });
};

Chat.prototype.mute = function(shadow, moderatorInfo, username, time, callback) {
    var self = this;
    var now = new Date();
    var ms  = lib.parseTimeString(time);
    var end = new Date(Date.now() + ms);

    // Query the db to make sure that the username exists.
    db.getUserByName(username, function(err, userInfo) {

        if (err) {
            callback(err);
            return;
        }
        assert(userInfo);

        if (userInfo.admin) {
            callback('Cannot mute an admin');
            return;
        }

        // Overriding previous mutes.
        self.muted[userInfo.username] =
            { time:        now,
              moderator:   moderatorInfo.username,
              timespec:    time,
              end:         end,
              shadow:      shadow
            };

        var msg = {
            time:        now,
            type:        'mute',
            moderator:   moderatorInfo.username,
            username:    userInfo.username,
            timespec:    time,
            shadow:      shadow
        };

        if (shadow) {
            self.modTable.push(msg);
            self.emit('modmsg', msg);
        } else {
            self.chatTable.push(msg);
            self.emit('msg', msg);
        }
        callback(null);
    });
};

Chat.prototype.unmute = function(moderatorInfo, username, callback) {
    var self = this;
    var now = new Date();

    if (!lib.hasOwnProperty(self.muted, username))
        return callback('USER_NOT_MUTED');

    var shadow = self.muted[username].shadow;
    delete self.muted[username];

    var msg = {
        time:      now,
        type:      'unmute',
        moderator: moderatorInfo.username,
        username:  username,
        shadow:    shadow
    };

    if (shadow) {
        self.modTable.push(msg);
        self.emit('modmsg', msg);
    } else {
        self.chatTable.push(msg);
        self.emit('msg', msg);
    }
    callback(null);
};

Chat.prototype.listmuted = function () {
    return self.muted;
};

module.exports = Chat;
