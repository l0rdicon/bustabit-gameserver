module.exports = {
    PORT: process.env.PORT || 3842,
    USE_HTTPS: process.env.USE_HTTPS || true,
    HTTPS_KEY: process.env.HTTPS_KEY || '/certs/privkey.pem',
    HTTPS_CERT: process.env.HTTPS_CERT ||'/certs//cert.pem',
    HTTPS_CA: process.env.HTTPS_CA || '/certs/fullchain.pem',
    DATABASE_URL:  process.env.DATABASE_URL || "",
    ENC_KEY: process.env.ENC_KEY || 'devkey',
    PRODUCTION: process.env.NODE_ENV  === 'production',
    SITE_URL: 'https://games.freebitcoins.com',

    //Do not set any of this on production

    CRASH_AT: process.env.CRASH_AT //Force the crash point
};
