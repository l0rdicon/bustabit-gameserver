module.exports = {
    PORT: process.env.PORT || 3844,
    USE_HTTPS: process.env.USE_HTTPS || true,
    HTTPS_KEY: process.env.USE_HTTPS || '',
    HTTPS_CERT: process.env.USE_HTTPS || '',
    HTTPS_CA: process.env.USE_HTTPS || '',
    DATABASE_URL:  process.env.DATABASE_URL || "",
    ENC_KEY: process.env.ENC_KEY || 'enterkey',
    PRODUCTION: process.env.NODE_ENV  === 'production',
    SITE_URL: 'https:/games.freebitcoins.com',

    //Do not set any of this on production

    CRASH_AT: process.env.CRASH_AT //Force the crash point
};
