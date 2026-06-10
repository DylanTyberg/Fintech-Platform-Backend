// lambdas/shared/response.mjs

const SECURITY_HEADERS = {
  'Strict-Transport-Security': 'max-age=31536000; includeSubdomains; preload',
  'X-Content-Type-Options':    'nosniff',
  'X-Frame-Options':           'DENY',
  'X-XSS-Protection':          '1; mode=block',
  'Referrer-Policy':           'strict-origin-when-cross-origin',
  'Cache-Control':             'no-store',
};

export const ok = (body, statusCode = 200, extraHeaders = {}) => ({
  statusCode,
  headers: {
    'Content-Type': 'application/json',
    ...SECURITY_HEADERS,
    ...extraHeaders,        // CORS headers merge in here
  },
  body: body !== null ? JSON.stringify(body) : '',
});

export const err = (statusCode, message, extraHeaders = {}) => ({
  statusCode,
  headers: {
    'Content-Type': 'application/json',
    ...SECURITY_HEADERS,
    ...extraHeaders,
  },
  body: JSON.stringify({ error: message }),
});