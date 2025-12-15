import rateLimit from "express-rate-limit";

const isDevelopment = process.env.NODE_ENV !== "production";

const createRateLimiter = (windowMs, max, message) => {
  return rateLimit({
    windowMs,
    max,
    message: { error: message },
    standardHeaders: true,
    legacyHeaders: false,
    skip: (req) => isDevelopment,
  });
};

export const generalLimiter = isDevelopment
  ? rateLimit({
      windowMs: 15 * 60 * 1000,
      max: 10000,
      message: {
        error: "Too many requests from this IP, please try again later.",
      },
      standardHeaders: true,
      legacyHeaders: false,
    })
  : createRateLimiter(
      15 * 60 * 1000,
      100,
      "Too many requests from this IP, please try again later."
    );

export const authLimiter = createRateLimiter(
  15 * 60 * 1000,
  isDevelopment ? 50 : 5,
  "Too many login attempts, please try again after 15 minutes."
);

export const strictLimiter = createRateLimiter(
  15 * 60 * 1000,
  isDevelopment ? 5000 : 50,
  "Too many requests, please try again later."
);
