async function runWithRetry(apiCallFunction, maxRetries = 10, baseDelayMs = 15000) {
  const retriableStatusCodes = [429, 500, 503];
  const retriableStatusTexts = ["RESOURCE_EXHAUSTED", "UNAVAILABLE", "INTERNAL"];

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await apiCallFunction();
    } catch (err) {
      // 1. Check numeric status code (e.g. 429)
      const isRetriableCode = err.status && retriableStatusCodes.includes(Number(err.status));

      // 2. Check string status (e.g. "RESOURCE_EXHAUSTED")
      const isRetriableText = err.status && retriableStatusTexts.includes(err.status);

      // 3. Check message content (case-insensitive)
      const msg = (err.message || "").toUpperCase();
      const cause = err.cause || {};
      const causeMsg = (cause.message || "").toUpperCase();
      const causeCode = cause.code;

      const isRetriableMessage = msg.includes("RESOURCE_EXHAUSTED") ||
        msg.includes("429") ||
        msg.includes("TOO MANY REQUESTS") ||
        msg.includes("OVERLOADED") ||
        msg.includes("BODY TIMEOUT") ||
        msg.includes("TIMEOUT") ||
        causeMsg.includes("BODY TIMEOUT");

      const isNetworkError = msg.includes("FETCH FAILED") ||
        msg.includes("ECONNRESET") ||
        (err.code === 'UND_ERR_BODY_TIMEOUT') ||
        (causeCode === 'UND_ERR_BODY_TIMEOUT');

      if (isRetriableCode || isRetriableText || isRetriableMessage || isNetworkError) {
        if (attempt === maxRetries) {
          logger.error(`[runWithRetry] Final retry attempt (${attempt}) failed.`, { fullError: err });
          throw err;
        }

        // Cap delay at 3 minutes
        const MAX_DELAY_MS = 180000;
        const backoff = Math.pow(2, attempt - 1);
        const jitter = Math.random() * 5000;
        const cappedBaseDelay = Math.min(baseDelayMs * backoff, MAX_DELAY_MS);
        const delay = cappedBaseDelay + jitter;

        logger.warn(`[runWithRetry] Retriable error (${err.status || "unknown"}). Attempt ${attempt} of ${maxRetries}. Retrying in ~${Math.round(delay / 1000)}s...`);
        await sleep(delay);

      } else {
        // Non-retriable error
        logger.error(`[runWithRetry] Non-retriable error encountered:`, err);
        throw err;
      }
    }
  }
}

async function runWithRetryI(apiCallFunction, maxRetries = 10, baseDelayMs = 15000) {
  const retriableStatusCodes = [429, 500, 503];
  const retriableStatusTexts = ["RESOURCE_EXHAUSTED", "UNAVAILABLE", "INTERNAL"];

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await apiCallFunction();
    } catch (err) {
      // 1. Check numeric status code (e.g. 429)
      const isRetriableCode = err.status && retriableStatusCodes.includes(Number(err.status));

      // 2. Check string status (e.g. "RESOURCE_EXHAUSTED")
      const isRetriableText = err.status && retriableStatusTexts.includes(err.status);

      // 3. Check message content (case-insensitive)
      const msg = (err.message || "").toUpperCase();
      const cause = err.cause || {};
      const causeMsg = (cause.message || "").toUpperCase();
      const causeCode = cause.code;

      const isRetriableMessage = msg.includes("RESOURCE_EXHAUSTED") ||
        msg.includes("429") ||
        msg.includes("TOO MANY REQUESTS") ||
        msg.includes("OVERLOADED") ||
        msg.includes("BODY TIMEOUT") ||
        msg.includes("TIMEOUT") ||
        causeMsg.includes("BODY TIMEOUT");

      const isNetworkError = msg.includes("FETCH FAILED") ||
        msg.includes("ECONNRESET") ||
        (err.code === 'UND_ERR_BODY_TIMEOUT') ||
        (causeCode === 'UND_ERR_BODY_TIMEOUT');

      if (isRetriableCode || isRetriableText || isRetriableMessage || isNetworkError) {
        if (attempt === maxRetries) {
          logger.error(`[runWithRetryI] Final retry attempt (${attempt}) failed.`, { fullError: err });
          throw err;
        }

        // Cap delay at 3 minutes
        const MAX_DELAY_MS = 180000;
        const backoff = Math.pow(2, attempt - 1);
        const jitter = Math.random() * 5000;
        const cappedBaseDelay = Math.min(baseDelayMs * backoff, MAX_DELAY_MS);
        const delay = cappedBaseDelay + jitter;

        logger.warn(`[runWithRetryI] Retriable error (${err.status || "unknown"}). Attempt ${attempt} of ${maxRetries}. Retrying in ~${Math.round(delay / 1000)}s...`);
        await sleep(delay);

      } else {
        // Non-retriable error
        logger.error(`[runWithRetryI] Non-retriable error encountered:`, err);
        throw err;
      }
    }
  }
}