async function getAccessToken() {
  const auth = new GoogleAuth({
    scopes: ['https://www.googleapis.com/auth/cloud-platform']
  });
  const client = await auth.getClient();
  const accessToken = await client.getAccessToken();
  return accessToken.token;
}

async function callCF(name, body) {
  const url = `https://ecozeAIRegion-projectId.cloudfunctions.net/${name}`;
  const maxRetries = 1;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const rsp = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
      });

      // --- NEW: Check if the HTTP response was successful ---
      if (!rsp.ok) {
        // --- MODIFICATION: Create a custom error that includes the status ---
        const err = new Error(`[callCF] ${name} → received non-ok status ${rsp.status}`);
        err.status = rsp.status; // Attach the status code
        throw err;
      }
      // --- END: New check ---

      const txt = await rsp.text();
      logger.info(`[callCF] ${name} → status ${rsp.status}`);
      return txt.trim();
    } catch (err) {
      // --- MODIFICATION: Expanded retry logic ---
      const isNetworkError = err.code === "ECONNRESET" || err.code === "ETIMEDOUT";
      // Retry on 500 (Internal Server Error), 502 (Bad Gateway), 503 (Service Unavailable)
      const isRetriableHttpError = err.status && [500, 502, 503].includes(err.status);

      logger.warn(
        `[callCF] attempt ${attempt}/${maxRetries} calling ${name} failed:`,
        err.message // Use err.message which now includes the status
      );

      if (attempt < maxRetries && (isNetworkError || isRetriableHttpError)) {
        // exponential backoff: 500ms, then 1s
        await sleep(500 * attempt);
        continue;
      }
      // --- END: Expanded retry logic ---

      // give up and rethrow
      throw err;
    }
  }
}