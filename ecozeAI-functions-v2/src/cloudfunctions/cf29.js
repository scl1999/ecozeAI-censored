//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf29 = onRequest({
  region: REGION,
  timeoutSeconds: 60, // A short timeout is sufficient
  memory: "1GiB",     // Minimal memory is needed
  secrets: SECRETS,
}, async (req, res) => {
  logger.info("[cf29] Invoked");

  try {
    // 1. Get and validate the userName argument
    const userName = req.method === "POST" ? req.body?.userName : req.query.userName;

    if (!userName || typeof userName !== 'string' || !userName.trim()) {
      logger.warn("[cf29] Missing or invalid userName argument.");
      res.status(400).json({ error: "The 'userName' argument is required and must be a non-empty string." });
      return;
    }
    const sanitizedUserName = userName.trim();

    // 2. Initialize Cloud Storage client and define the path
    const storage = new Storage();
    const bucket = storage.bucket("projectId.appspot.com");
    // In Cloud Storage, "directories" are placeholder objects ending with a '/'
    const directoryPath = `eai_companies/${sanitizedUserName}/`;
    const directoryFile = bucket.file(directoryPath);

    // 3. Check if the directory placeholder object already exists
    const [exists] = await directoryFile.exists();

    if (exists) {
      // If it exists, the function's job is done.
      logger.info(`[cf29] Directory '${directoryPath}' already exists. No action taken.`);
      res.status(200).json({
        status: "exists",
        message: `Directory for user '${sanitizedUserName}' already exists.`,
      });
      return;
    }

    // 4. If it does not exist, create it by saving an empty placeholder object
    await directoryFile.save('');

    logger.info(`[cf29] Successfully created directory: '${directoryPath}'`);
    res.status(201).json({
      status: "created",
      message: `Successfully created directory for user '${sanitizedUserName}'.`,
      path: directoryPath
    });

  } catch (err) {
    logger.error("[cf29] Uncaught error:", err);
    res.status(500).json({
      status: "error",
      message: "An internal error occurred.",
      error: String(err)
    });
  }
});