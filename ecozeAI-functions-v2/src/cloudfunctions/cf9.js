//const { admin, db, logger, ...} = require('../../config/firebase');
//...

exports.cf9 = onMessagePublished({
  topic: "datastore-import-job-status-check", // Listens to the topic from the initiator
  region: REGION,
  timeoutSeconds: 540,
  memory: "1GiB", // Can be smaller
}, async (event) => {
  const { operationName, documentIds } = event.data.message.json;

  if (!operationName || !documentIds || documentIds.length === 0) {
    logger.error("Received invalid message for status check:", event.data.message.json);
    return;
  }

  logger.info(`Checking status for import operation: ${operationName}`);

  try {
    // Get the status of the long-running operation
    const [operation] = await discoveryEngineClient.checkImportDocumentsProgress(operationName);

    // If the operation is not done, we can simply let the function end.
    // Pub/Sub can be configured with a retry policy to check again later.
    if (operation.done === false) {
      logger.info(`Operation ${operationName} is still in progress. Will retry later.`);
      // Throwing an error will cause Pub/Sub to automatically retry the message later.
      throw new Error(`Operation not complete, triggering retry for ${operationName}`);
    }

    // If we get here, the operation is done.
    logger.info(`Operation ${operationName} is complete.`);

    // Check if the completed operation had an error.
    if (operation.error) {
      logger.error(`Operation ${operationName} finished with an error:`, operation.error);
      return; // Stop processing this message
    }

    // Operation was successful, update Firestore.
    const firestoreBatch = db.batch();
    documentIds.forEach(docId => {
      const docRef = db.collection("emissions_factors").doc(docId);
      firestoreBatch.update(docRef, { vertexAISearchable: true });
    });

    await firestoreBatch.commit();
    logger.info(`Successfully updated 'vertexAISearchable' flag for ${documentIds.length} documents in Firestore for operation ${operationName}.`);

  } catch (err) {
    // This will catch the "not complete" error and other issues
    logger.error(`Failed to process operation ${operationName}:`, err);
    // Re-throw the error to ensure Pub/Sub retries it.
    throw err;
  }
});

//-----------------------------------------------------------------------------------------------------------------------------------------------------------------

/****************************************************************************************
 * ecozeAI Lite $$$
 ****************************************************************************************/