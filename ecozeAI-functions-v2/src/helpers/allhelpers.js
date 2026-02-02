const { logger, db, tasksClient } = require('../config/firebase');
const { runGeminiStream } = require('../services/ai/gemini');
const { logAITransaction, logAIReasoning } = require('../services/ai/costs');
const { REGION, TEST_QUEUE_ID } = require('../config/constants');
const { CloudTasksClient } = require("@google-cloud/tasks");

function calculateAverage(numbers, filterZeros = false) {
  const values = filterZeros ? numbers.filter(n => typeof n === 'number' && isFinite(n) && n !== 0) : numbers.filter(n => typeof n === 'number' && isFinite(n));
  if (values.length === 0) return 0;
  const sum = values.reduce((acc, val) => acc + val, 0);
  return sum / values.length;
}

async function searchExistingEmissionsFactorsWithAI({ query, productId, materialId }) {
  logger.info(`[searchExistingEmissionsFactorsWithAI] Starting search for query: "${query}"`);

  // 1. Define the specific system prompt for the AI's task.
  const SYS_MSG_DB_SEARCH = "..."

  // 2. Configure and run the AI call with Vertex AI Search grounding.
  const vGenerationConfig = {
    //
    //
    //
    //
    retrieval: {
      vertexAiSearch: {
        // !!! IMPORTANT !!! Replace this with your actual datastore ID
        datastore: 'projects/projectId/locations/global/collections/default_collection/dataStores/ecoze-ai-search-emissions-factors_1752512690221',
      },
    },
  }],
    //
    includeThoughts: true,
    thinkingBudget: 24576 // Correct budget for the pro model
},
  };

const { answer: rawAIResponse, thoughts, cost, totalTokens, searchQueries, model } = await runGeminiStream({
  model: 'aiModel', //flash
  generationConfig: vGenerationConfig,
  user: query,
});

const mSnap = materialId ? await db.collection("materials").doc(materialId).get() : null;
const linkedProductId = mSnap && mSnap.exists ? mSnap.data().linked_product?.id || null : null;

// 3. Log the transaction for cost tracking.
await logAITransaction({
  cfName: 'cf10',
  productId: productId || linkedProductId,
  materialId: materialId,
  cost: cost,
  flashTks: totalTokens,
  searchQueries: searchQueries,
  modelUsed: model,
});

await logAIReasoning({
  sys: SYS_MSG_DB_SEARCH,
  user: query,
  thoughts: thoughts,
  answer: rawAIResponse,
  cloudfunction: 'cf10',
  productId: productId,
  materialId: materialId,
});

// 4. Return the AI's direct response, with a simple validity check.
if (!rawAIResponse || /^Unknown$/i.test(rawAIResponse.trim()) || !rawAIResponse.includes("*name_1")) {
  logger.warn("[searchExistingEmissionsFactorsWithAI] AI returned 'Unknown' or an invalid format.");
  return { response: "[Relevant Emissions Factors]\n*None found*", model: model };
}

logger.info(`[searchExistingEmissionsFactorsWithAI] Returning direct AI response:\n${rawAIResponse}`);
return { response: rawAIResponse, model: model };
}

async function getTestQueuePath() {
  if (testQueuePath) return testQueuePath;

  testTasksCli = new CloudTasksClient();
  const project = process.env.GCP_PROJECT_ID || 'projectId';
  const location = REGION;
  testQueuePath = testTasksCli.queuePath(project, location, TEST_QUEUE_ID);

  try {
    await testTasksCli.getQueue({ name: testQueuePath });
    logger.info(`[cf58] Found existing queue: ${TEST_QUEUE_ID}`);
  } catch (error) {
    if (error.code === 5) { // 5 = NOT_FOUND
      logger.warn(`[cf58] Queue "${TEST_QUEUE_ID}" not found. Creating it...`);
      await testTasksCli.createQueue({
        parent: testTasksCli.locationPath(project, location),
        queue: {
          name: testQueuePath,
          rateLimits: { maxConcurrentDispatches: 20 },
        },
      });
      logger.info(`[cf58] Successfully created queue: ${TEST_QUEUE_ID}`);
    } else {
      throw error; // Re-throw other errors
    }
  }
  return testQueuePath;
}

async function scheduleNextCheck(productId) {
  const project = process.env.GCP_PROJECT_ID || 'projectId';
  const location = 'ecozeAIRegion'; // Or your tasks queue region
  const queue = 'apcf-status-queue';
  const functionUrl = `https://${location}-${project}.cloudfunctions.net/cf40`;

  const queuePath = tasksClient.queuePath(project, location, queue);

  const fiveMinutesFromNow = new Date();
  fiveMinutesFromNow.setMinutes(fiveMinutesFromNow.getMinutes() + 5);

  const task = {
    httpRequest: {
      httpMethod: 'POST',
      url: functionUrl,
      headers: { 'Content-Type': 'application/json' },
      body: Buffer.from(JSON.stringify({ productId })).toString('base64'),
    },
    scheduleTime: {
      seconds: Math.floor(fiveMinutesFromNow.getTime() / 1000),
    },
  };

  try {
    const [response] = await tasksClient.createTask({ parent: queuePath, task });
    logger.info(`[cf40] Scheduled next check for product ${productId}. Task: ${response.name}`);
  } catch (error) {
    logger.error(`[cf40] Failed to schedule next check for product ${productId}:`, error);
    // Throw error to indicate failure, which can be useful for monitoring
    throw new Error('Failed to create Cloud Task.');
  }
}

async function deleteDocumentAndSubcollections(docRef) {
  const subcollections = await docRef.listCollections();
  for (const subcollection of subcollections) {
    await deleteCollection(subcollection);
  }
  await docRef.delete();
}

async function deleteCollection(collectionRef, batchSize = 200) {
  const query = collectionRef.limit(batchSize);

  return new Promise((resolve, reject) => {
    deleteQueryBatch(query, resolve, reject);
  });
}

async function deleteQueryBatch(query, resolve, reject) {
  try {
    const snapshot = await query.get();

    // When there are no documents left, we are done
    if (snapshot.size === 0) {
      resolve();
      return;
    }

    const batch = db.batch();
    for (const doc of snapshot.docs) {
      // For each document, recursively delete its subcollections
      const subcollections = await doc.ref.listCollections();
      for (const subcollection of subcollections) {
        await deleteCollection(subcollection);
      }
      batch.delete(doc.ref);
    }
    await batch.commit();

    // Recurse on the same query to process the next batch
    process.nextTick(() => {
      deleteQueryBatch(query, resolve, reject);
    });
  } catch (err) {
    reject(err);
  }
}

async function scheduleCheckInEmail(userName, daysFromNow, checkInNumber) {
  const project = process.env.GCP_PROJECT_ID || 'projectId';
  const location = REGION;
  const queue = 'emails';

  // The full path to the queue
  const queuePath = tasksClient.queuePath(project, location, queue);

  // The URL of the Cloud Function to invoke
  const url = `https://${location}-${project}.cloudfunctions.net/apcfInitialTestingCheckIn`;

  // Construct the payload for the cf57 function
  const payload = {
    recipient: "sam.linfield@ecoze.app",
    subject: `${checkInNumber}${checkInNumber === 1 ? 'st' : (checkInNumber === 2 ? 'nd' : 'rd')} check in on test user: ${userName}`,
    body: `Check in on ${userName}, to see how they are getting on with the testing`,
  };

  // Calculate the future time for the task
  const futureDate = new Date();
  futureDate.setDate(futureDate.getDate() + daysFromNow);
  const scheduleSeconds = Math.floor(futureDate.getTime() / 1000);

  // Construct the Cloud Task request
  const task = {
    httpRequest: {
      httpMethod: 'POST',
      url: url,
      headers: {
        'Content-Type': 'application/json',
      },
      body: Buffer.from(JSON.stringify(payload)).toString('base64'),
    },
    scheduleTime: {
      seconds: scheduleSeconds,
    },
  };

  logger.info(`[scheduleCheckInEmail] Creating task for check-in #${checkInNumber} to run in ${daysFromNow} days for user: ${userName}.`);
  await tasksClient.createTask({ parent: queuePath, task });
}

module.exports = {
  calculateAverage,
  searchExistingEmissionsFactorsWithAI,
  getTestQueuePath,
  scheduleNextCheck,
  deleteDocumentAndSubcollections,
  deleteCollection,
  scheduleCheckInEmail
};