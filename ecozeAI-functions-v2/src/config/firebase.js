const { onMessagePublished } = require("firebase-functions/v2/pubsub");
const { onSchedule } = require("firebase-functions/v2/scheduler");
const { CloudBillingClient } = require("@google-cloud/billing");
const { ProjectsClient } = require("@google-cloud/resource-manager");
const billingClient = new CloudBillingClient();
const resourceManagerClient = new ProjectsClient();
const { onDocumentCreated } = require("firebase-functions/v2/firestore");
const fetch = require("node-fetch");
const { onRequest } = require("firebase-functions/v2/https");
const logger = require("firebase-functions/logger");
const admin = require("firebase-admin");
const similarity = require("string-similarity");
if (!admin.apps.length) admin.initializeApp();
const db = admin.firestore();
const sleep = ms => new Promise(r => setTimeout(r, ms));
const { CloudTasksClient } = require("@google-cloud/tasks");
const tasksClient = new CloudTasksClient();
const https = require("https");
const { GoogleGenAI } = require('@google/genai');
const { DocumentServiceClient } = require("@google-cloud/discoveryengine").v1;
const discoveryEngineClient = new DocumentServiceClient();
const { PubSub } = require('@google-cloud/pubsub');
const pubSubClient = new PubSub();
const { Storage } = require("@google-cloud/storage");
const axios = require("axios");
const xlsx = require("xlsx");
const path = require("path");
const OpenAI = require("openai");
const { GoogleAuth } = require("google-auth-library");
const { CheerioCrawler, Configuration } = require('crawlee');

module.exports = {
    admin,
    db,
    logger,
    onRequest,
    onDocumentCreated,
    onMessagePublished,
    onSchedule,
    fetch,
    sleep,
    tasksClient,
    discoveryEngineClient,
    pubSubClient,
    storage: new Storage(),
    axios,
    xlsx,
    path,
    OpenAI,
    GoogleAuth,
    GoogleGenAI,
    https
};