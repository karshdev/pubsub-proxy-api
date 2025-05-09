const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const { PubSub } = require('@google-cloud/pubsub');
const morgan = require('morgan');
const dotenv = require('dotenv');
const rateLimit = require('express-rate-limit');

dotenv.config();

const app = express();
const PORT = process.env.PORT || 3000;

app.use(helmet());
app.use(cors());
app.use(express.json({ limit: '10mb' }));

app.use(morgan('combined'));

const limiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 100,
  message: 'Too many requests from this IP, please try again after 15 minutes'
});

app.use('/api/publish', limiter);

let pubSubClient;
try {
  pubSubClient = new PubSub({
    projectId: process.env.GOOGLE_CLOUD_PROJECT_ID
  });
  console.log('PubSub client initialized successfully');
} catch (error) {
  console.error('Error initializing PubSub client:', error);
}

app.get('/health', (req, res) => {
  res.status(200).json({ status: 'healthy' });
});

app.post('/api/publish', async (req, res) => {
  try {
    const { topic, message } = req.body;
    
    if (!topic) {
      return res.status(400).json({ error: 'Topic name is required' });
    }
    
    if (!message) {
      return res.status(400).json({ error: 'Message is required' });
    }
    
    const messageBuffer = Buffer.from(JSON.stringify(message));
    
    const topicObject = pubSubClient.topic(topic);
    
    const [exists] = await topicObject.exists();
    if (!exists) {
      return res.status(404).json({ error: `Topic '${topic}' does not exist` });
    }
    
    const messageId = await topicObject.publish(messageBuffer);
    
    return res.status(200).json({
      success: true,
      messageId,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('Error publishing message:', error);
    
    if (error.code === 7) {
      return res.status(403).json({ error: 'Permission denied. Check service account permissions.' });
    }
    
    return res.status(500).json({
      error: 'Failed to publish message',
      message: error.message,
      details:  error.stack
    });
  }
});

app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({
    error: 'Internal Server Error',
    message:  err.message 
  });
});

app.listen(PORT, () => {
  console.log(`Pub/Sub Proxy API running on port ${PORT}`);
});

module.exports = app;