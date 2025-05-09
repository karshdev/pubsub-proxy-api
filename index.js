// Dependencies
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const { PubSub } = require('@google-cloud/pubsub');
const morgan = require('morgan');
const dotenv = require('dotenv');
const rateLimit = require('express-rate-limit');

// Load environment variables
dotenv.config();

// Initialize Express app
const app = express();
const PORT = process.env.PORT || 3000;

// Security middleware
app.use(helmet());
app.use(cors());
app.use(express.json({ limit: '10mb' }));

// Logging middleware
app.use(morgan('combined'));

// Rate limiting
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 100,
  message: 'Too many requests from this IP, please try again after 15 minutes'
});
app.use('/api/publish', limiter);

// Get project ID from environment or use default
const defaultProjectId = process.env.GOOGLE_CLOUD_PROJECT_ID || 'apt-icon-384804';

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'healthy' });
});

// Publish to Pub/Sub endpoint
app.post('/api/publish', async (req, res) => {
  try {
    const { topic, message, credentials, projectId } = req.body;
    
    // Validate request
    if (!topic) {
      return res.status(400).json({ error: 'Topic name is required' });
    }
    
    if (!message) {
      return res.status(400).json({ error: 'Message is required' });
    }

    // Initialize PubSub client - use credentials from request if provided
    let pubSubClient;
    
    if (credentials) {
      // Using credentials from request
      pubSubClient = new PubSub({
        projectId: projectId || defaultProjectId,
        credentials: typeof credentials === 'string' 
          ? JSON.parse(credentials) 
          : credentials
      });
    } else {
      // Fallback to environment credentials if available
      pubSubClient = new PubSub({
        projectId: projectId || defaultProjectId
      });
    }
    
    // Convert message to appropriate format
    const messageBuffer = Buffer.from(JSON.stringify(message));
    
    // Get topic object
    const topicObject = pubSubClient.topic(topic);
    
    // Check if topic exists
    try {
      const [exists] = await topicObject.exists();
      if (!exists) {
        return res.status(404).json({ error: `Topic '${topic}' does not exist` });
      }
    } catch (error) {
      // Handle authentication or permissions errors
      if (error.code === 7 || error.code === 16) {
        return res.status(403).json({ 
          error: 'Permission denied or authentication failure',
          message: error.message,
          details: 'Check your service account credentials and permissions'
        });
      }
      throw error;
    }
    
    // Publish message
    const messageId = await topicObject.publish(messageBuffer);
    
    // Return success
    return res.status(200).json({
      success: true,
      messageId,
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    console.error('Error publishing message:', error);
    
    // Handle specific error types
    if (error.code === 7) {
      return res.status(403).json({ error: 'Permission denied. Check service account permissions.' });
    }
    
    // General error handling
    return res.status(500).json({
      error: 'Failed to publish message',
      message: error.message,
      details: error.stack 
    });
  }
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({
    error: 'Internal Server Error',
    message: err.message 
  });
});

// Start server
app.listen(PORT, () => {
  console.log(`Pub/Sub Proxy API running on port ${PORT}`);
});

module.exports = app;