// Dependencies
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

const defaultProjectId = process.env.GOOGLE_CLOUD_PROJECT_ID || 'apt-icon-384804';

app.get('/health', (req, res) => {
    res.status(200).json({ status: 'healthy' });
});

app.post('/api/publish', async (req, res) => {
    try {
        const { topic, message, credentials, projectId } = req.body;

        if (!topic) {
            return res.status(400).json({ error: 'Topic name is required' });
        }

        if (!message) {
            return res.status(400).json({ error: 'Message is required' });
        }
        
        let pubSubClient;
        
        if (credentials) {
            // Determine if credentials is a service account JSON or an OAuth token
            if (typeof credentials === 'string') {
                try {
                    // Try to parse as JSON first
                    const parsedCredentials = JSON.parse(credentials);
                    pubSubClient = new PubSub({
                        projectId: projectId || defaultProjectId,
                        credentials: parsedCredentials
                    });
                } catch (parseError) {
                    // If parsing fails, treat it as an OAuth token
                    pubSubClient = new PubSub({
                        projectId: projectId || defaultProjectId,
                        authClient: {
                            getRequestHeaders: () => {
                                return Promise.resolve({
                                    'Authorization': `Bearer ${credentials}`
                                });
                            }
                        }
                    });
                }
            } else {
                // Credentials is already an object
                pubSubClient = new PubSub({
                    projectId: projectId || defaultProjectId,
                    credentials
                });
            }
        } else {
            // No credentials provided, use default authentication
            pubSubClient = new PubSub({
                projectId: projectId || defaultProjectId
            });
        }

        const messageBuffer = Buffer.from(JSON.stringify(message));

        const topicObject = pubSubClient.topic(topic);

        try {
            const [exists] = await topicObject.exists();
            if (!exists) {
                return res.status(404).json({ error: `Topic '${topic}' does not exist` });
            }
        } catch (error) {
            if (error.code === 7 || error.code === 16) {
                return res.status(403).json({
                    error: 'Permission denied or authentication failure',
                    message: error.message,
                    details: 'Check your service account credentials and permissions'
                });
            }
            throw error;
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
            details: error.stack
        });
    }
});
app.use((err, req, res, next) => {
    console.error(err.stack);
    res.status(500).json({
        error: 'Internal Server Error',
        message: err.message
    });
});

app.listen(PORT, () => {
    console.log(`Pub/Sub Proxy API running on port ${PORT}`);
});

module.exports = app;