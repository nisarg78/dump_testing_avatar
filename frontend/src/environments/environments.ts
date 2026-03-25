export const environment = {
  production: false,
  
  // API Configuration
  api: {
    endpoint: 'https://g95hdf84lk.execute-api.ca-central-1.amazonaws.com/dev',
    // Set to `true` to keep using the built-in dev mock responses.
    // Set to `false` (recommended when the API Gateway stage is deployed)
    // so the frontend calls the real backend.
    useMock: false
  },

  // Named API paths (concatenate with `api.endpoint`)
  apiPaths: {
    chatQuery: '/chat/query',
    chatHistory: '/chat/history',
    documentsUpload: '/documents/upload',
    documentsStatus: '/documents/status',
    documentsList: '/documents/list',
    documentsDelete: '/documents'
  },
  
  // WebSocket Configuration - DISABLED (not using WebSocket)
  // ws: {
  //   endpoint: 'ws://localhost:8000/ws'
  // },
  
  // AWS Cognito Configuration — DISABLED (not using Cognito/Azure AD)
  // Uncomment and fill in real values to re-enable SSO via Cognito.
  // cognito: {
  //   userPoolId: 'ca-central-1_XXXXXXXXX',
  //   clientId: 'your-cognito-app-client-id',
  //   region: 'ca-central-1',
  //   domain: 'mech-avatar-dev.auth.ca-central-1.amazoncognito.com',
  //   redirectSignIn: 'http://localhost:4200/callback',
  //   redirectSignOut: 'http://localhost:4200/login',
  //   responseType: 'code',
  //   scope: ['openid', 'email', 'profile']
  // },

  // Feature Flags
  features: {
    enableKnowledgeBase: true,
    enableFileUpload: true,
    enableWebSocket: false,  // DISABLED - not using WebSocket
    enableActivityPanel: true,
    enableChatHistory: true,
    enablePersonas: true
  },

  // Validation Rules
  validation: {
    query: {
      minLength: 1,
      maxLength: 500
    },
    file: {
      maxSizeMB: 100,
      allowedExtensions: ['pdf', 'docx', 'txt', 'md', 'json', 'csv', 'xlsx', 'yaml', 'yml', 'py', 'js', 'ts', 'java'],
      allowedMimeTypes: [
        'application/pdf',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        'text/plain',
        'text/markdown',
        'application/json',
        'text/csv',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'application/x-yaml',
        'text/yaml'
      ]
    },
    rateLimit: {
      queriesPerHour: 100
    }
  },

  // Persona Configuration
  personas: [
    { id: 'new-joiner', name: 'New Joiner', description: 'Onboarding and learning assistance' },
    { id: 'designer', name: 'Designer', description: 'Technical documentation and explanations' },
    { id: 'qa-tester', name: 'QA Tester', description: 'Test case and coverage assistance' },
    { id: 'developer', name: 'Developer', description: 'Code and architecture support' },
    { id: 'tech-support', name: 'Technical Support', description: 'Production issue resolution' },
    { id: 'business-support', name: 'Business Support', description: 'Policy and operations guidance' }
  ]
};
