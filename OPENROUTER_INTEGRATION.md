# OpenRouter AI Provider Integration

**Status**: Implementation Complete ✅
**Date**: 2025-10-30
**Feature**: Multi-Provider AI Support (Anthropic + OpenRouter)

## Overview

Added OpenRouter as an alternative AI provider alongside Anthropic's direct API. Users can now choose between:

- **⚛️ Anthropic Direct**: Best performance with direct API access to Claude models
- **🔀 OpenRouter**: Unified API for multiple AI providers (Anthropic, OpenAI, Google, Meta)

## What Was Changed

### 1. Database Schema (`src/database.js`)

**New Settings Added**:
- `ai_provider` - Provider selection ('anthropic' or 'openrouter')
- `openrouter_api_key` - OpenRouter API key
- `openrouter_model` - Selected OpenRouter model

**Migration Support**:
- Automatically adds new settings to existing databases
- Default provider: Anthropic (maintains backward compatibility)

### 2. AI Service Layer (`src/ai-service.js`)

**Enhanced Initialization**:
```javascript
aiService.initialize({
    provider: 'anthropic' | 'openrouter',
    anthropic_api_key: 'sk-ant-...',
    anthropic_model: 'claude-sonnet-4-20250514',
    openrouter_api_key: 'sk-or-...',
    openrouter_model: 'anthropic/claude-3.5-sonnet'
});
```

**New Methods**:
- `callOpenRouter(prompt, systemPrompt)` - OpenRouter API client
- `callAI(prompt, systemPrompt)` - Provider-agnostic wrapper

**Features**:
- Native HTTPS client for OpenRouter (no external dependencies)
- Automatic response format conversion to Anthropic-compatible structure
- Provider-specific error handling

### 3. Backend API (`src/server.js`)

**Updated Endpoints**:

```
GET /api/ai/settings
- Returns current provider, API keys (masked), model selections
- Includes readiness status for current provider

POST /api/ai/settings
- Accepts provider selection and configuration for both providers
- Validates provider choice
- Re-initializes AI service with new settings

GET /api/ai/openrouter-models
- Returns list of available OpenRouter models
- Includes model provider information (Anthropic, OpenAI, Google, Meta)

POST /api/ai/test-connection
- Tests connection to selected provider
- Supports both Anthropic and OpenRouter
- Returns test response for verification
```

**Available OpenRouter Models**:
- `anthropic/claude-3.5-sonnet` - Best balance (recommended)
- `anthropic/claude-sonnet-4-20250514` - Most capable
- `openai/gpt-4-turbo` - OpenAI's fastest GPT-4
- `openai/gpt-4o` - OpenAI flagship
- `google/gemini-pro-1.5` - Google's advanced AI
- `meta-llama/llama-3.1-70b-instruct` - Meta's open-source powerhouse

### 4. Frontend UI (`src/index.html`)

**New Interface Elements**:

1. **Provider Selection Radio Buttons**:
   - Clear visual distinction between providers
   - Shows benefits of each option

2. **Provider-Specific Configuration Panels**:
   - Anthropic Settings: API key + model dropdown
   - OpenRouter Settings: API key + model dropdown + benefits info

3. **Enhanced Model Descriptions**:
   - Shows model provider (OpenAI, Google, Meta, etc.)
   - Displays model capabilities and use cases

4. **Test Connection Support**:
   - Provider-aware connection testing
   - Clear success/error messages

5. **N8N Teaser**:
   - Coming soon banner for advanced workflow automation

**JavaScript Functions Updated**:
- `switchAIProvider()` - Toggle between provider UIs
- `loadAISettings()` - Load both provider configurations
- `saveAISettings()` - Save provider-specific settings
- `testAIConnection()` - Test selected provider

## Usage

### For Users

1. **Navigate to AI Settings**:
   - Click "🤖 AI Settings" in the sidebar

2. **Choose Provider**:
   - Select **Anthropic Direct** for best performance and direct Claude access
   - Select **OpenRouter** for unified access to multiple AI providers

3. **Configure Selected Provider**:

   **Anthropic**:
   - Enter API key from [console.anthropic.com](https://console.anthropic.com)
   - Select Claude model

   **OpenRouter**:
   - Enter API key from [openrouter.ai/keys](https://openrouter.ai/keys)
   - Select model from available providers

4. **Test Connection**:
   - Click "🔌 Test Connection" to verify API key

5. **Save Settings**:
   - Click "💾 Save Settings"
   - System shows provider status and readiness

### For Developers

**Server Initialization**:
The AI service is initialized on server startup with configuration from the database:

```javascript
// Server loads settings from database
const aiSettings = await db.all('SELECT key, value FROM ai_settings');
const config = {};
aiSettings.forEach(s => {
    config[s.key] = s.value;
});

// Initialize AI service with configuration
aiService.initialize(config);
```

**Adding New Providers**:
To add a new AI provider:

1. Add provider option to database defaults
2. Create `call<Provider>()` method in `AIService`
3. Update `callAI()` to route to new provider
4. Add provider test logic to `/api/ai/test-connection`
5. Update frontend UI with provider option

## Benefits of OpenRouter

- **Unified Access**: Single API for Anthropic, OpenAI, Google, Meta, and more
- **Cost Optimization**: Automatic routing to most cost-effective provider
- **Failover Support**: Built-in failover if primary model is unavailable
- **Pay-as-You-Go**: No upfront commitments, pay only for what you use
- **Latest Models**: Access cutting-edge models from all major providers

## Testing

### Manual Testing Steps

1. **Test Anthropic Provider**:
   - Select Anthropic radio button
   - Enter valid Anthropic API key
   - Select Claude model
   - Click "Test Connection"
   - Verify success message

2. **Test OpenRouter Provider**:
   - Select OpenRouter radio button
   - Enter valid OpenRouter API key
   - Select model (e.g., `anthropic/claude-3.5-sonnet`)
   - Click "Test Connection"
   - Verify success message

3. **Test Provider Switching**:
   - Save settings with Anthropic
   - Switch to OpenRouter
   - Save settings with OpenRouter
   - Reload page and verify correct provider is selected

4. **Test AI Features**:
   - Configure provider
   - Navigate to Transaction Mappings
   - Click "Generate AI Suggestions"
   - Verify suggestions are generated correctly

## Error Handling

- Invalid provider selection returns 400 error
- Missing API key shows user-friendly error
- Connection test failures show detailed error messages
- API keys are masked in UI for security
- Provider-specific error messages in logs

## Security Considerations

- API keys stored in database (not in code)
- Keys masked in frontend display (shows last 4 chars)
- Keys never logged in server output
- HTTPS required for OpenRouter API calls
- API key validation before making requests

## Future Enhancements

### N8N Workflow Integration (Coming Soon)
Advanced AI workflows with visual automation builder:
- Multi-step AI processes with branching logic
- Data transformations between steps
- External integrations (webhooks, databases, APIs)
- Scheduled workflows and triggers

### Potential Improvements
- Model performance analytics
- Cost tracking per provider
- A/B testing between providers
- Custom model routing rules
- Rate limiting and quota management

## Migration Notes

**Existing Users**:
- No action required - defaults to Anthropic provider
- Existing Anthropic API keys continue to work
- Can switch to OpenRouter at any time

**Database Schema**:
- Automatic migration adds new settings
- No data loss or manual intervention needed

## API Reference

### AI Settings Endpoints

**GET /api/ai/settings**
```json
{
    "success": true,
    "settings": {
        "ai_provider": "anthropic",
        "anthropic_api_key": "••••••••sk-ant-1234",
        "anthropic_model": "claude-sonnet-4-20250514",
        "openrouter_api_key": null,
        "openrouter_model": "anthropic/claude-3.5-sonnet",
        "is_configured": true,
        "current_provider": "anthropic"
    }
}
```

**POST /api/ai/settings**
```json
{
    "ai_provider": "openrouter",
    "openrouter_api_key": "sk-or-v1-abc123...",
    "openrouter_model": "anthropic/claude-3.5-sonnet"
}
```

Response:
```json
{
    "success": true,
    "message": "AI settings saved successfully",
    "provider": "openrouter",
    "is_ready": true
}
```

**GET /api/ai/openrouter-models**
```json
{
    "success": true,
    "models": [
        {
            "id": "anthropic/claude-3.5-sonnet",
            "name": "Claude 3.5 Sonnet",
            "description": "Best balance of performance and cost",
            "provider": "Anthropic"
        }
    ]
}
```

**POST /api/ai/test-connection**
```json
{
    "provider": "openrouter",
    "api_key": "sk-or-v1-abc123..."
}
```

Response:
```json
{
    "success": true,
    "message": "OpenRouter API connection successful",
    "response": "Connection successful"
}
```

## Troubleshooting

**Issue**: "AI service not initialized"
- **Solution**: Configure and save API key in AI Settings

**Issue**: "Invalid API key"
- **Solution**: Verify API key is correct and active in provider dashboard

**Issue**: "Provider not found"
- **Solution**: Ensure `ai_provider` is either 'anthropic' or 'openrouter'

**Issue**: OpenRouter returns 401
- **Solution**: Check API key has credits and is valid

**Issue**: Anthropic returns 401
- **Solution**: Verify API key from console.anthropic.com

## Related Files

- `src/database.js` - Schema and migrations
- `src/ai-service.js` - Core AI service logic
- `src/server.js` - API endpoints
- `src/index.html` - Frontend UI and JavaScript
- `CLAUDE.md` - Project documentation

## Conclusion

The OpenRouter integration provides flexible AI provider options while maintaining backward compatibility. Users can choose between direct Anthropic access for optimal performance or OpenRouter for unified multi-provider access. The implementation is production-ready with comprehensive error handling, security measures, and user-friendly interfaces.
