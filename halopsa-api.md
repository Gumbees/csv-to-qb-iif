# Halo API Documentation

## Authentication

### Authorisation Code Authentication
The Halo API uses OAuth 2.0 for authentication, specifically the Authorization Code flow. This type of authentication is used in web applications which have a server backend that can securely store client secrets (i.e. not a mobile or desktop app).

The typical workflow involves:
1. The user accesses your application
2. Your application redirects them to Halo's authentication endpoint (https://login.halopsa.com/oauth/authorize)
3. Users log in and authorize your application
4. Halo redirects back with an authorisation code
5. Your server exchanges this for an access token

#### Implementation Steps:

1. **Register your application** at https://login.halopsa.com/admin/applications - you will need to provide:
   - Application Name
   - Application URL (must be HTTPS)
   - Redirect URI (where the user is sent after authentication, e.g. https://myapp.com/halo-callback)

2. **Request initial authorization** from the user via a redirect to:
```
https://login.halopsa.com/oauth/authorize?
  response_type=code&
  client_id={your_client_id}&
  redirect_uri={your_redirect_uri}&
  scope={your_scopes}
```

3. **Exchange code for token** when user is redirected back with the authorization code:
```
POST https://login.halopsa.com/oauth/token
Content-Type: application/x-www-form-urlencoded

grant_type=authorization_code&
client_id={your_client_id}&
client_secret={your_client_secret}&
code={authorization_code}&
redirect_uri={your_redirect_uri}
```

4. **Use access token** to make API calls by including it in the Authorization header:
```
Authorization: Bearer {access_token}
```

## API Endpoints Overview

### Resources
- Agents
- Appointments  
- Assets
- Attachments
- Clients
- Contracts
- Invoices
- Items
- Knowledge Base
- Opportunities
- Projects
- Quotes
- Reports
- Sites
- Status
- Suppliers
- Teams
- Ticket Types
- Tickets
- Users

## Reporting and Reports

### GET /Report
Endpoint for retrieving Halo Report records.

**Permissions Required**: AgentReport Read

Returns an object containing the count of reports, and an array of report objects.

**Query Parameters:**
| Parameter | Type | Data Type | Description |
|-----------|------|-----------|-------------|
| count | query | int | Number of records to return |
| search | query | string | Filters response based on the search string |
| pageinate | query | bool | Whether to use Pagination in the response |
| page_size | query | int | When using Pagination, the size of the page |
| page_no | query | int | When using Pagination, the page number to return |
| orderby | query | string | The name of the first field to order by |
| orderbydesc | query | bool | Whether to order ascending or descending |
| orderby2 | query | string | The name of the second field to order by |
| orderbydesc2 | query | bool | Whether to order ascending or descending |
| orderby3 | query | string | The name of the third field to order by |
| orderbydesc3 | query | bool | Whether to order ascending or descending |
| orderby4 | query | string | The name of the fourth field to order by |
| orderbydesc4 | query | bool | Whether to order ascending or descending |
| orderby5 | query | string | The name of the fifth field to order by |
| orderbydesc5 | query | bool | Whether to order ascending or descending |
| ticket_id | query | int | Filters by the specified ticket |
| client_id | query | int | Filters by the specified client |
| site_id | query | int | Filters by the specified site |
| user_id | query | int | Filters by the specified user |
| reportgroup_id | query | int | Filters by the specified report group |
| chartonly | query | bool | Whether to return only records for reports that include graphs |

**Example Response:**
```json
{
  "root": {
    ...
  }
}
```

### GET /Report/{id}
Returns a single report object.

**Permissions Required**: AgentReport Read

**Path Parameters:**
| Parameter | Type | Data Type | Description |
|-----------|------|-----------|-------------|
| id | path (required) | int | The Report's ID |

**Query Parameters:**
| Parameter | Type | Data Type | Description |
|-----------|------|-----------|-------------|
| includedetails | query | bool | Whether to include extra objects in the response |
| loadreport | query | bool | Whether to include the report data in the response |

**Example Response:**
```json
{
  "root": {
    ...
  }
}
```

### POST /Report
Adds or updates one or more reports. If id is included then updates, if not included then creates new.

**Permissions Required**: AgentReport Modify

**Example Request:**
```json
{
  "root": [
    ...
  ]
}
```

### DELETE /Report/{id}
Deletes the report and related objects with the specified id.

**Permissions Required**: AgentReport Modify

**Path Parameters:**
| Parameter | Type | Data Type | Description |
|-----------|------|-----------|-------------|
| id | path (required) | int | The Report's ID |
