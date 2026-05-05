# API Notes

All APIs are server-side Next.js route handlers. External VEX/RobotEvents APIs are never called from the browser.

## Public APIs

```http
GET /api/health
GET /api/teams/:teamNumber
GET /api/events/:eventSku
GET /api/events/:eventSku/rankings
```

Validation:

- Team number: `7157B` style, normalized uppercase
- Event SKU: `RE-V5RC-26-4025` style, normalized uppercase

## Admin API

```http
POST /api/admin/refresh
Authorization: Bearer <ADMIN_TOKEN>
```

Body:

```json
{
  "eventSku": "RE-V5RC-26-4025",
  "teamNumber": "7157B",
  "source": "mock"
}
```

`source` may be `mock` or `live`. Live requires `ROBOTEVENTS_API_KEY`.

## Error Shape

```json
{
  "error": {
    "code": "invalid_team_number",
    "message": "Invalid team number"
  }
}
```

No raw stack traces or secrets are returned.
