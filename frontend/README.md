# Gigs Near Me Frontend

This is a simple React single-page application for querying event recommendations.

## Development

Install dependencies and start the development server:

```
npm install
npm run dev
```

The app expects a backend service exposing an `/api/match` endpoint accepting a `MatchingRequest` JSON and returning a list of `RecommendedEvent` objects.
