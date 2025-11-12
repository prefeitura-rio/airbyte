# Typesense Source

This is the repository for the Typesense source connector, written in Python using the Airbyte CDK.

## Configuration

The connector supports Typesense connection via the following parameters:

- **protocol**: Protocol to use (http or https, default: http)
- **host**: Typesense hostname or IP address (required)
  - Examples: `localhost`, `typesense.example.com`, `192.168.1.100`
- **port**: Typesense port (default: 8108)
- **api_key**: Typesense API key for authentication (required)
- **page_size**: Documents per batch (default: 250)

## Sync Modes

The connector currently supports:
- **Full Refresh**: Syncs all documents in a collection using the export endpoint

## Collections

The connector automatically discovers all collections in the Typesense instance and creates streams for each:
- Stream names match the collection names
- Each collection stream uses `id` as the primary key

## Data Structure

Documents are structured as `{id, data}`:
```json
{
  "id": "document-id",
  "data": {
    "field1": "value1",
    "field2": "value2",
    ...
  }
}
```

This structure handles dynamic schemas well, as all collection-specific fields are nested under `data`.
