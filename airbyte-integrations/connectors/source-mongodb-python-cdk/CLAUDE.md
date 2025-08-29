# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Airbyte MongoDB source connector written in Python using the Airbyte CDK and PyMongo. The connector automatically discovers MongoDB databases and collections, creating streams for each collection with support for both full refresh and incremental sync modes using MongoDB's native `_id` field or custom cursor fields.

## Key Development Commands

### Environment Setup
```bash
# Enter development environment (uses Nix flake)
direnv allow

# Install dependencies 
just install
```

### Development Workflow
```bash
# Show all available tasks
just

# Common development tasks
just lint          # Run linting
just format        # Format code 
just check         # Type checking
just validate      # Run all checks
just clean         # Clean artifacts
```

### Local Testing
```bash
# Connector commands (using just)
just spec                              # Show connector spec
just check-connection                  # Test connection
just discover                          # Discover schema
just read /path/to/catalog.json        # Read data
just create-config                     # Create example config

# Or using uv directly
uv run source-mongodb spec
uv run source-mongodb check --config secrets/config.json
uv run source-mongodb discover --config secrets/config.json
uv run source-mongodb read --config secrets/config.json --catalog /path/to/configured_catalog.json
```

### Docker Operations
```bash
# Build connector image
just build
# Or: airbyte-ci connectors --name=source-mongodb build

# Run full test suite  
just test
# Or: airbyte-ci connectors --name=source-mongodb test
```

### Dependency Management
```bash
# Add new dependency
uv add <package-name>

# Add development dependency
uv add --dev <package-name>
```

## Architecture Overview

### Core Components

**Source (`source_mongodb/source.py`)**
- `SourceMongodb`: Main connector class extending `AbstractSource`
- `get_connection_string()`: Builds MongoDB connection string with authentication and TLS options
- `check_connection()`: Validates connection using MongoDB's `ping` command
- `streams()`: Discovers databases and collections, creates appropriate stream instances
- Handles both single database and multi-database discovery modes
- Excludes system databases (`admin`, `local`, `config`) and system collections (starting with `system.`)

**Streams (`source_mongodb/streams.py`)**
- `MongoStream`: Base stream class with common MongoDB functionality
  - Connection management with PyMongo client
  - Document serialization (ObjectId → string conversion)
  - Pagination support with configurable page sizes
- `MongoCollection`: Full refresh stream for complete collection syncs
- `MongoCollectionIncremental`: Incremental stream with cursor-based state management
  - Default cursor field: `_id` (ObjectId)
  - Configurable cursor fields for custom incremental logic
  - Proper state management for resumable syncs

**Entry Points**
- `main.py`: Docker entrypoint calling `source_mongodb.run:run`
- `source_mongodb/run.py`: CLI entrypoint using Airbyte CDK's `launch` function

### Data Flow

1. **Connection**: Connector builds MongoDB connection string with auth/TLS parameters
2. **Discovery**: Queries `list_database_names()` and `list_collection_names()` for each database
3. **Stream Creation**: Creates `MongoCollection` or `MongoCollectionIncremental` for each collection
4. **Incremental Logic**: Uses cursor fields to fetch only new/updated documents
5. **Serialization**: Converts BSON types (especially ObjectId) to JSON-compatible formats
6. **Pagination**: Fetches documents in configurable batches for memory efficiency

### Configuration Schema

Required fields (defined in `source_mongodb/spec.yaml`):
- `connection_string`: Complete MongoDB URI with all connection parameters

Optional fields:
- `database`: Specific database to sync (overrides database in connection string)
- `page_size`: Documents per batch (default: 1000)
- `cursor_field`: Custom cursor field for incremental syncs (default: _id)

The connection string supports all standard MongoDB URI features:
- Authentication: `mongodb://username:password@host:port/database`
- Replica sets: `mongodb://host1:port1,host2:port2/database?replicaSet=mySet`
- TLS/SSL: `mongodb://host:port/database?tls=true`
- MongoDB Atlas: `mongodb+srv://username:password@cluster.mongodb.net/database`

### Development Environment

**Nix Flake Setup (`flake.nix`)**
- Python 3.11 runtime
- uv for dependency management
- just for task automation
- Development tools (ruff, mypy)
- Automatic virtual environment activation via direnv

**Task Automation (`justfile`)**
- Standardized development commands
- Linting, formatting, and type checking
- Connector testing and building
- Configuration management


## Development Patterns

### Stream Inheritance Hierarchy
```
Stream (Airbyte CDK)
└── MongoStream (base MongoDB functionality)
    ├── MongoCollection (full refresh)
    └── MongoCollectionIncremental (incremental sync)
```

### Connection String Building
The connector builds MongoDB connection strings dynamically based on configuration:
- Handles authentication with username/password
- Supports TLS with certificate validation options
- Configurable authentication source database
- URL encoding of credentials for special characters

### ObjectId Serialization
MongoDB's ObjectId fields are automatically converted to strings throughout the document tree:
- Primary `_id` field conversion
- Nested ObjectId fields in subdocuments
- ObjectId values in arrays
- Recursive serialization for complex document structures

### Incremental Sync Strategy
- Uses MongoDB's native `_id` field (ObjectId) as default cursor for natural document ordering
- Supports custom cursor fields (e.g., timestamps, sequence numbers)
- Maintains cursor state between sync runs for resumability
- Handles ObjectId comparison as strings for consistent state management

### Error Handling
Comprehensive error handling for MongoDB-specific scenarios:
- Authentication failures
- Connection timeouts and network issues
- SSL/TLS certificate validation errors
- Database/collection access permissions

## Version Management

When publishing new versions:
1. Update `dockerImageTag` in `metadata.yaml`
2. Update `version` in `pyproject.toml`
3. Ensure `metadata.yaml` content reflects MongoDB-specific details
4. Update connector documentation and changelog

## MongoDB-Specific Considerations

- **Collection Discovery**: Automatically excludes system collections for cleaner stream lists
- **BSON Handling**: Proper serialization of MongoDB-specific data types
- **Indexing**: Leverages MongoDB's natural document ordering for efficient incremental syncs
- **Pagination**: Uses MongoDB's `skip()` and `limit()` for memory-efficient large collection processing
- **Connection Pooling**: PyMongo client handles connection pooling automatically