# MongoDB Source

This is the repository for the MongoDB source connector, written in Python using the Airbyte CDK.
For information about how to use this connector within Airbyte, see [the documentation](https://docs.airbyte.com/integrations/sources/mongodb).

## Local development

### Prerequisites

* Nix with flakes enabled - installation instructions [here](https://nixos.org/download.html#nix-install-linux)
* direnv - installation instructions [here](https://direnv.net/docs/installation.html)

### Setting up the development environment

1. Allow direnv for this directory:
```bash
direnv allow
```

This will automatically set up the development environment using Nix flake, including:
- Python 3.11
- uv for dependency management
- just for task automation
- Development tools (ruff, mypy)

### Installing the connector

From this connector directory, run:
```bash
just install
```

Or alternatively:
```bash
uv sync
```

### Available tasks

View all available development tasks:
```bash
just
```

Common tasks:
```bash
just lint          # Run linting
just format        # Format code
just check         # Type checking
just validate      # Run all checks
just spec          # Show connector spec
```

### Create credentials

**If you are a community contributor**, follow the instructions in the [documentation](https://docs.airbyte.com/integrations/sources/mongodb)
to generate the necessary credentials. Then create a file `secrets/config.json` conforming to the `source_mongodb/spec.yaml` file.
Note that any directory named `secrets` is gitignored across the entire Airbyte repo, so there is no danger of accidentally checking in sensitive information.

### Locally running the connector

Using just (recommended):
```bash
just spec                              # Show connector specification
just check-connection                  # Test connection (requires secrets/config.json)
just discover                          # Discover schema (requires secrets/config.json)
just read /path/to/catalog.json        # Read data (requires config and catalog)
just create-config                     # Create example config file
```

Or using uv directly:
```bash
uv run source-mongodb spec
uv run source-mongodb check --config secrets/config.json
uv run source-mongodb discover --config secrets/config.json
uv run source-mongodb read --config secrets/config.json --catalog /path/to/configured_catalog.json
```


### Docker workflow

#### Setup Docker environment
1. Copy the example environment file and customize it:
```bash
cp .env.example .env
# Edit .env to set your Docker Hub username
```

2. Reload direnv to load the environment variables:
```bash
direnv reload
```

#### Building and pushing Docker images

Check your Docker configuration:
```bash
just docker-info
```

Build and push a new version:
```bash
just docker-release 0.1.0    # Builds and pushes your-username/airbyte-source-mongodb-python:0.1.0
```

Or build and push separately:
```bash
just docker-build 0.1.0      # Build only
just docker-push 0.1.0       # Push only
```

#### Using Airbyte CI (alternative)

You can also use the official Airbyte CI tools:
```bash
just build    # Uses airbyte-ci to build
just test     # Uses airbyte-ci to test
```

#### Running as a docker container

After building, run any of the connector commands:
```bash
docker run --rm your-username/airbyte-source-mongodb-python:0.1.0 spec
docker run --rm -v $(pwd)/secrets:/secrets your-username/airbyte-source-mongodb-python:0.1.0 check --config /secrets/config.json
docker run --rm -v $(pwd)/secrets:/secrets your-username/airbyte-source-mongodb-python:0.1.0 discover --config /secrets/config.json
```


### Development workflow

The project uses `just` for task automation. Common development tasks:

```bash
just install       # Install dependencies
just lint          # Check code style
just lint-fix      # Fix linting issues automatically
just format        # Format code
just check         # Type checking with mypy
just validate      # Run all checks (lint + format + type check)
just clean         # Clean build artifacts
```

### Dependency Management

All dependencies are managed via uv. To add a new dependency:

```bash
uv add <package-name>              # Add runtime dependency
uv add --dev <package-name>        # Add development dependency
```

Please commit the changes to `pyproject.toml` and `uv.lock` files.

## Publishing a new version of the connector

You've checked out the repo, implemented a million dollar feature, and you're ready to share your changes with the world. Now what?
1. Bump the connector version (please follow [semantic versioning for connectors](https://docs.airbyte.com/contributing-to-airbyte/resources/pull-requests-handbook/#semantic-versioning-for-connectors)): 
    - bump the `dockerImageTag` value in `metadata.yaml`
    - bump the `version` value in `pyproject.toml`
2. Make sure the `metadata.yaml` content is up to date.
3. Make sure the connector documentation and its changelog is up to date (`docs/integrations/sources/mongodb.md`).
4. Create a Pull Request: use [our PR naming conventions](https://docs.airbyte.com/contributing-to-airbyte/resources/pull-requests-handbook/#pull-request-title-convention).
5. Pat yourself on the back for being an awesome contributor.
6. Someone from Airbyte will take a look at your PR and iterate with you to merge it into master.
7. Once your PR is merged, the new version of the connector will be automatically published to Docker Hub and our connector registry.

## MongoDB Configuration

### Connection Configuration

The connector supports MongoDB connection via connection string URI:

- **connection_string**: Complete MongoDB URI with all connection parameters (required)
  - Examples: 
    - `mongodb://localhost:27017`
    - `mongodb://username:password@localhost:27017/mydb?authSource=admin`
    - `mongodb+srv://username:password@cluster.mongodb.net/mydb?retryWrites=true&w=majority`
    - `mongodb://localhost:27017,localhost:27018/mydb?replicaSet=myReplicaSet`
- **database**: Specific database to sync (optional, overrides database in connection string)
- **page_size**: Documents per batch (default: 1000)
- **cursor_field**: Field to use as cursor for incremental syncs (default: _id)

### Sync Modes

The connector supports:
- **Full Refresh**: Syncs all documents in a collection
- **Incremental**: Syncs only new/updated documents based on the cursor field

### Collections

The connector automatically discovers all collections in the specified database(s) and creates streams for each:
- System collections (starting with `system.`) are automatically excluded
- Stream names follow the format `{database_name}_{collection_name}`
- Each collection stream uses `_id` as the primary key
- Incremental syncs use the configured cursor field (default: `_id`)

### Data Types

MongoDB documents are serialized to JSON with the following transformations:
- `ObjectId` fields are converted to strings
- Nested documents and arrays are preserved
- All other BSON types are handled according to PyMongo's default serialization