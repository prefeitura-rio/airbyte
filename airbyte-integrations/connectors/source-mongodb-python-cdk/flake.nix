{
  description = "Airbyte MongoDB Source Connector Development Environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        python = pkgs.python311;
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            # Python and uv
            python
            uv
            
            # Development tools
            git
            just
            docker
            
            # Airbyte CLI (if available) or tools needed for building
            nodejs_20
            
            # Linting tools
            ruff
            mypy
            
            # Other useful tools
            jq
            curl
            tree
          ];

          shellHook = ''
            echo "🚀 Airbyte MongoDB Source Connector Development Environment"
            echo "Python: $(python --version)"
            echo "uv: $(uv --version)"
            echo ""
            echo "Available commands:"
            echo "  just                       # Show available tasks"
            echo "  just install               # Install dependencies"
            echo "  just lint                  # Run linting"
            echo "  just check                 # Type check with mypy"
            echo "  just docker-info           # Show Docker configuration"
            echo "  just docker-release <tag>  # Build and push Docker image"
            echo "  uv run source-mongodb ...  # Run connector commands"
            echo ""
            
            # Create virtual environment if it doesn't exist
            if [ ! -d ".venv" ]; then
              echo "Creating virtual environment..."
              uv venv
            fi
            
            # Activate virtual environment
            source .venv/bin/activate
            
            # Install dependencies if pyproject.toml exists
            if [ -f "pyproject.toml" ]; then
              echo "Installing dependencies with uv..."
              uv sync
            fi
          '';

          # Environment variables
          PYTHONPATH = ".";
          UV_VENV = ".venv";
        };
      });
}