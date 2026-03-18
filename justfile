# SF Permitting Bottleneck Analyzer

# Run the full pipeline: fetch data, clean, generate report, launch dashboard
up: fetch clean report dashboard

# Fetch raw data from DataSF (cached for 24h)
fetch:
    uv run python main.py fetch

# Clean and process raw data
clean:
    uv run python main.py clean

# Run analysis and print summary to terminal
analyze:
    uv run python main.py analyze

# Generate HTML policy brief
report:
    uv run python main.py report

# Launch interactive Dash dashboard at http://127.0.0.1:8050
dashboard:
    uv run python main.py dashboard

# Run full pipeline end-to-end
pipeline:
    uv run python main.py pipeline

# Run unit tests
test:
    uv run pytest tests/ -v

# Run mypy type checking
typecheck:
    uv run mypy src/

# Build and run with Docker
docker-up:
    docker compose up --build

# Format + lint (add ruff later if desired)
check: typecheck test

# Force re-download all data (ignores cache)
fetch-fresh:
    uv run python main.py fetch --force

# Install all dependencies
install:
    uv sync
