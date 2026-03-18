FROM python:3.12-slim

RUN pip install --no-cache-dir uv

WORKDIR /app

COPY pyproject.toml uv.lock .python-version ./
COPY src/ ./src/
COPY main.py ./

RUN uv sync --no-dev

EXPOSE 8050

CMD ["uv", "run", "python", "main.py", "dashboard"]
