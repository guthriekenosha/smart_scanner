FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8081

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Build Tailwind CSS during image build (no Node required)
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY tailwind.config.js /app/tailwind.config.js
COPY static/ /app/static/
COPY templates/ /app/templates/
COPY app.py /app/app.py

# Download Tailwind CLI and compile CSS
RUN curl -sSL -o /usr/local/bin/tailwindcss \
      https://github.com/tailwindlabs/tailwindcss/releases/download/v3.4.10/tailwindcss-linux-x64 \
    && chmod +x /usr/local/bin/tailwindcss \
    && tailwindcss -c /app/tailwind.config.js -i /app/static/tailwind.css -o /app/static/build.css --minify

EXPOSE 8081

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8081"]
