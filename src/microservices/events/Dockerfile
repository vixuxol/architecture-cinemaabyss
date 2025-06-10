FROM python:3.12.3-slim

RUN apt-get update && apt-get install -y libpq-dev build-essential && apt-get install -y curl

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "__main__.py"]