FROM python:3.10

WORKDIR /app
COPY consumer/ .  
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

CMD ["python", "simulation_consumer.py"]
