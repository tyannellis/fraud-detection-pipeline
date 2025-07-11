FROM python:3.10

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir faker confluent-kafka

CMD ["python", "fraud_detection_sim_data_script.py"]
