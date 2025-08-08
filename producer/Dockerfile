FROM python:3.10

WORKDIR /app
COPY producer/ .  
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "fraud_detection_sim_data_script.py"]
