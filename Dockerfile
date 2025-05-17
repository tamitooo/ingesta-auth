FROM python:3-slim
WORKDIR /programas/ingesta
RUN mkdir -p /home/ubuntu/.aws
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN chmod +x ./ingesta-analisis.py
CMD ["python3", "./ingesta-auth.py"]
