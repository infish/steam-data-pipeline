FROM python:3.10

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

RUN mkdir -p logs

CMD ["python", "src/main.py"]
