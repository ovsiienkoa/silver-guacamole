FROM python:3.13

WORKDIR /app

COPY . /app

RUN pip install --upgrade pip && pip install -r requirments.txt

CMD ["python", "cloudparser.py"]
