FROM python:3.11.4-alpine

RUN apk update && apk add --no-cache postgresql

WORKDIR /app

COPY requirements.txt ./requirements.txt

RUN pip install --upgrade pip
RUN pip install --no-cache-dir --upgrade -r ./requirements.txt

COPY ./code ./code

CMD ["python", "code/run_setup_database.py"]
