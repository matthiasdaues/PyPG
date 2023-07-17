FROM python:3.11.4-alpine

RUN apk update && apk add --no-cache postgresql

WORKDIR /app

COPY requirements.txt ./requirements.txt

RUN pip install --no-cache-dir --upgrade -r ./requirements.txt

COPY . ./app

CMD ["python", "code/run_setup_database.py"]
