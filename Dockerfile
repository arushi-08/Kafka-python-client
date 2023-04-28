FROM python:3.11

WORKDIR /app
RUN apt-get update && apt-get install -y coreutils

COPY . .

RUN pip install -r requirements.txt

CMD ["/bin/bash", "benchmark.sh"]