FROM python:3.6

WORKDIR /

COPY client.py /

CMD python3 client.py
