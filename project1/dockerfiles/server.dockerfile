FROM python:3.6

WORKDIR /

COPY server.py /

CMD python3 server.py
