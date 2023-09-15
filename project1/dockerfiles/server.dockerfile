FROM python:3.6

WORKDIR /

COPY server.py /
EXPOSE 9000-9999
CMD python3 server.py -i $SERVER_ID
