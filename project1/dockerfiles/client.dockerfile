FROM python:3.6

WORKDIR /

COPY client.py /
EXPOSE 7000-7999
CMD python3 client.py -i $CLIENT_ID
