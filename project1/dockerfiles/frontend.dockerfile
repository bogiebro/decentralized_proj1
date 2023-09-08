FROM python:3.6

WORKDIR /

COPY frontend.py rwlock.py /

CMD python3 frontend.py
