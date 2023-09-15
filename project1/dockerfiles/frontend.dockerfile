FROM python:3.6

WORKDIR /

COPY frontend.py rwlock.py /

EXPOSE 8001
CMD python3 frontend.py
