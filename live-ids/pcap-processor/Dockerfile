FROM cicflowmeter:amd64

USER root

WORKDIR /app

RUN apk add --update --no-cache python3 py3-pip && \
    pip3 install --break-system-packages watchdog

COPY pcap_watcher.py .

ENTRYPOINT []

CMD ["python3", "-u", "pcap_watcher.py"]