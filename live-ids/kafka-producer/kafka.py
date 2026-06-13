import os
import time
import json
import shutil
import queue
import threading
import pandas as pd

from confluent_kafka import Producer
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


WATCH_DIR = os.getenv("WATCH_DIR", "/data/captures/input_csv")
PROCESSED_DIR = os.getenv("PROCESSED_DIR", "/data/processed_csv")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "network-flows")

BATCH_SIZE = int(os.getenv("PRODUCER_BATCH_SIZE", "1000"))
BATCH_SLEEP = float(os.getenv("PRODUCER_BATCH_SLEEP", "0.5"))


producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP
})

file_queue = queue.Queue()

queued_files = set()
processing_files = set()
processed_files = set()

state_lock = threading.Lock()


def delivery_report(err, msg):
    if err is not None:
        print(f"[Kafka-Producer] [ERROR] Delivery failed: {err}", flush=True)


def wait_until_file_ready(path, checks=3, delay=1):
    previous_size = -1

    for _ in range(checks):
        if not os.path.exists(path):
            return False

        current_size = os.path.getsize(path)

        if current_size == previous_size:
            return True

        previous_size = current_size
        time.sleep(delay)

    return True


def enqueue_file(path):
    if not path.endswith(".csv"):
        return

    if not os.path.exists(path):
        return

    filename = os.path.basename(path)

    with state_lock:
        if (
            path in queued_files
            or filename in processing_files
            or filename in processed_files
        ):
            return

        queued_files.add(path)

    print(f"[Kafka-Producer] [INFO] Queued CSV: {filename}", flush=True)
    file_queue.put(path)


def publish_csv(path):
    filename = os.path.basename(path)

    if not filename.endswith(".csv"):
        return

    with state_lock:
        if filename in processed_files or filename in processing_files:
            queued_files.discard(path)
            return

        processing_files.add(filename)
        queued_files.discard(path)

    if not os.path.exists(path):
        with state_lock:
            processing_files.discard(filename)
        return

    print(f"[Kafka-Producer] Detected CSV: {filename}", flush=True)

    try:
        if not wait_until_file_ready(path):
            print(f"[Kafka-Producer] [WARNING] File was removed before processing: {filename}", flush=True)
            return

        df = pd.read_csv(path)
        df.columns = [c.strip() for c in df.columns]

        sent_count = 0
        for _, row in df.iterrows():
            record = row.where(pd.notnull(row), None).to_dict()
            record["source_file"] = filename

            producer.produce(
                KAFKA_TOPIC,
                value=json.dumps(record).encode("utf-8"),
                callback=delivery_report
            )

            producer.poll(0)
            sent_count += 1

            if sent_count % BATCH_SIZE == 0:
                producer.flush()
                print(f"[Kafka-Producer] [INFO] Sent {sent_count} rows from {filename}", flush=True)
                time.sleep(BATCH_SLEEP)
                
        producer.flush()
        print(f"[Kafka-Producer] [INFO] Finished sending {sent_count} rows from {filename}", flush=True)

        os.makedirs(PROCESSED_DIR, exist_ok=True)

        destination = os.path.join(PROCESSED_DIR, filename)

        if os.path.exists(path):
            shutil.move(path, destination)

        with state_lock:
            processed_files.add(filename)

    except FileNotFoundError:
        print(f"[Kafka-Producer] [WARNING] Skipped already moved file: {filename}", flush=True)

    except Exception as e:
        print(f"[Kafka-Producer] [ERROR] Failed to process {filename}: {e}", flush=True)

    finally:
        with state_lock:
            processing_files.discard(filename)
            queued_files.discard(path)


def worker():
    while True:
        path = file_queue.get()

        try:
            publish_csv(path)
        finally:
            file_queue.task_done()


def scan_existing_files():
    for file in os.listdir(WATCH_DIR):
        path = os.path.join(WATCH_DIR, file)

        if os.path.isfile(path) and file.endswith(".csv"):
            enqueue_file(path)


class CsvHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return

        enqueue_file(event.src_path)

    def on_moved(self, event):
        if event.is_directory:
            return

        enqueue_file(event.dest_path)


if __name__ == "__main__":
    os.makedirs(WATCH_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    print(f"[Kafka-Producer] [INFO] Watching: {WATCH_DIR}", flush=True)
    print(f"[Kafka-Producer] [INFO] Kafka: {KAFKA_BOOTSTRAP}", flush=True)
    print(f"[Kafka-Producer] [INFO] Topic: {KAFKA_TOPIC}", flush=True)

    threading.Thread(target=worker, daemon=True).start()

    scan_existing_files()

    observer = Observer()
    observer.schedule(CsvHandler(), WATCH_DIR, recursive=False)
    observer.start()

    try:
        while True:
            scan_existing_files()
            time.sleep(5)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()