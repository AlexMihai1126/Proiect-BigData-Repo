import os
import time
import shutil
import subprocess
import threading
from queue import Queue
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

PCAP_INPUT_TEMPLATE = os.getenv("PCAP_INPUT_TEMPLATE", "/tmp/pcap/input_{}")
PCAP_OUTPUT_TEMPLATE = os.getenv("PCAP_OUTPUT_TEMPLATE", "/tmp/pcap/output_{}")

WATCH_DIR = os.getenv("WATCH_DIR", "/tmp/pcap/input_pcap")
CSV_READY_DIR = os.getenv("CSV_READY_DIR", "/tmp/pcap/input_csv")
PROCESSED_DIR = os.getenv("PROCESSED_DIR", "/tmp/pcap/processed_pcap")
FAILED_DIR = os.getenv("FAILED_DIR", "/tmp/pcap/failed_pcap")

CICFLOWMETER_CWD = os.getenv("CICFLOWMETER_CWD", "/CICFlowMeter/bin")
CICFLOWMETER_COMMAND = os.getenv("CICFLOWMETER_COMMAND", "./cfm")

pcap_queue = Queue()
queued_files = set()
processed_files = set()
lock = threading.Lock()


def is_pcap(path):
    return os.path.isfile(path) and path.endswith(".pcap")


def safe_name(filename):
    name = os.path.splitext(filename)[0]
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in name)


def wait_until_file_ready(path, checks=5, delay=1):
    previous_size = -1
    stable_checks = 0

    while stable_checks < checks:
        if not os.path.exists(path):
            return False

        current_size = os.path.getsize(path)

        if current_size > 0 and current_size == previous_size:
            stable_checks += 1
        else:
            stable_checks = 0

        previous_size = current_size
        time.sleep(delay)

    return True


def build_job_dirs(filename):
    base = safe_name(filename)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    job_name = f"{base}_{timestamp}"

    input_dir = PCAP_INPUT_TEMPLATE.format(job_name)
    output_dir = PCAP_OUTPUT_TEMPLATE.format(job_name)

    return input_dir, output_dir


def unique_destination(folder, filename):
    destination = os.path.join(folder, filename)

    if not os.path.exists(destination):
        return destination

    base, ext = os.path.splitext(filename)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")

    return os.path.join(folder, f"{base}_{timestamp}{ext}")


def move_generated_csvs(output_dir):
    moved_count = 0

    os.makedirs(CSV_READY_DIR, exist_ok=True)

    for file in os.listdir(output_dir):
        source_path = os.path.join(output_dir, file)

        if not os.path.isfile(source_path):
            continue

        if not file.endswith(".csv"):
            continue

        destination_path = unique_destination(CSV_READY_DIR, file)

        shutil.move(source_path, destination_path)

        print(f"[PCAP-Processor] [INFO] Moved CSV to Kafka input: {destination_path}", flush=True)
        moved_count += 1

    if moved_count == 0:
        raise RuntimeError(f"PCAP-Processor] [CIC] [ERROR] - CICFlowMeter finished but no CSV files were found in {output_dir}")

    return moved_count


def enqueue_pcap(path):
    filename = os.path.basename(path)

    if not filename.endswith(".pcap"):
        return

    if not os.path.exists(path):
        return

    with lock:
        if filename in queued_files or filename in processed_files:
            return

        queued_files.add(filename)

    print(f"[PCAP-Processor] [INFO] Detected PCAP: {filename}", flush=True)

    try:
        if not wait_until_file_ready(path):
            print(f"[PCAP-Processor] [WARNING] File was deleted before queueing: {filename}", flush=True)
            return

        input_dir, output_dir = build_job_dirs(filename)

        os.makedirs(input_dir, exist_ok=False)
        os.makedirs(output_dir, exist_ok=False)

        temp_pcap_path = os.path.join(input_dir, filename)

        shutil.move(path, temp_pcap_path)

        pcap_queue.put({
            "filename": filename,
            "input_dir": input_dir,
            "output_dir": output_dir,
            "pcap_path": temp_pcap_path,
        })

        print(f"[PCAP-Processor] [INFO] Queued: {filename}", flush=True)
        print(f"[PCAP-Processor] [INFO] Temp input folder: {input_dir}", flush=True)
        print(f"[PCAP-Processor] [INFO] Temp output folder: {output_dir}", flush=True)

    except Exception as e:
        print(f"[PCAP-Processor] [ERROR] Failed to queue {filename}: {e}", flush=True)

        with lock:
            queued_files.discard(filename)


def run_cicflowmeter(job):
    filename = job["filename"]
    input_dir = job["input_dir"]
    output_dir = job["output_dir"]

    print(f"[PCAP-Processor] [INFO] Running CICFlowMeter for {filename}", flush=True)

    result = subprocess.run(
        [
            CICFLOWMETER_COMMAND,
            input_dir,
            output_dir,
        ],
        cwd=CICFLOWMETER_CWD,
        capture_output=True,
        text=True,
    )

    if result.stdout:
        print(result.stdout, flush=True)

    if result.stderr:
        print(result.stderr, flush=True)

    if result.returncode != 0:
        raise RuntimeError(f"[PCAP-Processor] [CIC] [ERROR] CICFlowMeter failed for {filename}")


def process_job(job):
    filename = job["filename"]
    input_dir = job["input_dir"]
    output_dir = job["output_dir"]
    pcap_path = job["pcap_path"]

    try:
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        os.makedirs(FAILED_DIR, exist_ok=True)
        os.makedirs(CSV_READY_DIR, exist_ok=True)

        if not os.path.exists(pcap_path):
            raise FileNotFoundError(f"[PCAP-Processor] [ERROR] PCAP not found in temp folder: {pcap_path}")

        run_cicflowmeter(job)

        moved_csv_count = move_generated_csvs(output_dir)

        processed_pcap_destination = unique_destination(PROCESSED_DIR, filename)
        shutil.move(pcap_path, processed_pcap_destination)

        with lock:
            processed_files.add(filename)

        print(f"[PCAP-Processor] [INFO] Processed PCAP: {filename}", flush=True)
        print(f"[PCAP-Processor] [INFO] Generated CSV files moved: {moved_csv_count}", flush=True)

    except Exception as e:
        print(f"[PCAP-Processor] [ERROR] Failed to process {filename}: {e}", flush=True)

        if os.path.exists(pcap_path):
            failed_destination = unique_destination(FAILED_DIR, filename)
            shutil.move(pcap_path, failed_destination)
            print(f"[PCAP-Processor] [INFO] Moved failed PCAP to: {failed_destination}", flush=True)

    finally:
        shutil.rmtree(input_dir, ignore_errors=True)
        shutil.rmtree(output_dir, ignore_errors=True)

        with lock:
            queued_files.discard(filename)

        print(f"[PCAP-Processor] [INFO] Removed temp input folder: {input_dir}", flush=True)
        print(f"[PCAP-Processor] [INFO] Removed temp output folder: {output_dir}", flush=True)


def cicflowmeter_worker():
    print("[PCAP-Processor] [INFO] Worker ready", flush=True)

    while True:
        job = pcap_queue.get()

        try:
            process_job(job)
        finally:
            pcap_queue.task_done()


class PcapHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            enqueue_pcap(event.src_path)

    def on_moved(self, event):
        if not event.is_directory:
            enqueue_pcap(event.dest_path)


if __name__ == "__main__":
    os.makedirs(WATCH_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    os.makedirs(FAILED_DIR, exist_ok=True)
    os.makedirs(CSV_READY_DIR, exist_ok=True)

    input_template_parent = os.path.dirname(PCAP_INPUT_TEMPLATE.format("dummy"))
    output_template_parent = os.path.dirname(PCAP_OUTPUT_TEMPLATE.format("dummy"))

    os.makedirs(input_template_parent, exist_ok=True)
    os.makedirs(output_template_parent, exist_ok=True)

    for file in os.listdir(PROCESSED_DIR):
        if file.endswith(".pcap"):
            processed_files.add(file)

    print(f"[PCAP-Processor] [INFO] Watching folder: {WATCH_DIR}", flush=True)

    threading.Thread(target=cicflowmeter_worker, daemon=True).start()

    for file in sorted(os.listdir(WATCH_DIR)):
        path = os.path.join(WATCH_DIR, file)

        if is_pcap(path):
            enqueue_pcap(path)

    observer = Observer()
    observer.schedule(PcapHandler(), WATCH_DIR, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        print("[PCAP-Processor] [SHUTDOWN] Stopping script...", flush=True)
        observer.stop()

    observer.join()