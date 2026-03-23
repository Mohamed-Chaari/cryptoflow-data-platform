import subprocess
import time

if __name__ == "__main__":
    processes = [
        subprocess.Popen(["python", "jobs/price_aggregator.py"]),
        subprocess.Popen(["python", "jobs/alert_detector.py"]),
        subprocess.Popen(["python", "jobs/news_processor.py"])
    ]

    try:
        while True:
            time.sleep(1)
            for i, p in enumerate(processes):
                if p.poll() is not None:
                    print(f"Process {i} exited with code {p.returncode}. Restarting...")
                    script = p.args[1]
                    processes[i] = subprocess.Popen(["python", script])
    except KeyboardInterrupt:
        for p in processes:
            p.terminate()
