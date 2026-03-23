"""
Author: Mohamed Chaari
"""
import subprocess
import time

if __name__ == "__main__":
    processes = [
        subprocess.Popen(["python", "producers/crypto_producer.py"]),
        subprocess.Popen(["python", "producers/news_producer.py"])
    ]

    try:
        while True:
            time.sleep(1)
            for p in processes:
                if p.poll() is not None:
                    # Relaunch if process exits
                    if p.args[1] == "producers/crypto_producer.py":
                        processes[0] = subprocess.Popen(["python", "producers/crypto_producer.py"])
                    elif p.args[1] == "producers/news_producer.py":
                        processes[1] = subprocess.Popen(["python", "producers/news_producer.py"])
    except KeyboardInterrupt:
        for p in processes:
            p.terminate()
