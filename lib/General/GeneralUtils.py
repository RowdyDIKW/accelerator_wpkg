import time

def print_with_current_datetime(message):
    current_datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f"[{current_datetime}] {message}")

indent = "-      "