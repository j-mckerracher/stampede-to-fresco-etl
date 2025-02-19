import signal
import sys
from helpers.data_processor import DataProcessor
from helpers.utilities import get_base_dir


def signal_handler(signum, frame):
    print("\nReceived signal to shut down...")
    sys.exit(0)


def main():
    base_dir = get_base_dir()
    processor = DataProcessor(base_dir)

    # Set up signal handler
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    processor.start()


if __name__ == "__main__":
    main()
