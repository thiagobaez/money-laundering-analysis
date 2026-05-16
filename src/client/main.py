import logging
import os
import socket
import csv
import io

from common.message_protocol import external

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
INPUT_FILE = os.environ["INPUT_FILE"]
OUTPUT_FILE = os.environ["OUTPUT_FILE"]
SELECTED_TX_ROWS = [0, 1, 2, 3, 4, 5, 6, 9]


class Client:
    def __init__(self):
        self._socket = None
        self._closed = False

    def connect(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((SERVER_HOST, SERVER_PORT))

    def _send_file(self, filepath: str):
        with open(filepath, newline="\n") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
            next(csv_reader)
            for row in csv_reader:
                selected_row = [row[i] for i in SELECTED_TX_ROWS if i < len(row)]
                buf = io.StringIO()
                csv.writer(buf).writerow(selected_row)
                line = buf.getvalue().strip()
                if line:
                    external.send_data(self._socket, line.encode("utf-8"), external.MsgType.DATA)
        external.send_eof(self._socket)

    def _receive_results(self, output_path: str):
        with open(output_path, "w", encoding="utf-8") as out:
            while True:
                msg_type, payload = external.recv_msg(self._socket)
                logging.info(f"Received message of type {msg_type}")
                if msg_type == external.MsgType.EOF:
                    break
                if msg_type == external.MsgType.RESULT:
                    out.write(payload.decode("utf-8") + "\n")


    def disconnect(self):
        if self._socket and not self._closed:
            try:
                self._socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self._socket.close()
            self._closed = True

    @property
    def closed(self):
        return self._closed


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    client = Client()

    try:
        client.connect()
        client._send_file(INPUT_FILE)
        client._receive_results(OUTPUT_FILE)
    except ConnectionError:
        if not client.closed:
            logging.error("The connection with the server was lost")
            return 1
    except Exception as e:
        logging.error(e)
        return 2
    finally:
        if not client.closed:
            client.disconnect()

    return 0


if __name__ == "__main__":
    main()
