import logging
import os
import socket
import csv

from common.message_protocol import external

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
TXS_FILE = os.environ["TXS_FILE"]
ACC_FILE = os.environ["ACC_FILE"]
OUTPUT_FILE = os.environ["OUTPUT_FILE"]
SELECTED_TX_ROWS = [0, 1, 2, 3, 4, 5, 6, 9]
SELECTED_ACC_ROWS = [0, 1]


class Client:
    def __init__(self):
        self._socket = None
        self._closed = False

    def connect(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((SERVER_HOST, SERVER_PORT))

    def _send_file(self, filepath: str, is_txs: bool):
        with open(filepath, newline="\n") as csvfile:
            msg_type = external.MsgType.TX if is_txs else external.MsgType.ACC
            reader = csv.reader(csvfile, delimiter=",", quotechar='"')
            next(reader)
            cols = SELECTED_TX_ROWS if is_txs else SELECTED_ACC_ROWS
            for row in reader:
                selected_row = [row[i] for i in cols if i < len(row)]
                line = ",".join(selected_row)
                if line:
                    external.send_data(self._socket, line.encode("utf-8"), msg_type)
        external.send_eof(self._socket)

    def _receive_results(self, output_path: str):
        with open(output_path, "w", encoding="utf-8") as out:
            while True:
                msg_type, payload = external.recv_msg(self._socket)
                if msg_type == external.MsgType.EOF:
                    break
                if msg_type == external.MsgType.RESULT:
                    out.write(payload.decode("utf-8") + "\n")

    def run(self):
        self._send_file(TXS_FILE, is_txs=True)
        self._send_file(ACC_FILE, is_txs=False)
        self._receive_results(OUTPUT_FILE)

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
        client.run()
    except socket.error:
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
