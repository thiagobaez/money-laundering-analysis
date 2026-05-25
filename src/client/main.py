import logging
import os
import socket
import csv
import gzip
import io

from common.message_protocol import external

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
INPUT_FILE = os.environ["INPUT_FILE"]
OUTPUT_FILE = os.environ["OUTPUT_FILE"]


class Client:
    def __init__(self):
        self._socket = None
        self._closed = False
        self._header = None

    def connect(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((SERVER_HOST, SERVER_PORT))

    @staticmethod
    def open_file(filepath):
        if filepath.endswith(".gz"):
            return gzip.open(filepath, "rt", encoding="utf-8")
        return open(filepath, "r", encoding="utf-8")

    def _send_file(self, filepath: str):
        with self.open_file(filepath) as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
            self._header = next(csv_reader)
            for row in csv_reader:
                output = io.StringIO()
                writer = csv.writer(output)
                writer.writerow(row)
                line = output.getvalue().strip()
                external.send_data(
                    self._socket, line.encode("utf-8"), external.MsgType.DATA
                )
        external.send_eof(self._socket)

    def _receive_results(self, output_path: str):
        output_dir = os.path.dirname(output_path)
        MSG_TYPE_TO_QUERY = {
            external.MsgType.RESULT_QUERY1: 1,
            external.MsgType.RESULT_QUERY3: 3,
            external.MsgType.RESULT_QUERY4: 4,
            external.MsgType.RESULT_QUERY5: 5,
        }
        file_handles = {}
        counts = {msg_type: 0 for msg_type in MSG_TYPE_TO_QUERY}
        try:
            while True:
                msg_type, payload = external.recv_msg(self._socket)
                if msg_type == external.MsgType.EOF:
                    logging.info(f"Received message of type {msg_type.name}")
                    break
                if msg_type in MSG_TYPE_TO_QUERY:
                    if msg_type not in file_handles:
                        query_num = MSG_TYPE_TO_QUERY[msg_type]
                        path = os.path.join(output_dir, f"query{query_num}", "tx.csv")
                        os.makedirs(
                            os.path.join(output_dir, f"query{query_num}"), exist_ok=True
                        )
                        file_handles[msg_type] = open(path, "w", encoding="utf-8")
                        if self._header:
                            file_handles[msg_type].write(",".join(self._header) + "\n")
                            file_handles[msg_type].flush()
                    file_handles[msg_type].write(payload.decode("utf-8") + "\n")
                    file_handles[msg_type].flush()
                    counts[msg_type] += 1
        finally:
            for fh in file_handles.values():
                fh.close()
            for msg_type, count in counts.items():
                query_num = MSG_TYPE_TO_QUERY[msg_type]
                query_dir = os.path.join(output_dir, f"query{query_num}")
                os.makedirs(query_dir, exist_ok=True)
                count_path = os.path.join(query_dir, "count.csv")
                with open(count_path, "w", encoding="utf-8") as f:
                    f.write("count\n")
                    f.write(str(count) + "\n")

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
