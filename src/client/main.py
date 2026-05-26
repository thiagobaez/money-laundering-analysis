import logging
import os
import socket
import csv
import gzip

from common.message_protocol import external

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
INPUT_FILE = os.environ["INPUT_FILE"]
OUTPUT_FILE = os.environ["OUTPUT_FILE"]
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))


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

    def _send_batch(self, batch):
        external.send_batch(self._socket, batch, external.MsgType.DATA_BATCH)

    def _send_file(self, filepath: str):
        with self.open_file(filepath) as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
            self._header = next(csv_reader)
            batch = []
            for row in csv_reader:
                batch.append(row)
                if len(batch) >= BATCH_SIZE:
                    self._send_batch(batch)
                    batch = []
            if batch:
                self._send_batch(batch)
        external.send_eof(self._socket)

    def _receive_results(self, output_path: str):
        output_dir = os.path.dirname(output_path)
        MSG_TYPE_TO_QUERY = {
            external.MsgType.RESULT_BATCH_QUERY1: 1,
            external.MsgType.RESULT_BATCH_QUERY3: 3,
            external.MsgType.RESULT_BATCH_QUERY4: 4,
            external.MsgType.RESULT_BATCH_QUERY5: 5,
        }
        file_handles = {}
        counts = {msg_type: 0 for msg_type in MSG_TYPE_TO_QUERY}
        try:
            while True:
                msg_type, payload = external.recv_msg(self._socket)
                if msg_type == external.MsgType.EOF:
                    logging.info("Received EOF from server")
                    break
                if msg_type in MSG_TYPE_TO_QUERY:
                    if msg_type not in file_handles:
                        query_num = MSG_TYPE_TO_QUERY[msg_type]
                        query_dir = os.path.join(output_dir, f"query{query_num}")
                        os.makedirs(query_dir, exist_ok=True)
                        tx_path = os.path.join(query_dir, "tx.csv")
                        file_handles[msg_type] = open(
                            tx_path, "w", encoding="utf-8", newline=""
                        )
                        writer = csv.writer(file_handles[msg_type])
                        if self._header:
                            writer.writerow(self._header)

                    rows = external.recv_batch(payload)
                    writer = csv.writer(file_handles[msg_type])
                    writer.writerows(rows)
                    file_handles[msg_type].flush()
                    counts[msg_type] += len(rows)

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
                    f.write(f"{count}\n")

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
    logging.basicConfig(level=logging.ERROR)
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
