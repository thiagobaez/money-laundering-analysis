import json
import logging
import socket
import os
import signal
import multiprocessing
from gateway.message_handler import MessageHandler
from common.middleware import MessageMiddlewareQueueRabbitMQ
from common.middleware import MessageMiddlewareExchangeRabbitMQ
from common.message_protocol import internal, external

_QUERY_RESULT_TYPES = {
    1: external.MsgType.RESULT_BATCH_QUERY1,
    3: external.MsgType.RESULT_BATCH_QUERY3,
    4: external.MsgType.RESULT_BATCH_QUERY4,
    5: external.MsgType.RESULT_BATCH_QUERY5,
}

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE_NAME = os.environ.get("INPUT_QUEUE")
INPUT_ROUTING_KEYS = (
    os.environ.get("INPUT_ROUTING_KEYS", "").split(",")
    if os.environ.get("INPUT_ROUTING_KEYS")
    else None
)
INPUT_EXCHANGE_NAME = os.environ.get("INPUT_EXCHANGE_NAME")
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
NUM_EXPECTED_EOFS = int(os.environ["NUM_EXPECTED_EOFS"])
AVG_JOINER_AMOUNT = int(os.environ.get("AVG_JOINER_AMOUNT", "0"))
SG_DETECT_AMOUNT = int(os.environ.get("SG_DETECT_AMOUNT", "0"))
DATA_DIR = os.environ.get("DATA_DIR", "/data")

_ACTIVE_CLIENTS_FILE = "active_clients.json"


def _active_clients_path():
    return os.path.join(DATA_DIR, _ACTIVE_CLIENTS_FILE)


def _save_active_clients(client_ids):
    path = _active_clients_path()
    if not client_ids:
        if os.path.exists(path):
            os.remove(path)
        return
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(list(client_ids), f)
    os.replace(tmp_path, path)


def _load_active_clients():
    path = _active_clients_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            client_ids = json.load(f)
        return set(client_ids)
    except (OSError, json.JSONDecodeError):
        return set()


def _send_cancellation_eof(client_id):
    outputs = []
    try:
        if INPUT_QUEUE_NAME:
            outputs.append(MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE_NAME))
        if INPUT_EXCHANGE_NAME and INPUT_ROUTING_KEYS:
            outputs.append(
                MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST, INPUT_EXCHANGE_NAME, INPUT_ROUTING_KEYS
                )
            )
        cancel_eof = internal.serialize([client_id])
        for output in outputs:
            output.send(cancel_eof)
    except Exception as e:
        logging.error(
            "Failed to send cancellation EOF for client_id=%s: %s", client_id, e
        )
    finally:
        for output in outputs:
            try:
                output.close()
            except Exception as e:
                logging.error(e)


def handle_client_request(client_socket, msg_handler):
    outputs = []
    if INPUT_QUEUE_NAME:
        outputs.append(MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE_NAME))
    if INPUT_EXCHANGE_NAME and INPUT_ROUTING_KEYS:
        outputs.append(
            MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, INPUT_EXCHANGE_NAME, INPUT_ROUTING_KEYS
            )
        )
    try:
        while True:
            message = external.recv_msg(client_socket)
            if message[0] == external.MsgType.DATA_BATCH:
                batch = external.recv_batch(message[1])
                serialized = internal.serialize([msg_handler.client_id, batch])
                for output in outputs:
                    output.send(serialized)
            if message[0] == external.MsgType.EOF:
                eof = msg_handler.serialize_eof()
                for output in outputs:
                    output.send(eof)
                logging.info(
                    "All data received for client_id=%s", msg_handler.client_id
                )
                return
    except socket.error:
        logging.error("The connection with the client was lost")
        _send_cancellation_eof(msg_handler.client_id)
    except Exception as e:
        logging.error(e)
    finally:
        for output in outputs:
            output.close()


def handle_client_response(client_map, active_clients, num_expected_eofs):
    output_queue = MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
    eof_counts = {}
    q3_eof_seen = {}
    q4_eof_seen = {}

    def _remove_active_client(client_id):
        if client_id in active_clients:
            del active_clients[client_id]
            _save_active_clients(set(active_clients.keys()))

    def _consume_result(message, ack, nack):
        client_id = None
        try:
            deserialized = internal.deserialize(message)
            if not isinstance(deserialized, list) or len(deserialized) == 0:
                ack()
                return

            client_id = deserialized[0]
            if client_id not in client_map:
                ack()
                return

            handler, client_socket = client_map[client_id]

            if (
                AVG_JOINER_AMOUNT > 0
                and len(deserialized) == 4
                and deserialized[1] == 3
                and deserialized[2] == "EOF"
            ):
                worker_id = deserialized[3]
                q3_eof_seen.setdefault(client_id, set()).add(worker_id)

                if len(q3_eof_seen[client_id]) < AVG_JOINER_AMOUNT:
                    ack()
                    return

                del q3_eof_seen[client_id]

                eof_counts[client_id] = eof_counts.get(client_id, 0) + 1

                if eof_counts[client_id] >= num_expected_eofs:
                    external.send_msg(client_socket, external.MsgType.EOF)
                    del client_map[client_id]
                    _remove_active_client(client_id)
                    del eof_counts[client_id]

                ack()
                return

            if (
                SG_DETECT_AMOUNT > 0
                and len(deserialized) == 4
                and deserialized[1] == 4
                and deserialized[2] == "EOF"
            ):
                worker_id = deserialized[3]
                q4_eof_seen.setdefault(client_id, set()).add(worker_id)

                if len(q4_eof_seen[client_id]) < SG_DETECT_AMOUNT:
                    ack()
                    return

                del q4_eof_seen[client_id]

                eof_counts[client_id] = eof_counts.get(client_id, 0) + 1

                if eof_counts[client_id] >= num_expected_eofs:
                    external.send_msg(client_socket, external.MsgType.EOF)
                    del client_map[client_id]
                    _remove_active_client(client_id)
                    del eof_counts[client_id]

                ack()
                return

            result = handler.deserialize_result(message)

            if result is None:
                eof_counts[client_id] = eof_counts.get(client_id, 0) + 1

                logging.info(
                    "EOF %d/%d received for client_id=%s",
                    eof_counts[client_id],
                    num_expected_eofs,
                    client_id,
                )

                if eof_counts[client_id] >= num_expected_eofs:
                    external.send_msg(client_socket, external.MsgType.EOF)
                    del client_map[client_id]
                    _remove_active_client(client_id)
                    del eof_counts[client_id]
            else:
                query_id, rows = result
                msg_type = _QUERY_RESULT_TYPES[query_id]
                external.send_batch(client_socket, rows, msg_type)

            ack()

        except socket.error:
            logging.error("The connection with the client was lost")
            if client_id is not None and client_id in client_map:
                del client_map[client_id]
                _remove_active_client(client_id)
                _send_cancellation_eof(client_id)
            ack()
        except Exception as e:
            logging.error(e)
            ack()

    output_queue.start_consuming(_consume_result)
    output_queue.close()


def handle_sigterm(server_socket, client_map, sigterm_received):
    server_socket.shutdown(socket.SHUT_RDWR)
    for _, client_socket in client_map.values():
        client_socket.shutdown(socket.SHUT_RDWR)
    sigterm_received.value = 1


def main():
    logging.basicConfig(level=logging.INFO)

    with multiprocessing.Manager() as manager:
        client_map = manager.dict()
        active_clients = manager.dict()
        sigterm_received = manager.Value("c_short", 0)

        stale_client_ids = _load_active_clients()
        for client_id in stale_client_ids:
            _send_cancellation_eof(client_id)
            logging.info(
                "Cancelled orphaned pipeline state for client_id=%s", client_id
            )
        path = _active_clients_path()
        if os.path.exists(path):
            os.remove(path)

        with multiprocessing.Pool(processes=os.process_cpu_count()) as processes_pool:
            processes_pool.apply_async(
                handle_client_response, (client_map, active_clients, NUM_EXPECTED_EOFS)
            )

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                logging.info("Listening to connections")
                server_socket.bind((SERVER_HOST, SERVER_PORT))
                server_socket.listen()
                signal.signal(
                    signal.SIGTERM,
                    lambda _signum, _frame: handle_sigterm(
                        server_socket, client_map, sigterm_received
                    ),
                )
                while True:
                    try:
                        client_socket, _ = server_socket.accept()
                        logging.info("A new client has connected")
                        msg_handler = MessageHandler()
                        client_map[msg_handler.client_id] = [msg_handler, client_socket]
                        active_clients[msg_handler.client_id] = True
                        _save_active_clients(set(active_clients.keys()))
                        processes_pool.apply_async(
                            handle_client_request,
                            (client_socket, msg_handler),
                        )
                    except socket.error:
                        if sigterm_received.value == 0:
                            logging.error("The connection with the client was lost")
                            return 1
                        else:
                            return 0
                    except Exception as e:
                        logging.error(e)
                        return 2
    return 0


if __name__ == "__main__":
    main()
