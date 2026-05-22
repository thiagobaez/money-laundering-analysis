import csv
import io
import logging
import socket
import os
import signal
import time
import multiprocessing
from gateway.message_handler import MessageHandler
from common.middleware import MessageMiddlewareQueueRabbitMQ
from common.middleware import MessageMiddlewareExchangeRabbitMQ
from common.message_protocol import internal, external

_QUERY_RESULT_TYPES = {
    1: external.MsgType.RESULT_QUERY1,
    3: external.MsgType.RESULT_QUERY3,
    4: external.MsgType.RESULT_QUERY4,
    5: external.MsgType.RESULT_QUERY5,
}

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_ROUTING_KEYS = os.environ["INPUT_ROUTING_KEYS"].split(",")
INPUT_EXCHANGE_NAME = os.environ["INPUT_EXCHANGE_NAME"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
NUM_EXPECTED_EOFS = int(os.environ["NUM_EXPECTED_EOFS"])
SEND_RATE_LIMIT = float(os.environ.get("SEND_RATE_LIMIT", "0"))


def handle_client_request(client_socket, msg_handler):
    input_queue = MessageMiddlewareExchangeRabbitMQ(
        MOM_HOST, INPUT_EXCHANGE_NAME, INPUT_ROUTING_KEYS
    )
    try:
        while True:
            message = external.recv_msg(client_socket)

            if message[0] == external.MsgType.DATA:
                csv_fields = next(csv.reader(io.StringIO(message[1].decode("utf-8"))))
                input_queue.send(msg_handler.serialize_tx([csv_fields]))
                external.send_msg(client_socket, external.MsgType.ACK)
                if SEND_RATE_LIMIT > 0:
                    time.sleep(SEND_RATE_LIMIT)

            if message[0] == external.MsgType.EOF:
                input_queue.send(msg_handler.serialize_eof())
                external.send_msg(client_socket, external.MsgType.ACK)
                logging.info(
                    "All data received for client_id=%s", msg_handler.client_id
                )
                return

    except socket.error:
        logging.error("The connection with the client was lost")
    except Exception as e:
        logging.error(e)
    finally:
        input_queue.close()


def handle_client_response(client_map, num_expected_eofs):
    output_queue = MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
    eof_counts = {}

    def _consume_result(message, ack, nack):
        client_id = None
        try:
            deserialized = internal.deserialize(message)
            if not isinstance(deserialized, list) or len(deserialized) == 0:
                ack()
                return
            client_id = deserialized[0]
            if client_id not in client_map:
                nack()
                return
            handler, client_socket = client_map[client_id]
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
                    del eof_counts[client_id]
            else:
                query_id, *data = result
                msg_type = _QUERY_RESULT_TYPES[query_id]
                external.send_data(
                    client_socket, ",".join(data).encode("utf-8"), msg_type
                )
            ack()
        except socket.error:
            logging.error("The connection with the client was lost")
            if client_id is not None and client_id in client_map:
                del client_map[client_id]
            ack()
        except Exception as e:
            logging.error(e)
            nack()
            output_queue.stop_consuming()

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
        sigterm_received = manager.Value("c_short", 0)
        with multiprocessing.Pool(processes=os.process_cpu_count()) as processes_pool:
            processes_pool.apply_async(
                handle_client_response, (client_map, NUM_EXPECTED_EOFS)
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
