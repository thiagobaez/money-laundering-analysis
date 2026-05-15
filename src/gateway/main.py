import os
import logging
import socket
import signal
import multiprocessing
import uuid
from common.middleware import MessageMiddlewareQueueRabbitMQ
from common.message_protocol import external, internal
from common.message_protocol.external import MsgType

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]


class MessageHandler:
    def __init__(self):
        self.client_id = str(uuid.uuid4())

    def serialize_tx(self, fields: list) -> bytes:
        return internal.serialize([self.client_id] + fields)

    def serialize_eof(self) -> bytes:
        return internal.serialize(internal.EOF_MESSAGE)

    def deserialize_result(self, message: bytes) -> tuple | None:
        fields = internal.deserialize(message)
        if not isinstance(fields, list) or len(fields) < 3:
            return None
        if fields[0] != self.client_id:
            return None
        return (fields[1], fields[2])


def handle_client_request(client_socket, message_handler):
    output_queue = MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
    try:
        while True:
            msg_type, payload = external.recv_msg(client_socket)
            if msg_type == MsgType.DATA:
                output_queue.send(
                    message_handler.serialize_tx(internal.deserialize(payload))
                )
                external.send_data(client_socket, b"")
            elif msg_type == MsgType.EOF:
                output_queue.send(message_handler.serialize_eof())
                external.send_eof(client_socket)
                return
    except socket.error as e:
        logging.error(f"Socket error in handle_client_request: {e}")
    finally:
        output_queue.close()


def handle_client_response(client_map):
    input_queue = MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)

    def _consume_result(message, ack, nack):
        for client_id, (handler, sock) in list(client_map.items()):
            result = handler.deserialize_result(message)
            if result is None:
                continue
            query_number, data = result
            try:
                external.send_data(sock, internal.serialize([query_number, data]))
                ack()
                return
            except socket.error as e:
                logging.error(f"Socket error sending result to client {client_id}: {e}")
                ack()
                del client_map[client_id]
                return
        nack()

    try:
        input_queue.start_consuming(_consume_result)
    except Exception as e:
        logging.error(f"Exception in handle_client_response: {e}")
        input_queue.stop_consuming()
    finally:
        input_queue.close()


def handle_sigterm(server_socket, client_map, sigterm_received):
    server_socket.shutdown(socket.SHUT_RDWR)
    for _, (_, sock) in list(client_map.items()):
        sock.shutdown(socket.SHUT_RDWR)
    sigterm_received.value = 1


def main():
    logging.basicConfig(level=logging.INFO)

    with multiprocessing.Manager() as manager:
        client_map = manager.dict()
        sigterm_received = manager.Value("c_short", 0)

        pool = multiprocessing.Pool(processes=os.process_cpu_count())
        pool.apply_async(handle_client_response, (client_map,))

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((SERVER_HOST, SERVER_PORT))
        server_socket.listen()

        signal.signal(
            signal.SIGTERM,
            lambda signum, frame: handle_sigterm(
                server_socket, client_map, sigterm_received
            ),
        )

        try:
            while True:
                try:
                    client_socket, _ = server_socket.accept()
                    handler = MessageHandler()
                    client_map[handler.client_id] = [handler, client_socket]
                    pool.apply_async(handle_client_request, (client_socket, handler))
                except socket.error as e:
                    if sigterm_received.value == 0:
                        logging.error(f"Socket error in accept loop: {e}")
                        return 1
                    else:
                        return 0
                except Exception as e:
                    logging.error(f"Unexpected error in accept loop: {e}")
                    return 2
        finally:
            pool.close()
            pool.join()


if __name__ == "__main__":
    main()
