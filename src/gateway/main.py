import logging
import socket
import multiprocessing
from gateway.message_handler import MessageHandler
# from common import middleware
from common import message_protocol


SERVER_HOST = "localhost"
SERVER_PORT = 8080
"""
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
"""


def handle_client_request(client_socket, msg_handler):
    # input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
    try:
        eof_count = 0
        while True:
            message = message_protocol.external.recv_msg(client_socket)

            if message[0] == message_protocol.external.MsgType.TX:
                # serialized_message = msg_handler.serialize_tx_message(message[1])
                # input_queue.send(serialized_message)
                print(f"[TX] client_id={msg_handler.client_id} | {message[1].decode('utf-8')}")
                message_protocol.external.send_msg(
                    client_socket, message_protocol.external.MsgType.ACK
                )

            elif message[0] == message_protocol.external.MsgType.ACC:
                # serialized_message = msg_handler.serialize_acc_message(message[1])
                # input_queue.send(serialized_message)
                print(f"[ACC] client_id={msg_handler.client_id} | {message[1].decode('utf-8')}")
                message_protocol.external.send_msg(
                    client_socket, message_protocol.external.MsgType.ACK
                )

            elif message[0] == message_protocol.external.MsgType.EOF:
                # serialized_message = msg_handler.serialize_eof_message()
                # input_queue.send(serialized_message)
                message_protocol.external.send_msg(
                    client_socket, message_protocol.external.MsgType.ACK
                )
                eof_count += 1
                if eof_count == 2:
                    logging.info("All data received for client_id=%s", msg_handler.client_id)
                    confirmation = (
                        f"Confirmacion: todos los datos recibidos (client_id={msg_handler.client_id})"
                    ).encode("utf-8")
                    message_protocol.external.send_data(
                        client_socket,
                        confirmation,
                        message_protocol.external.MsgType.RESULT,
                    )
                    message_protocol.external.send_eof(client_socket)
                    return

    except socket.error:
        logging.error("The connection with the client was lost")
    except Exception as e:
        logging.error(e)
    # finally:
    #     input_queue.close()


"""
def handle_client_response(client_list):
    output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)

    def _consume_result(message, ack, nack):
        client_index = 0
        try:
            for [message_handler_instance, client_socket] in client_list:
                deserialized_message = (
                    message_handler_instance.deserialize_result_message(message)
                )

                if not deserialized_message:
                    client_index += 1
                    continue

                message_protocol.external.send_msg(
                    client_socket,
                    message_protocol.external.MsgType.FRUIT_TOP,
                    deserialized_message,
                )
                message_protocol.external.recv_msg(client_socket)
                break
            client_list.pop(client_index)
            ack()
        except socket.error:
            logging.error("The connection with the server was lost")
            client_list.pop(client_index)
            ack()
        except Exception as e:
            logging.error(e)
            nack()
            output_queue.stop_consuming()

    output_queue.start_consuming(_consume_result)
    output_queue.close()
"""


def main():
    logging.basicConfig(level=logging.INFO)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((SERVER_HOST, SERVER_PORT))
        server_socket.listen()
        logging.info("Listening on %s:%d", SERVER_HOST, SERVER_PORT)

        while True:
            try:
                client_socket, addr = server_socket.accept()
                logging.info("New client connected from %s", str(addr))
                msg_handler = MessageHandler()
                p = multiprocessing.Process(
                    target=handle_client_request,
                    args=(client_socket, msg_handler),
                )
                p.start()
                client_socket.close()
            except KeyboardInterrupt:
                logging.info("Server stopped")
                return 0
            except socket.error as e:
                logging.error("Socket error: %s", str(e))
                return 1
            except Exception as e:
                logging.error(str(e))
                return 2

    return 0


if __name__ == "__main__":
    main()
