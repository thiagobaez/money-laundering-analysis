import os
import socket
from unittest.mock import MagicMock

os.environ.setdefault("SERVER_HOST", "localhost")
os.environ.setdefault("SERVER_PORT", "5678")
os.environ.setdefault("INPUT_FILE", "/tmp/input.csv")
os.environ.setdefault("OUTPUT_FILE", "/tmp/output.txt")

from client.main import Client
from common.message_protocol.external import MsgType


class TestSendFile:
    def test_sends_one_data_message_per_row_and_eof(self, mocker, tmp_path):
        mock_send_data = mocker.patch("client.main.external.send_data")
        mock_send_eof = mocker.patch("client.main.external.send_eof")

        client = Client()
        client._socket = MagicMock()

        csv_file = tmp_path / "input.csv"
        csv_file.write_text(
            "h0,h1,h2,h3,h4,h5,h6,h7,h8,h9\n"
            "a,b,c,d,e,f,g,h,i,j\n"
            "1,2,3,4,5,6,7,8,9,0\n"
        )

        client._send_file(str(csv_file))

        assert mock_send_data.call_count == 2
        mock_send_eof.assert_called_once_with(client._socket)

    def test_header_row_is_skipped(self, mocker, tmp_path):
        mock_send_data = mocker.patch("client.main.external.send_data")
        mocker.patch("client.main.external.send_eof")

        client = Client()
        client._socket = MagicMock()

        csv_file = tmp_path / "input.csv"
        csv_file.write_text("header_only\n")

        client._send_file(str(csv_file))

        mock_send_data.assert_not_called()

    def test_column_selection_drops_unselected_fields(self, mocker, tmp_path):
        mock_send_data = mocker.patch("client.main.external.send_data")
        mocker.patch("client.main.external.send_eof")

        client = Client()
        client._socket = MagicMock()

        # SELECTED_TX_ROWS = [0,1,2,3,4,5,6,9] — indices 7 and 8 should be dropped
        csv_file = tmp_path / "input.csv"
        csv_file.write_text("h0,h1,h2,h3,h4,h5,h6,h7,h8,h9\na,b,c,d,e,f,g,DROP,DROP,keep\n")

        client._send_file(str(csv_file))

        sent_payload = mock_send_data.call_args[0][1]
        assert b"DROP" not in sent_payload
        assert b"keep" in sent_payload

    def test_sends_with_data_msg_type(self, mocker, tmp_path):
        mock_send_data = mocker.patch("client.main.external.send_data")
        mocker.patch("client.main.external.send_eof")

        client = Client()
        client._socket = MagicMock()

        csv_file = tmp_path / "input.csv"
        csv_file.write_text("header\nfield\n")

        client._send_file(str(csv_file))

        _, _, msg_type = mock_send_data.call_args[0]
        assert msg_type == MsgType.DATA


class TestReceiveResults:
    def test_writes_result_messages_to_file(self, mocker, tmp_path):
        mocker.patch("client.main.external.recv_msg").side_effect = [
            (MsgType.RESULT_QUERY1, b"line1"),
            (MsgType.RESULT_QUERY3, b"line2"),
            (MsgType.EOF, None),
        ]

        client = Client()
        client._socket = MagicMock()

        out = tmp_path / "output.txt"
        client._receive_results(str(out))

        content = out.read_text()
        assert "line1\n" in content
        assert "line2\n" in content

    def test_stops_on_eof(self, mocker, tmp_path):
        recv_mock = mocker.patch("client.main.external.recv_msg")
        recv_mock.side_effect = [(MsgType.EOF, None)]

        client = Client()
        client._socket = MagicMock()

        out = tmp_path / "output.txt"
        client._receive_results(str(out))

        assert recv_mock.call_count == 1
        assert out.read_text() == ""

    def test_ignores_non_result_messages(self, mocker, tmp_path):
        mocker.patch("client.main.external.recv_msg").side_effect = [
            (MsgType.ACK, None),
            (MsgType.RESULT_QUERY5, b"real"),
            (MsgType.EOF, None),
        ]

        client = Client()
        client._socket = MagicMock()

        out = tmp_path / "output.txt"
        client._receive_results(str(out))

        assert out.read_text() == "real\n"


class TestDisconnect:
    def test_shuts_down_and_closes_socket(self):
        client = Client()
        client._socket = MagicMock()

        client.disconnect()

        client._socket.shutdown.assert_called_once_with(socket.SHUT_RDWR)
        client._socket.close.assert_called_once()
        assert client.closed

    def test_is_idempotent(self):
        client = Client()
        client._socket = MagicMock()

        client.disconnect()
        client.disconnect()

        client._socket.close.assert_called_once()

    def test_oserror_on_shutdown_is_handled(self):
        client = Client()
        client._socket = MagicMock()
        client._socket.shutdown.side_effect = OSError()

        client.disconnect()

        client._socket.close.assert_called_once()
        assert client.closed
