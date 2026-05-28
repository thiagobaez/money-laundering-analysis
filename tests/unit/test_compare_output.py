import csv
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from compare_output import _compare_query, _row_key_default, _row_key_q4


def _write_csv(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows(rows)


class TestRowKeyQ4:
    def test_origin_is_col0_dest_is_col1(self):
        # format: [origin, dest, c1, c2, ...]
        row = ["A", "B", "X", "Y"]
        key = _row_key_q4(row)
        assert key[0] == "A"
        assert key[1] == "B"

    def test_intermediates_are_a_set(self):
        row = ["A", "B", "X", "Y"]
        key = _row_key_q4(row)
        assert key[2] == frozenset({"X", "Y"})

    def test_intermediate_order_does_not_matter(self):
        key1 = _row_key_q4(["A", "B", "X", "Y"])
        key2 = _row_key_q4(["A", "B", "Y", "X"])
        assert key1 == key2

    def test_two_column_row_no_intermediates(self):
        row = ["A", "B"]
        key = _row_key_q4(row)
        assert key[0] == "A"
        assert key[1] == "B"
        assert key[2] == frozenset()

    def test_single_column_row(self):
        row = ["A"]
        assert _row_key_q4(row) == ("A",)


class TestCompareQuery:
    def test_identical_files_match(self, tmp_path):
        rows = [["h1", "h2"], ["a", "1"], ["b", "2"]]
        actual = str(tmp_path / "actual" / "tx.csv")
        expected = str(tmp_path / "expected" / "tx.csv")
        _write_csv(actual, rows)
        _write_csv(expected, rows)

        ok = _compare_query(actual, expected, 1, _row_key_default, has_header=True)
        assert ok

    def test_row_order_does_not_matter(self, tmp_path):
        header = [["h1", "h2"]]
        actual_rows = header + [["a", "1"], ["b", "2"]]
        expected_rows = header + [["b", "2"], ["a", "1"]]
        actual = str(tmp_path / "actual" / "tx.csv")
        expected = str(tmp_path / "expected" / "tx.csv")
        _write_csv(actual, actual_rows)
        _write_csv(expected, expected_rows)

        ok = _compare_query(actual, expected, 1, _row_key_default, has_header=True)
        assert ok

    def test_missing_row_detected(self, tmp_path):
        header = [["h1"]]
        actual = str(tmp_path / "actual" / "tx.csv")
        expected = str(tmp_path / "expected" / "tx.csv")
        _write_csv(actual, header + [["a"]])
        _write_csv(expected, header + [["a"], ["b"]])

        ok = _compare_query(actual, expected, 1, _row_key_default, has_header=True)
        assert not ok

    def test_extra_row_detected(self, tmp_path):
        header = [["h1"]]
        actual = str(tmp_path / "actual" / "tx.csv")
        expected = str(tmp_path / "expected" / "tx.csv")
        _write_csv(actual, header + [["a"], ["b"]])
        _write_csv(expected, header + [["a"]])

        ok = _compare_query(actual, expected, 1, _row_key_default, has_header=True)
        assert not ok

    def test_q4_intermediate_order_insensitive(self, tmp_path):
        # format: [origin, dest, c1, c2, c3]
        actual = str(tmp_path / "actual" / "tx.csv")
        expected = str(tmp_path / "expected" / "tx.csv")
        _write_csv(actual, [["orig", "dest", "X", "Y", "Z"]])
        _write_csv(expected, [["orig", "dest", "Z", "X", "Y"]])

        ok = _compare_query(actual, expected, 4, _row_key_q4, has_header=False)
        assert ok

    def test_q4_different_dest_fails(self, tmp_path):
        actual = str(tmp_path / "actual" / "tx.csv")
        expected = str(tmp_path / "expected" / "tx.csv")
        _write_csv(actual, [["orig", "dest_A", "X"]])
        _write_csv(expected, [["orig", "dest_B", "X"]])

        ok = _compare_query(actual, expected, 4, _row_key_q4, has_header=False)
        assert not ok

    def test_q4_different_intermediates_fails(self, tmp_path):
        actual = str(tmp_path / "actual" / "tx.csv")
        expected = str(tmp_path / "expected" / "tx.csv")
        _write_csv(actual, [["orig", "dest", "X"]])
        _write_csv(expected, [["orig", "dest", "Y"]])

        ok = _compare_query(actual, expected, 4, _row_key_q4, has_header=False)
        assert not ok

    def test_missing_actual_file(self, tmp_path):
        expected = str(tmp_path / "expected" / "tx.csv")
        _write_csv(expected, [["a"]])

        ok = _compare_query(
            str(tmp_path / "nonexistent.csv"),
            expected,
            1,
            _row_key_default,
            has_header=False,
        )
        assert not ok
