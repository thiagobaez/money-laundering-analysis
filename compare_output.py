#!/usr/bin/env python3

import csv
import os
import sys
from typing import Callable

EXPECTED_BASE = "expected"


def _load_csv(path: str) -> list[list[str]]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.reader(f))


def _row_key_default(row: list[str]):
    return tuple(row)


def _row_key_q4(row: list[str]):
    if len(row) < 2:
        return tuple(row)
    origin = row[0]
    dest = row[1]
    intermediates = frozenset(row[2:])
    return (origin, dest, intermediates)


def _compare_query(
    actual_path: str,
    expected_path: str,
    query_num: int,
    row_key: Callable,
    has_header: bool,
) -> bool:
    if not os.path.exists(actual_path):
        print(f"  [MISSING] archivo actual no encontrado: {actual_path}")
        return False
    if not os.path.exists(expected_path):
        print(f"  [MISSING] archivo expected no encontrado: {expected_path}")
        return False

    actual_rows = _load_csv(actual_path)
    expected_rows = _load_csv(expected_path)

    if has_header:
        actual_header = actual_rows[0] if actual_rows else []
        expected_header = expected_rows[0] if expected_rows else []
        actual_rows = actual_rows[1:]
        expected_rows = expected_rows[1:]
        if actual_header != expected_header:
            print(
                f"  [HEADER MISMATCH] actual={actual_header} expected={expected_header}"
            )

    actual_keys = [row_key(r) for r in actual_rows if r]
    expected_keys = [row_key(r) for r in expected_rows if r]

    actual_counts: dict = {}
    for k in actual_keys:
        actual_counts[k] = actual_counts.get(k, 0) + 1

    expected_counts: dict = {}
    for k in expected_keys:
        expected_counts[k] = expected_counts.get(k, 0) + 1

    missing = {k: v for k, v in expected_counts.items() if actual_counts.get(k, 0) < v}
    extra = {k: v for k, v in actual_counts.items() if expected_counts.get(k, 0) < v}

    if not missing and not extra:
        print(f"  [OK] tx: {len(actual_keys)} filas coinciden")
        return True

    print(f"  [FAIL] tx:")
    if missing:
        print(f"    Faltan {sum(missing.values())} fila(s) en el output actual:")
        for k, count in list(missing.items())[:10]:
            print(f"      x{count}: {k}")
        if len(missing) > 10:
            print(f"      ... y {len(missing) - 10} mas")
    if extra:
        print(
            f"    Sobran {sum(extra.values())} fila(s) inesperadas en el output actual:"
        )
        for k, count in list(extra.items())[:10]:
            print(f"      x{count}: {k}")
        if len(extra) > 10:
            print(f"      ... y {len(extra) - 10} mas")
    return False


def _compare_count(actual_path: str, expected_path: str) -> bool:
    if not os.path.exists(actual_path):
        print(f"  [MISSING] count actual no encontrado: {actual_path}")
        return False
    if not os.path.exists(expected_path):
        print(f"  [SKIP] no hay count expected en: {expected_path}")
        return True

    actual_rows = _load_csv(actual_path)
    expected_rows = _load_csv(expected_path)

    try:
        actual_count = int(actual_rows[1][0])
        expected_count = int(expected_rows[1][0])
    except (IndexError, ValueError) as e:
        print(f"  [ERROR] No se pudo parsear el count: {e}")
        return False

    if actual_count == expected_count:
        print(f"  [OK] count: {actual_count}")
        return True
    else:
        print(f"  [FAIL] count: obtenido={actual_count}, esperado={expected_count}")
        return False


QUERY_CONFIG = {
    1: {"row_key": _row_key_default, "has_header": True},
    3: {"row_key": _row_key_default, "has_header": True},
    4: {"row_key": _row_key_q4, "has_header": False},
    5: {"row_key": _row_key_default, "has_header": True},
}


def _load_clients_from_compose() -> list[dict]:
    try:
        compose_file = open(".compose").read().strip()
    except FileNotFoundError:
        print("[ERROR] No se encontró el archivo .compose")
        sys.exit(1)

    try:
        import yaml
    except ImportError:
        print("[ERROR] PyYAML no está instalado. Instálalo con: pip install pyyaml")
        sys.exit(1)

    try:
        with open(compose_file) as f:
            compose = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"[ERROR] No se encontró el compose file: {compose_file}")
        sys.exit(1)

    services = compose.get("services", {})
    clients = []

    for service_name, service in services.items():
        env_list = service.get("environment", [])
        input_file = None
        output_file = None

        for var in env_list:
            if isinstance(var, str):
                if var.startswith("INPUT_FILE="):
                    input_file = var.split("=", 1)[1]
                elif var.startswith("OUTPUT_FILE="):
                    output_file = var.split("=", 1)[1]

        if input_file and output_file:
            dataset_name = os.path.basename(input_file)
            output_dir = os.path.dirname(output_file).lstrip("/")
            clients.append(
                {
                    "service": service_name,
                    "dataset": dataset_name,
                    "output_dir": output_dir,
                    "expected_dir": os.path.join(EXPECTED_BASE, dataset_name),
                }
            )

    if not clients:
        print("[ERROR] No se encontraron clientes en el compose file")
        sys.exit(1)

    return clients


def compare_query(query_num: int, actual_base: str, expected_base: str) -> bool:
    cfg = QUERY_CONFIG[query_num]
    actual_tx = os.path.join(actual_base, f"query{query_num}", "tx.csv")
    expected_tx = os.path.join(expected_base, f"query{query_num}_tx.csv")
    actual_count = os.path.join(actual_base, f"query{query_num}", "count.csv")
    expected_count = os.path.join(expected_base, f"query{query_num}_count.csv")

    print(f"\n  --- Query {query_num} ---")
    ok_count = _compare_count(actual_count, expected_count)
    ok_tx = _compare_query(
        actual_tx, expected_tx, query_num, cfg["row_key"], cfg["has_header"]
    )
    return ok_count and ok_tx


def compare_client(client: dict, queries: list[int]) -> bool:
    print(f"\n{'=' * 60}")
    print(f"Cliente: {client['service']}  |  Dataset: {client['dataset']}")
    print(f"  output:   {client['output_dir']}")
    print(f"  expected: {client['expected_dir']}")

    all_ok = True
    for q in queries:
        ok = compare_query(q, client["output_dir"], client["expected_dir"])
        all_ok = all_ok and ok

    status = "OK" if all_ok else "DIFERENCIAS"
    print(f"\n  Resultado {client['service']}: {status}")
    return all_ok


def main():
    if len(sys.argv) > 1:
        choice = sys.argv[1].strip().lower()
    else:
        print("Queries disponibles: 1, 3, 4, 5, all")
        choice = input("Que query queres comparar? [1/3/4/5/all]: ").strip().lower()

    if choice == "all":
        queries = list(QUERY_CONFIG.keys())
    elif choice in ("1", "3", "4", "5"):
        queries = [int(choice)]
    else:
        print(f"Opcion invalida: '{choice}'. Usa 1, 3, 4, 5 o all.")
        sys.exit(1)

    clients = _load_clients_from_compose()

    all_ok = True
    for client in clients:
        ok = compare_client(client, queries)
        all_ok = all_ok and ok

    print(f"\n{'=' * 60}")
    if all_ok:
        print("Resultado final: TODAS LAS QUERIES COINCIDEN")
        sys.exit(0)
    else:
        print("Resultado final: SE ENCONTRARON DIFERENCIAS")
        sys.exit(1)


if __name__ == "__main__":
    main()
