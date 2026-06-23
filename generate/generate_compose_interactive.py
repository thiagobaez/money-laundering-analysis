#!/usr/bin/env python3
"""
Generador interactivo de docker-compose.
Para cada parametro muestra el valor por defecto entre corchetes;
presiona Enter para aceptarlo o escribe uno nuevo.
"""

import os
import sys
import yaml

from generate_compose_q1 import generate_compose_q1
from generate_compose_q3 import generate_compose_q3
from generate_compose_q4 import generate_compose_q4
from generate_compose_q5 import generate_compose as generate_compose_q5
from generate_compose_all import generate_compose_all

_DATASETS_DIR = os.path.join(os.path.dirname(__file__), "..", "datasets")


def _list_datasets() -> list:
    try:
        files = sorted(
            f
            for f in os.listdir(_DATASETS_DIR)
            if os.path.isfile(os.path.join(_DATASETS_DIR, f))
        )
        return files
    except FileNotFoundError:
        return []


def _ask(label: str, default):
    type_fn = type(default)
    try:
        raw = input(f"  {label:<28} [{default}]: ").strip()
    except (EOFError, KeyboardInterrupt):
        print()
        sys.exit(0)
    if raw == "":
        return default
    try:
        return type_fn(raw)
    except ValueError:
        print(f"    Valor invalido, se usa el default: {default}")
        return default


def _ask_bool(label: str, default: bool) -> bool:
    default_str = "s" if default else "n"
    try:
        raw = input(f"  {label:<28} [s/n, default={default_str}]: ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print()
        sys.exit(0)
    if raw == "":
        return default
    if raw in ("s", "si", "y", "yes"):
        return True
    if raw in ("n", "no"):
        return False
    print(f"    Valor invalido, se usa el default: {default_str}")
    return default


def _ask_file(label: str, default: str) -> str:
    datasets = _list_datasets()
    if datasets:
        print(f"  Archivos disponibles en datasets/:")
        for idx, name in enumerate(datasets, 1):
            print(f"    {idx}) {name}")
    try:
        raw = input(f"  {label:<28} [{default}]: ").strip()
    except (EOFError, KeyboardInterrupt):
        print()
        sys.exit(0)
    if raw == "":
        return default
    if raw.isdigit():
        idx = int(raw) - 1
        if 0 <= idx < len(datasets):
            return datasets[idx]
        print(f"    Numero invalido, se usa el default: {default}")
        return default
    return raw


def _ask_input_files(default: str = "HI-Medium_Trans.csv") -> list:
    n = _ask("n_clients", 1)
    files = []
    for i in range(n):
        f = _ask_file(f"input_file client {i}", default)
        files.append(f)
    return files


def _dump(compose: dict, output: str):
    with open(output, "w") as f:
        yaml.dump(
            compose,
            f,
            default_flow_style=False,
            sort_keys=False,
            Dumper=yaml.SafeDumper,
        )
    print(f"\nGenerado: {output}")


def _gen_q1():
    print("\n[ Query 1 — parametros ]")
    input_files = _ask_input_files("HI-Medium_Trans.csv")
    n_filter_usd = _ask("filter_usd   (workers)", 3)
    n_filter_amount = _ask("filter_amount (workers)", 3)
    batch_size = _ask("batch_size", 10000)
    output = _ask("output file", "docker-compose-q1.yaml")
    _dump(
        generate_compose_q1(input_files, n_filter_usd, n_filter_amount, batch_size),
        output,
    )


def _gen_q3():
    print("\n[ Query 3 — parametros ]")
    input_files = _ask_input_files("HI-Medium_Trans.csv")
    n_filter_usd = _ask("filter_usd   (workers)", 3)
    n_split_date = _ask("split_date   (workers)", 3)
    n_avg = _ask("avg          (workers)", 2)
    n_avg_joiner = _ask("avg_joiner   (workers)", 5)
    batch_size = _ask("batch_size", 1000)
    output = _ask("output file", "docker-compose-q3.yaml")
    _dump(
        generate_compose_q3(
            input_files, n_filter_usd, n_split_date, n_avg, n_avg_joiner, batch_size
        ),
        output,
    )


def _gen_q4():
    print("\n[ Query 4 — parametros ]")
    input_files = _ask_input_files("HI-Medium_Trans.csv")
    n_filter_usd = _ask("filter_usd   (workers)", 3)
    n_filter_date = _ask("filter_date  (workers)", 3)
    n_split = _ask("split        (workers)", 3)
    n_detect = _ask("og/dt/sg_det (workers)", 3)
    batch_size = _ask("batch_size", 20000)
    output = _ask("output file", "docker-compose-q4.yaml")
    _dump(
        generate_compose_q4(
            input_files,
            n_filter_usd,
            n_filter_date,
            n_split,
            n_detect,
            batch_size,
        ),
        output,
    )


def _gen_q5():
    print("\n[ Query 5 — parametros ]")
    input_files = _ask_input_files("HI-Medium_Trans.csv")
    n_filter_fmt = _ask("filter_fmt    (workers)", 7)
    n_converter = _ask("converter     (workers)", 3)
    n_filter_amount = _ask("filter_amount (workers)", 2)
    batch_size = _ask("batch_size", 10000)
    output = _ask("output file", "docker-compose-q5.yaml")
    _dump(
        generate_compose_q5(
            n_filter_fmt, n_converter, n_filter_amount, input_files, batch_size
        ),
        output,
    )


def _gen_all():
    print("\n[ Todas las queries — parametros ]")
    input_files = _ask_input_files("HI-Medium_Trans.csv")

    print("  -- filter_usd compartido (Q1/Q3/Q4) --")
    n_filter_usd = _ask("  filter_usd   (workers)", 7)
    filter_usd_batch_size = _ask("  batch_size", 10000)

    print("  -- Q1 --")
    q1_n_filter_amount = _ask("  filter_amount (workers)", 3)
    q1_batch_size = _ask("  batch_size", 10000)

    print("  -- Q3 --")
    q3_n_split_date = _ask("  split_date   (workers)", 3)
    q3_n_avg = _ask("  avg          (workers)", 2)
    q3_n_avg_joiner = _ask("  avg_joiner   (workers)", 5)
    q3_batch_size = _ask("  batch_size", 10000)

    print("  -- Q4 --")
    q4_n_filter_date = _ask("  filter_date  (workers)", 3)
    q4_n_split = _ask("  split        (workers)", 3)
    q4_n_detect = _ask("  og/dt/sg_det (workers)", 3)
    q4_batch_size = _ask("  batch_size", 20000)

    print("  -- Q5 --")
    q5_n_filter_fmt = _ask("  filter_fmt    (workers)", 7)
    q5_n_converter = _ask("  converter     (workers)", 3)
    q5_n_filter_amount = _ask("  filter_amount (workers)", 2)
    q5_batch_size = _ask("  batch_size", 10000)

    print("  -- Chaos Monkey --")
    use_chaos = _ask_bool("  agregar chaos monkey?", False)
    chaos_kill_interval = 30
    if use_chaos:
        chaos_kill_interval = _ask("  kill_interval (segundos)", 30)

    print("  -- Watchdog --")
    use_watchdog = _ask_bool("  agregar watchdog?", False)
    watchdog_timeout = 30
    if use_watchdog:
        watchdog_timeout = _ask("  heartbeat timeout (segundos)", 30)

    output = _ask("output file", "docker-compose-all.yaml")

    _dump(
        generate_compose_all(
            input_files=input_files,
            n_filter_usd=n_filter_usd,
            filter_usd_batch_size=filter_usd_batch_size,
            q1_n_filter_amount=q1_n_filter_amount,
            q1_batch_size=q1_batch_size,
            q3_n_split_date=q3_n_split_date,
            q3_n_avg=q3_n_avg,
            q3_n_avg_joiner=q3_n_avg_joiner,
            q3_batch_size=q3_batch_size,
            q4_n_filter_date=q4_n_filter_date,
            q4_n_split=q4_n_split,
            q4_n_detect=q4_n_detect,
            q4_batch_size=q4_batch_size,
            q5_n_filter_fmt=q5_n_filter_fmt,
            q5_n_converter=q5_n_converter,
            q5_n_filter_amount=q5_n_filter_amount,
            q5_batch_size=q5_batch_size,
            chaos_monkey=use_chaos,
            chaos_kill_interval=chaos_kill_interval,
            watchdog=use_watchdog,
            watchdog_timeout=watchdog_timeout,
        ),
        output,
    )


_GENERATORS = {
    "1": _gen_q1,
    "3": _gen_q3,
    "4": _gen_q4,
    "5": _gen_q5,
    "all": _gen_all,
}


def main():
    print("=== Generador de docker-compose ===")
    print("Opciones: 1, 3, 4, 5, all")
    try:
        choice = input("Que compose queres generar? [1/3/4/5/all]: ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print()
        sys.exit(0)

    if choice not in _GENERATORS:
        print(f"Opcion invalida: '{choice}'")
        sys.exit(1)

    _GENERATORS[choice]()


if __name__ == "__main__":
    main()
