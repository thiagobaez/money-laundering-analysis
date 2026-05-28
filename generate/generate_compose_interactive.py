#!/usr/bin/env python3
"""
Generador interactivo de docker-compose.
Para cada parametro muestra el valor por defecto entre corchetes;
presiona Enter para aceptarlo o escribe uno nuevo.
"""
import sys
import yaml

from generate_compose_q1 import generate_compose_q1
from generate_compose_q3 import generate_compose_q3
from generate_compose_q4 import generate_compose_q4
from generate_compose_q5 import generate_compose as generate_compose_q5
from generate_compose_all import generate_compose_all


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


def _dump(compose: dict, output: str):
    with open(output, "w") as f:
        yaml.dump(compose, f, default_flow_style=False, sort_keys=False, Dumper=yaml.SafeDumper)
    print(f"\nGenerado: {output}")


# ---------------------------------------------------------------------------

def _gen_q1():
    print("\n[ Query 1 — parametros ]")
    input_file     = _ask("input_file",              "HI-Medium_Trans.csv")
    n_filter_usd   = _ask("filter_usd   (workers)",  3)
    n_filter_amount= _ask("filter_amount (workers)",  3)
    batch_size     = _ask("batch_size",               10000)
    output         = _ask("output file",              "docker-compose-q1.yaml")
    _dump(generate_compose_q1(input_file, n_filter_usd, n_filter_amount, batch_size), output)


def _gen_q3():
    print("\n[ Query 3 — parametros ]")
    input_file   = _ask("input_file",             "HI-Medium_Trans.csv")
    n_filter_usd = _ask("filter_usd   (workers)", 3)
    n_split_date = _ask("split_date   (workers)", 3)
    n_avg        = _ask("avg          (workers)", 2)
    n_avg_joiner = _ask("avg_joiner   (workers)", 5)
    batch_size   = _ask("batch_size",             1000)
    output       = _ask("output file",            "docker-compose-q3.yaml")
    _dump(generate_compose_q3(input_file, n_filter_usd, n_split_date, n_avg, n_avg_joiner, batch_size), output)


def _gen_q4():
    print("\n[ Query 4 — parametros ]")
    input_file    = _ask("input_file",              "HI-Medium_Trans.csv")
    n_filter_usd  = _ask("filter_usd   (workers)",  3)
    n_filter_date = _ask("filter_date  (workers)",  3)
    n_split       = _ask("split        (workers)",  3)
    n_og_detect   = _ask("og_detect    (workers)",  3)
    n_dt_detect   = _ask("dt_detect    (workers)",  3)
    n_sg_detect   = _ask("sg_detect    (workers)",  3)
    batch_size    = _ask("batch_size",               20000)
    output        = _ask("output file",              "docker-compose-q4.yaml")
    _dump(generate_compose_q4(input_file, n_filter_usd, n_filter_date, n_split,
                               n_og_detect, n_dt_detect, n_sg_detect, batch_size), output)


def _gen_q5():
    print("\n[ Query 5 — parametros ]")
    input_file      = _ask("input_file",               "HI-Medium_Trans.csv")
    n_filter_fmt    = _ask("filter_fmt    (workers)",   7)
    n_converter     = _ask("converter     (workers)",   3)
    n_filter_amount = _ask("filter_amount (workers)",   2)
    batch_size      = _ask("batch_size",                10000)
    output          = _ask("output file",               "docker-compose-q5.yaml")
    _dump(generate_compose_q5(n_filter_fmt, n_converter, n_filter_amount, input_file, batch_size), output)


def _gen_all():
    print("\n[ Todas las queries — parametros ]")
    input_file = _ask("input_file", "HI-Medium_Trans.csv")

    print("  -- Q1 --")
    q1_n_filter_usd    = _ask("  filter_usd   (workers)", 3)
    q1_n_filter_amount = _ask("  filter_amount (workers)", 3)
    q1_batch_size      = _ask("  batch_size",              10000)

    print("  -- Q3 --")
    q3_n_filter_usd  = _ask("  filter_usd   (workers)", 3)
    q3_n_split_date  = _ask("  split_date   (workers)", 3)
    q3_n_avg         = _ask("  avg          (workers)", 2)
    q3_n_avg_joiner  = _ask("  avg_joiner   (workers)", 5)
    q3_batch_size    = _ask("  batch_size",              1000)

    print("  -- Q4 --")
    q4_n_filter_usd  = _ask("  filter_usd   (workers)", 3)
    q4_n_filter_date = _ask("  filter_date  (workers)", 3)
    q4_n_split       = _ask("  split        (workers)", 3)
    q4_n_og_detect   = _ask("  og_detect    (workers)", 3)
    q4_n_dt_detect   = _ask("  dt_detect    (workers)", 3)
    q4_n_sg_detect   = _ask("  sg_detect    (workers)", 3)
    q4_batch_size    = _ask("  batch_size",              20000)

    print("  -- Q5 --")
    q5_n_filter_fmt    = _ask("  filter_fmt    (workers)", 7)
    q5_n_converter     = _ask("  converter     (workers)", 3)
    q5_n_filter_amount = _ask("  filter_amount (workers)", 2)
    q5_batch_size      = _ask("  batch_size",              10000)

    output = _ask("output file", "docker-compose-all.yaml")

    _dump(generate_compose_all(
        input_file=input_file,
        q1_n_filter_usd=q1_n_filter_usd,
        q1_n_filter_amount=q1_n_filter_amount,
        q1_batch_size=q1_batch_size,
        q3_n_filter_usd=q3_n_filter_usd,
        q3_n_split_date=q3_n_split_date,
        q3_n_avg=q3_n_avg,
        q3_n_avg_joiner=q3_n_avg_joiner,
        q3_batch_size=q3_batch_size,
        q4_n_filter_usd=q4_n_filter_usd,
        q4_n_filter_date=q4_n_filter_date,
        q4_n_split=q4_n_split,
        q4_n_og_detect=q4_n_og_detect,
        q4_n_dt_detect=q4_n_dt_detect,
        q4_n_sg_detect=q4_n_sg_detect,
        q4_batch_size=q4_batch_size,
        q5_n_filter_fmt=q5_n_filter_fmt,
        q5_n_converter=q5_n_converter,
        q5_n_filter_amount=q5_n_filter_amount,
        q5_batch_size=q5_batch_size,
    ), output)


# ---------------------------------------------------------------------------

_GENERATORS = {
    "1":   _gen_q1,
    "3":   _gen_q3,
    "4":   _gen_q4,
    "5":   _gen_q5,
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
