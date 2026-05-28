#!/usr/bin/env python3
"""
Genera un docker-compose con las 4 queries corriendo en simultaneo.

El gateway usa un exchange directo (input_gateway_exchange) con 2 routing keys:
  - "filter_usd"    → pool compartido que filtra USD y distribuye a q1/q3/q4
  - "q5_filter_fmt" → workers de q5 (filtro de formato y fecha)

NUM_EXPECTED_EOFS = 1 (q1) + q3_n_avg_joiner (q3) + q4_n_detect (q4) + 1 (q5)

IMPORTANTE: og_detect, dt_detect y sg_detect deben tener el mismo número de workers
porque cada sg_detect_i escucha exclusivamente en og_detect_{i+1}. Si difieren,
los sg_detect sobrantes nunca reciben datos y el sistema se bloquea esperando sus EOFs.
"""

import yaml
import argparse


def generate_compose_all(
    input_file: str,
    # shared filter_usd (queries 1, 3, 4)
    n_filter_usd: int = 7,
    filter_usd_batch_size: int = 10000,
    # q1
    q1_n_filter_amount: int = 3,
    q1_batch_size: int = 10000,
    # q3
    q3_n_split_date: int = 3,
    q3_n_avg: int = 2,
    q3_n_avg_joiner: int = 5,
    q3_batch_size: int = 10000,
    # q4
    q4_n_filter_date: int = 3,
    q4_n_split: int = 3,
    q4_n_detect: int = 3,
    q4_batch_size: int = 20000,
    # q5
    q5_n_filter_fmt: int = 7,
    q5_n_converter: int = 3,
    q5_n_filter_amount: int = 2,
    q5_batch_size: int = 10000,
):
    services = {}
    rabbitmq_healthy = {"rabbitmq": {"condition": "service_healthy"}}

    num_expected_eofs = 1 + q3_n_avg_joiner + q4_n_detect + 1

    # -------------------------------------------------------------------------
    # Routing keys para q4 (1-indexed)
    # -------------------------------------------------------------------------
    q4_origin_rks = ",".join([f"tx_origin_{i + 1}" for i in range(q4_n_detect)])
    q4_dest_rks = ",".join([f"tx_destination_{i + 1}" for i in range(q4_n_detect)])
    q4_og_rks = ",".join([f"og_detect_{i + 1}" for i in range(q4_n_detect)])
    q4_sg_rks = ",".join([f"sg_detect_{i + 1}" for i in range(q4_n_detect)])

    # -------------------------------------------------------------------------
    # Routing keys para q3
    # -------------------------------------------------------------------------
    q3_avg_joiner_rks = ",".join([f"avg_joiner_{i}" for i in range(q3_n_avg_joiner)])
    q3_avg_rks = ",".join([f"avg_queue_{i}" for i in range(q3_n_avg)])

    # =========================================================================
    # Q4 — empieza desde el fondo de la cadena
    # =========================================================================

    # q4_sg_detect
    for i in range(q4_n_detect):
        services[f"q4_sg_detect_{i}"] = {
            "build": {
                "context": "./src/",
                "dockerfile": "business/query4/sg_detect/Dockerfile",
            },
            "container_name": f"q4_sg_detect_{i}",
            "depends_on": {
                **dict(rabbitmq_healthy),
                "gateway": {"condition": "service_started"},
            },
            "environment": [
                "QUERY_NUMBER=4",
                "MOM_HOST=rabbitmq",
                "ORIGIN_EXCHANGE_NAME=q4_og_detect_exchange",
                f"ORIGIN_ROUTING_KEY=og_detect_{i + 1}",
                "DESTINATION_EXCHANGE_NAME=q4_dt_detect_exchange",
                f"DESTINATION_ROUTING_KEY=sg_detect_{i + 1}",
                "OUTPUT_QUEUE=results_queue",
                "MIN_COMMON=5",
                f"NUM_OG_WORKERS={q4_n_detect}",
                f"NUM_DT_WORKERS={q4_n_detect}",
                f"BATCH_SIZE={q4_batch_size}",
            ],
        }

    # q4_og_detect
    q4_sg_depends = {
        f"q4_sg_detect_{i}": {"condition": "service_started"}
        for i in range(q4_n_detect)
    }
    for i in range(q4_n_detect):
        services[f"q4_og_detect_{i}"] = {
            "build": {
                "context": "./src/",
                "dockerfile": "business/query4/og_detect/Dockerfile",
            },
            "container_name": f"q4_og_detect_{i}",
            "depends_on": {**dict(rabbitmq_healthy), **q4_sg_depends},
            "environment": [
                "QUERY_NUMBER=4",
                "MOM_HOST=rabbitmq",
                "EXCHANGE_NAME=q4_split_exchange",
                f"ORIGIN_ROUTING_KEY=tx_origin_{i + 1}",
                "OUTPUT_EXCHANGE_NAME=q4_og_detect_exchange",
                f"OUTPUT_ROUTING_KEYS={q4_og_rks}",
                "MIN_DESTINATIONS=5",
            ],
        }

    # q4_dt_detect
    for i in range(q4_n_detect):
        services[f"q4_dt_detect_{i}"] = {
            "build": {
                "context": "./src/",
                "dockerfile": "business/query4/dt_detect/Dockerfile",
            },
            "container_name": f"q4_dt_detect_{i}",
            "depends_on": {**dict(rabbitmq_healthy), **q4_sg_depends},
            "environment": [
                "QUERY_NUMBER=4",
                "MOM_HOST=rabbitmq",
                "INPUT_EXCHANGE_NAME=q4_split_exchange",
                f"INPUT_ROUTING_KEY=tx_destination_{i + 1}",
                "OUTPUT_EXCHANGE_NAME=q4_dt_detect_exchange",
                f"OUTPUT_ROUTING_KEYS={q4_sg_rks}",
                "MIN_ORIGINS=5",
            ],
        }

    # q4_split
    q4_og_depends = {
        f"q4_og_detect_{i}": {"condition": "service_started"}
        for i in range(q4_n_detect)
    }
    q4_dt_depends = {
        f"q4_dt_detect_{i}": {"condition": "service_started"}
        for i in range(q4_n_detect)
    }
    for i in range(q4_n_split):
        services[f"q4_split_{i}"] = {
            "build": {
                "context": "./src/",
                "dockerfile": "business/common_workers/split/Dockerfile",
            },
            "container_name": f"q4_split_{i}",
            "depends_on": {**dict(rabbitmq_healthy), **q4_og_depends, **q4_dt_depends},
            "environment": [
                "QUERY_NUMBER=4",
                f"SPLIT_AMOUNT={q4_n_split}",
                "INPUT_QUEUE=tx_usd_date",
                "EXCHANGE_NAME=q4_split_exchange",
                f"ORIGIN_ROUTING_KEYS={q4_origin_rks}",
                f"DESTINATION_ROUTING_KEYS={q4_dest_rks}",
                "MOM_HOST=rabbitmq",
                f"BATCH_SIZE={q4_batch_size}",
            ],
        }

    # q4_filter_date — recibe USD de filter_usd compartido, filtra por fecha
    q4_split_depends = {
        f"q4_split_{i}": {"condition": "service_started"} for i in range(q4_n_split)
    }
    for i in range(q4_n_filter_date):
        services[f"q4_filter_date_{i}"] = {
            "build": {
                "context": "./src/",
                "dockerfile": "business/common_workers/filter/Dockerfile",
            },
            "container_name": f"q4_filter_date_{i}",
            "depends_on": {**dict(rabbitmq_healthy), **q4_split_depends},
            "environment": [
                "ADD_QUERY_ID=True",
                f"FILTER_AMOUNT={q4_n_filter_date}",
                "QUERY_NUMBER=4",
                "GE_DATE=2022-09-01",
                "LE_DATE=2022-09-05",
                "INPUT_QUEUE=q4_filter_date",
                "OUTPUT_QUEUES=tx_usd_date",
                "MOM_HOST=rabbitmq",
                f"BATCH_SIZE={q4_batch_size}",
            ],
        }

    # =========================================================================
    # Q1
    # =========================================================================

    # q1_filter_amount — recibe USD de filter_usd compartido, filtra por monto
    for i in range(q1_n_filter_amount):
        services[f"q1_filter_amount_{i}"] = {
            "build": {
                "context": "./src/",
                "dockerfile": "business/common_workers/filter/Dockerfile",
            },
            "container_name": f"q1_filter_amount_{i}",
            "depends_on": {
                **dict(rabbitmq_healthy),
                "gateway": {"condition": "service_started"},
            },
            "environment": [
                f"FILTER_AMOUNT={q1_n_filter_amount}",
                "QUERY_NUMBER=1",
                "MOM_HOST=rabbitmq",
                "INPUT_QUEUE=q1_filter_amount",
                "OUTPUT_QUEUES=results_queue",
                "MAX_AMOUNT=50",
                "ADD_QUERY_ID=True",
                f"BATCH_SIZE={q1_batch_size}",
            ],
        }

    # =========================================================================
    # Q3
    # =========================================================================

    # avg_joiner (arranca primero — sin dependencias de pipeline)
    for i in range(q3_n_avg_joiner):
        services[f"avg_joiner_{i}"] = {
            "build": {
                "context": "./src/",
                "dockerfile": "business/query3/avg_joiner/Dockerfile",
            },
            "container_name": f"avg_joiner_{i}",
            "depends_on": dict(rabbitmq_healthy),
            "volumes": ["./src/business/query3/spill_to_disk:/data"],
            "environment": [
                "QUERY_NUMBER=3",
                "MOM_HOST=rabbitmq",
                "SECOND_PERIOD_QUEUE=second_period_queue",
                f"AVG_QUEUE=avg_joiner_{i}",
                "OUTPUT_QUEUE=results_queue",
                f"AVG_JOINER_AMOUNT={q3_n_avg_joiner}",
                f"AVG_AMOUNT={q3_n_avg}",
                f"DATA_DIR=/data/joiner_{i}",
                f"BATCH_SIZE={q3_batch_size}",
            ],
        }

    # avg (depende de avg_joiner para que bindee primero)
    avg_joiner_depends = {
        f"avg_joiner_{i}": {"condition": "service_started"}
        for i in range(q3_n_avg_joiner)
    }
    for i in range(q3_n_avg):
        services[f"avg_{i}"] = {
            "build": {
                "context": "./src/",
                "dockerfile": "business/query3/avg/Dockerfile",
            },
            "container_name": f"avg_{i}",
            "depends_on": {**dict(rabbitmq_healthy), **avg_joiner_depends},
            "environment": [
                "QUERY_NUMBER=3",
                "MOM_HOST=rabbitmq",
                f"INPUT_QUEUE=avg_queue_{i}",
                f"OUTPUT_QUEUES={q3_avg_joiner_rks}",
                f"AVG_AMOUNT={q3_n_avg}",
            ],
        }

    # split_date — depende de avg para que bindee las avg_queue primero
    q3_avg_depends = {
        f"avg_{i}": {"condition": "service_started"} for i in range(q3_n_avg)
    }
    for i in range(q3_n_split_date):
        services[f"split_date_{i}"] = {
            "build": {
                "context": "./src/",
                "dockerfile": "business/query3/split_date/Dockerfile",
            },
            "container_name": f"split_date_{i}",
            "depends_on": {
                **dict(rabbitmq_healthy),
                **q3_avg_depends,
            },
            "environment": [
                "QUERY_NUMBER=3",
                "MOM_HOST=rabbitmq",
                "INPUT_QUEUE=q3_split_queue",
                f"FIRST_PERIOD_QUEUES={q3_avg_rks}",
                "SECOND_PERIOD_QUEUE=second_period_queue",
                "FIRST_PERIOD_GE=2022-09-01",
                "FIRST_PERIOD_LE=2022-09-05",
                "SECOND_PERIOD_GE=2022-09-06",
                "SECOND_PERIOD_LE=2022-09-14",
                f"SPLIT_AMOUNT={q3_n_split_date}",
                f"BATCH_SIZE={q3_batch_size}",
            ],
        }

    # =========================================================================
    # Q5
    # =========================================================================

    # filter_q5_fmt (usa exchange con routing key propio)
    for i in range(q5_n_filter_fmt):
        services[f"filter_q5_fmt_{i}"] = {
            "build": {
                "context": "./src/",
                "dockerfile": "business/common_workers/filter/Dockerfile",
            },
            "container_name": f"filter_q5_fmt_{i}",
            "depends_on": {
                **dict(rabbitmq_healthy),
                "gateway": {"condition": "service_started"},
            },
            "environment": [
                "QUERY_NUMBER=5",
                "MOM_HOST=rabbitmq",
                "INPUT_EXCHANGE_NAME=input_gateway_exchange",
                "INPUT_ROUTING_KEYS=q5_filter_fmt",
                "OUTPUT_QUEUES=converter_queue",
                f"FILTER_AMOUNT={q5_n_filter_fmt}",
                "GE_DATE=2022-09-01",
                "LE_DATE=2022-09-05",
                "PAY_FMTS=Wire,ACH",
                f"BATCH_SIZE={q5_batch_size}",
            ],
        }

    # converter
    q5_fmt_depends = {
        f"filter_q5_fmt_{i}": {"condition": "service_started"}
        for i in range(q5_n_filter_fmt)
    }
    for i in range(q5_n_converter):
        services[f"converter_{i}"] = {
            "build": {
                "context": "./src/",
                "dockerfile": "business/common_workers/converter/Dockerfile",
                "network": "host",
            },
            "container_name": f"converter_{i}",
            "depends_on": {**dict(rabbitmq_healthy), **q5_fmt_depends},
            "environment": [
                "MOM_HOST=rabbitmq",
                "INPUT_QUEUE=converter_queue",
                "OUTPUT_QUEUE=filter_q5_amount_queue",
                f"CONVERTER_AMOUNT={q5_n_converter}",
                "FRANKFURTER_BASE=https://api.frankfurter.dev/v2",
                f"BATCH_SIZE={q5_batch_size}",
            ],
        }

    # filter_q5_amount
    q5_converter_depends = {
        f"converter_{i}": {"condition": "service_started"}
        for i in range(q5_n_converter)
    }
    for i in range(q5_n_filter_amount):
        services[f"filter_q5_amount_{i}"] = {
            "build": {
                "context": "./src/",
                "dockerfile": "business/common_workers/filter/Dockerfile",
            },
            "container_name": f"filter_q5_amount_{i}",
            "depends_on": {**dict(rabbitmq_healthy), **q5_converter_depends},
            "environment": [
                "QUERY_NUMBER=5",
                "ADD_QUERY_ID=True",
                "MOM_HOST=rabbitmq",
                "INPUT_QUEUE=filter_q5_amount_queue",
                "OUTPUT_QUEUES=results_queue",
                f"FILTER_AMOUNT={q5_n_filter_amount}",
                "MAX_AMOUNT=1.0",
                f"BATCH_SIZE={q5_batch_size}",
            ],
        }

    # =========================================================================
    # Pool compartido filter_usd — queries 1, 3 y 4
    # Lee del gateway via routing key "filter_usd", filtra USD y broadcastea
    # las transacciones a los 3 pipelines downstream simultáneamente.
    # =========================================================================
    q1_filter_amount_depends = {
        f"q1_filter_amount_{i}": {"condition": "service_started"}
        for i in range(q1_n_filter_amount)
    }
    split_date_depends = {
        f"split_date_{i}": {"condition": "service_started"}
        for i in range(q3_n_split_date)
    }
    q4_filter_date_depends = {
        f"q4_filter_date_{i}": {"condition": "service_started"}
        for i in range(q4_n_filter_date)
    }
    for i in range(n_filter_usd):
        services[f"filter_usd_{i}"] = {
            "build": {
                "context": "./src/",
                "dockerfile": "business/common_workers/filter/Dockerfile",
            },
            "container_name": f"filter_usd_{i}",
            "depends_on": {
                **dict(rabbitmq_healthy),
                "gateway": {"condition": "service_started"},
                **q1_filter_amount_depends,
                **split_date_depends,
                **q4_filter_date_depends,
            },
            "environment": [
                f"FILTER_AMOUNT={n_filter_usd}",
                "QUERY_NUMBER=0",
                "INPUT_EXCHANGE_NAME=input_gateway_exchange",
                "INPUT_ROUTING_KEYS=filter_usd",
                "MOM_HOST=rabbitmq",
                "OUTPUT_QUEUES=q1_filter_amount,q3_split_queue,q4_filter_date",
                "USD_ONLY=True",
                f"BATCH_SIZE={filter_usd_batch_size}",
            ],
        }

    # =========================================================================
    # client0 — depende de los workers de primera linea de cada query
    # =========================================================================
    filter_usd_depends = {
        f"filter_usd_{i}": {"condition": "service_started"} for i in range(n_filter_usd)
    }
    q5_filter_fmt_depends_client = {
        f"filter_q5_fmt_{i}": {"condition": "service_started"}
        for i in range(q5_n_filter_fmt)
    }
    services["client0"] = {
        "container_name": "client",
        "build": {"context": "./src", "dockerfile": "client/Dockerfile"},
        "depends_on": {
            **filter_usd_depends,
            **q5_filter_fmt_depends_client,
        },
        "environment": [
            "SERVER_HOST=gateway",
            "SERVER_PORT=5678",
            f"INPUT_FILE=/datasets/{input_file}",
            "OUTPUT_FILE=/output/client0/output.csv",
        ],
        "volumes": ["./datasets:/datasets", "./output:/output"],
    }

    # =========================================================================
    # gateway
    # =========================================================================
    services["gateway"] = {
        "build": {"context": "./src/", "dockerfile": "gateway/Dockerfile"},
        "container_name": "gateway",
        "depends_on": dict(rabbitmq_healthy),
        "environment": [
            "INPUT_EXCHANGE_NAME=input_gateway_exchange",
            "INPUT_ROUTING_KEYS=filter_usd,q5_filter_fmt",
            "MOM_HOST=rabbitmq",
            "OUTPUT_QUEUE=results_queue",
            f"NUM_EXPECTED_EOFS={num_expected_eofs}",
            "PYTHONUNBUFFERED=1",
            "SERVER_HOST=gateway",
            "SERVER_PORT=5678",
        ],
    }

    # =========================================================================
    # rabbitmq
    # =========================================================================
    services["rabbitmq"] = {
        "build": {"context": "./src/", "dockerfile": "rabbitmq/Dockerfile"},
        "container_name": "rabbitmq",
        "environment": ["RABBITMQ_LOG_LEVELS=error"],
        "healthcheck": {
            "interval": "5s",
            "retries": 10,
            "start_period": "50s",
            "test": "rabbitmq-diagnostics check_port_connectivity",
            "timeout": "3s",
        },
        "ports": ["5672:5672", "15672:15672"],
    }

    return {"services": services}


def main():
    parser = argparse.ArgumentParser(
        description="Generate docker-compose-all.yaml (queries 1+3+4+5)"
    )
    parser.add_argument("--input-file", type=str, default="HI-Medium_Trans.csv")
    parser.add_argument("--output", type=str, default="docker-compose-all.yaml")
    # shared filter_usd
    parser.add_argument("--filter-usd", type=int, default=3)
    parser.add_argument("--filter-usd-batch-size", type=int, default=10000)
    # q1
    parser.add_argument("--q1-filter-amount", type=int, default=3)
    parser.add_argument("--q1-batch-size", type=int, default=10000)
    # q3
    parser.add_argument("--q3-split-date", type=int, default=3)
    parser.add_argument("--q3-avg", type=int, default=2)
    parser.add_argument("--q3-avg-joiner", type=int, default=5)
    parser.add_argument("--q3-batch-size", type=int, default=1000)
    # q4
    parser.add_argument("--q4-filter-date", type=int, default=3)
    parser.add_argument("--q4-split", type=int, default=3)
    parser.add_argument("--q4-detect", type=int, default=3)
    parser.add_argument("--q4-batch-size", type=int, default=20000)
    # q5
    parser.add_argument("--q5-filter-fmt", type=int, default=7)
    parser.add_argument("--q5-converter", type=int, default=3)
    parser.add_argument("--q5-filter-amount", type=int, default=2)
    parser.add_argument("--q5-batch-size", type=int, default=10000)
    args = parser.parse_args()

    compose = generate_compose_all(
        input_file=args.input_file,
        n_filter_usd=args.filter_usd,
        filter_usd_batch_size=args.filter_usd_batch_size,
        q1_n_filter_amount=args.q1_filter_amount,
        q1_batch_size=args.q1_batch_size,
        q3_n_split_date=args.q3_split_date,
        q3_n_avg=args.q3_avg,
        q3_n_avg_joiner=args.q3_avg_joiner,
        q3_batch_size=args.q3_batch_size,
        q4_n_filter_date=args.q4_filter_date,
        q4_n_split=args.q4_split,
        q4_n_detect=args.q4_detect,
        q4_batch_size=args.q4_batch_size,
        q5_n_filter_fmt=args.q5_filter_fmt,
        q5_n_converter=args.q5_converter,
        q5_n_filter_amount=args.q5_filter_amount,
        q5_batch_size=args.q5_batch_size,
    )

    with open(args.output, "w") as f:
        yaml.dump(
            compose,
            f,
            default_flow_style=False,
            sort_keys=False,
            Dumper=yaml.SafeDumper,
        )

    num_eofs = 1 + args.q3_avg_joiner + args.q4_detect + 1
    print(f"Generated {args.output} with:")
    print(f"  input_file:          {args.input_file}")
    print(
        f"  Shared filter_usd:   {args.filter_usd} workers (batch={args.filter_usd_batch_size})"
    )
    print(f"  Q1: filter_amount={args.q1_filter_amount}  batch={args.q1_batch_size}")
    print(
        f"  Q3: split_date={args.q3_split_date}  avg={args.q3_avg}  avg_joiner={args.q3_avg_joiner}  batch={args.q3_batch_size}"
    )
    print(
        f"  Q4: filter_date={args.q4_filter_date}  split={args.q4_split}  detect={args.q4_detect}  batch={args.q4_batch_size}"
    )
    print(
        f"  Q5: filter_fmt={args.q5_filter_fmt}  converter={args.q5_converter}  filter_amount={args.q5_filter_amount}  batch={args.q5_batch_size}"
    )
    print(f"  NUM_EXPECTED_EOFS:   {num_eofs}")


if __name__ == "__main__":
    main()
