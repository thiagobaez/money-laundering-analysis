#!/usr/bin/env python3
import yaml
import argparse


def generate_compose_q4(
    input_files: list,
    n_filter_usd: int = 3,
    n_filter_date: int = 3,
    n_split: int = 3,
    n_detect: int = 3,
    batch_size: int = 10000,
):
    services = {}
    rabbitmq_healthy = {"rabbitmq": {"condition": "service_healthy"}}

    origin_routing_keys = ",".join([f"tx_origin_{i + 1}" for i in range(n_detect)])
    destination_routing_keys = ",".join(
        [f"tx_destination_{i + 1}" for i in range(n_detect)]
    )
    og_detect_routing_keys = ",".join(
        [f"og_detect_{i + 1}" for i in range(n_detect)]
    )
    sg_detect_routing_keys = ",".join(
        [f"sg_detect_{i + 1}" for i in range(n_detect)]
    )

    for i in range(n_detect):
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
                f"NUM_OG_WORKERS={n_detect}",
                f"NUM_DT_WORKERS={n_detect}",
            ],
        }

    sg_detect_depends = {
        f"q4_sg_detect_{i}": {"condition": "service_started"}
        for i in range(n_detect)
    }
    for i in range(n_detect):
        services[f"q4_og_detect_{i}"] = {
            "build": {
                "context": "./src/",
                "dockerfile": "business/query4/og_detect/Dockerfile",
            },
            "container_name": f"q4_og_detect_{i}",
            "depends_on": {
                **dict(rabbitmq_healthy),
                **sg_detect_depends,
            },
            "environment": [
                "QUERY_NUMBER=4",
                "MOM_HOST=rabbitmq",
                "EXCHANGE_NAME=q4_split_exchange",
                f"ORIGIN_ROUTING_KEY=tx_origin_{i + 1}",
                "OUTPUT_EXCHANGE_NAME=q4_og_detect_exchange",
                f"OUTPUT_ROUTING_KEYS={og_detect_routing_keys}",
                "MIN_DESTINATIONS=5",
            ],
        }

    for i in range(n_detect):
        services[f"q4_dt_detect_{i}"] = {
            "build": {
                "context": "./src/",
                "dockerfile": "business/query4/dt_detect/Dockerfile",
            },
            "container_name": f"q4_dt_detect_{i}",
            "depends_on": {
                **dict(rabbitmq_healthy),
                **sg_detect_depends,
            },
            "environment": [
                "QUERY_NUMBER=4",
                "MOM_HOST=rabbitmq",
                "INPUT_EXCHANGE_NAME=q4_split_exchange",
                f"INPUT_ROUTING_KEY=tx_destination_{i + 1}",
                "OUTPUT_EXCHANGE_NAME=q4_dt_detect_exchange",
                f"OUTPUT_ROUTING_KEYS={sg_detect_routing_keys}",
                "MIN_ORIGINS=5",
            ],
        }

    og_detect_depends = {
        f"q4_og_detect_{i}": {"condition": "service_started"}
        for i in range(n_detect)
    }
    dt_detect_depends = {
        f"q4_dt_detect_{i}": {"condition": "service_started"}
        for i in range(n_detect)
    }
    for i in range(n_split):
        services[f"q4_split_{i}"] = {
            "build": {
                "context": "./src/",
                "dockerfile": "business/common_workers/split/Dockerfile",
            },
            "container_name": f"q4_split_{i}",
            "depends_on": {
                **dict(rabbitmq_healthy),
                **og_detect_depends,
                **dt_detect_depends,
            },
            "environment": [
                "QUERY_NUMBER=4",
                f"SPLIT_AMOUNT={n_split}",
                "INPUT_QUEUE=tx_usd_date",
                "EXCHANGE_NAME=q4_split_exchange",
                f"ORIGIN_ROUTING_KEYS={origin_routing_keys}",
                f"DESTINATION_ROUTING_KEYS={destination_routing_keys}",
                "MOM_HOST=rabbitmq",
                f"BATCH_SIZE={batch_size}",
            ],
        }

    split_depends = {
        f"q4_split_{i}": {"condition": "service_started"} for i in range(n_split)
    }
    for i in range(n_filter_date):
        services[f"q4_filter_date_{i}"] = {
            "build": {
                "context": "./src/",
                "dockerfile": "business/common_workers/filter/Dockerfile",
            },
            "container_name": f"q4_filter_date_{i}",
            "depends_on": {
                **dict(rabbitmq_healthy),
                **split_depends,
            },
            "environment": [
                "ADD_QUERY_ID=True",
                f"FILTER_AMOUNT={n_filter_date}",
                "QUERY_NUMBER=4",
                "GE_DATE=2022-09-01",
                "LE_DATE=2022-09-05",
                "INPUT_QUEUE=q4_filter_date",
                "OUTPUT_QUEUES=tx_usd_date",
                "MOM_HOST=rabbitmq",
                f"BATCH_SIZE={batch_size}",
            ],
        }

    filter_date_depends = {
        f"q4_filter_date_{i}": {"condition": "service_started"}
        for i in range(n_filter_date)
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
                **filter_date_depends,
            },
            "environment": [
                f"FILTER_AMOUNT={n_filter_usd}",
                "QUERY_NUMBER=1",
                "INPUT_EXCHANGE_NAME=input_gateway_exchange",
                "INPUT_ROUTING_KEYS=filter_usd",
                "MOM_HOST=rabbitmq",
                "OUTPUT_QUEUES=q4_filter_date",
                "USD_ONLY=True",
                f"BATCH_SIZE={batch_size}",
            ],
        }

    filter_usd_depends = {
        f"filter_usd_{i}": {"condition": "service_started"} for i in range(n_filter_usd)
    }
    for i, f in enumerate(input_files):
        services[f"client{i}"] = {
            "container_name": f"client{i}",
            "build": {"context": "./src", "dockerfile": "client/Dockerfile"},
            "depends_on": filter_usd_depends,
            "environment": [
                "SERVER_HOST=gateway",
                "SERVER_PORT=5678",
                f"INPUT_FILE=/datasets/{f}",
                f"OUTPUT_FILE=/output/client{i}/output.csv",
                f"BATCH_SIZE={batch_size}",
            ],
            "volumes": ["./datasets:/datasets", "./output:/output"],
        }

    services["gateway"] = {
        "build": {"context": "./src/", "dockerfile": "gateway/Dockerfile"},
        "container_name": "gateway",
        "depends_on": dict(rabbitmq_healthy),
        "environment": [
            "INPUT_EXCHANGE_NAME=input_gateway_exchange",
            "INPUT_ROUTING_KEYS=filter_usd",
            "MOM_HOST=rabbitmq",
            "OUTPUT_QUEUE=results_queue",
            f"NUM_EXPECTED_EOFS={n_detect}",
            "PYTHONUNBUFFERED=1",
            "SERVER_HOST=gateway",
            "SERVER_PORT=5678",
        ],
    }

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
    parser = argparse.ArgumentParser(description="Generate docker-compose-q4.yaml")
    parser.add_argument("--input-files", nargs="+", default=["HI-Medium_Trans.csv"])
    parser.add_argument("--output", type=str, default="docker-compose-q4.yaml")
    parser.add_argument("--batch-size", type=int, default=20000)
    parser.add_argument("--filter-usd", type=int, default=3)
    parser.add_argument("--filter-date", type=int, default=3)
    parser.add_argument("--split", type=int, default=3)
    parser.add_argument("--detect", type=int, default=3)
    args = parser.parse_args()

    compose = generate_compose_q4(
        input_files=args.input_files,
        n_filter_usd=args.filter_usd,
        n_filter_date=args.filter_date,
        n_split=args.split,
        n_detect=args.detect,
        batch_size=args.batch_size,
    )

    with open(args.output, "w") as f:
        yaml.dump(
            compose,
            f,
            default_flow_style=False,
            sort_keys=False,
            Dumper=yaml.SafeDumper,
        )

    print(f"Generated {args.output} with:")
    print(f"  input_files:   {args.input_files}")
    print(f"  filter_usd:    {args.filter_usd}")
    print(f"  filter_date:   {args.filter_date}")
    print(f"  split:         {args.split}")
    print(f"  detect:        {args.detect}")
    print(f"  batch_size:    {args.batch_size}")


if __name__ == "__main__":
    main()
