#!/usr/bin/env python3
import yaml
import argparse


def generate_compose_q3(
    input_file: str,
    send_rate_limit: float = 0.001,
    n_filter_usd: int = 1,
    n_split_date: int = 1,
    n_avg: int = 1,
    n_avg_joiner: int = 1,
):
    services = {}
    rabbitmq_healthy = {"rabbitmq": {"condition": "service_healthy"}}

    avg_joiner_routing_keys = ",".join([f"avg_joiner_{i}" for i in range(n_avg_joiner)])
    avg_routing_keys = ",".join([f"avg_{i}" for i in range(n_avg)])

    # client
    services["client0"] = {
        "container_name": "client",
        "build": {"context": "./src", "dockerfile": "client/Dockerfile"},
        "depends_on": ["gateway"],
        "environment": [
            "SERVER_HOST=gateway",
            "SERVER_PORT=5678",
            f"INPUT_FILE=/datasets/{input_file}",
            "OUTPUT_FILE=/output/client0/output.csv",
        ],
        "volumes": ["./datasets:/datasets", "./output:/output"],
    }

    # gateway
    services["gateway"] = {
        "build": {"context": "./src/", "dockerfile": "gateway/Dockerfile"},
        "container_name": "gateway",
        "depends_on": dict(rabbitmq_healthy),
        "environment": [
            "INPUT_QUEUE=filter_usd_queue",
            "MOM_HOST=rabbitmq",
            "OUTPUT_QUEUE=results_queue",
            "NUM_EXPECTED_EOFS=1",
            "PYTHONUNBUFFERED=1",
            "SERVER_HOST=gateway",
            "SERVER_PORT=5678",
            f"SEND_RATE_LIMIT={send_rate_limit}",
        ],
    }

    # filter_usd
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
            },
            "environment": [
                "QUERY_NUMBER=3",
                "MOM_HOST=rabbitmq",
                "INPUT_QUEUE=filter_usd_queue",
                "OUTPUT_QUEUES=q3_split_queue",
                f"FILTER_AMOUNT={n_filter_usd}",
                "USD_ONLY=True",
            ],
        }

    # avg_joiner (arranca primero — sin dependencias de pipeline)
    for i in range(n_avg_joiner):
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
                "AVG_EXCHANGE=avg_exchange",
                f"AVG_ROUTING_KEY=avg_joiner_{i}",
                "OUTPUT_QUEUE=results_queue",
                f"AVG_JOINER_AMOUNT={n_avg_joiner}",
            ],
        }

    # avg (depende de avg_joiner para que bindee primero)
    avg_joiner_depends = {
        f"avg_joiner_{i}": {"condition": "service_started"} for i in range(n_avg_joiner)
    }
    for i in range(n_avg):
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
                f"INPUT_EXCHANGE_NAME=first_period_exchange",
                f"INPUT_ROUTING_KEYS=avg_{i}",
                "OUTPUT_EXCHANGE=avg_exchange",
                f"OUTPUT_ROUTING_KEYS={avg_joiner_routing_keys}",
                f"AVG_AMOUNT={n_avg}",
            ],
        }

    # split_date (depende de avg para que bindee al first_period_exchange primero)
    filter_usd_depends = {
        f"filter_usd_{i}": {"condition": "service_started"} for i in range(n_filter_usd)
    }
    avg_depends = {f"avg_{i}": {"condition": "service_started"} for i in range(n_avg)}
    for i in range(n_split_date):
        services[f"split_date_{i}"] = {
            "build": {
                "context": "./src/",
                "dockerfile": "business/query3/split_date/Dockerfile",
            },
            "container_name": f"split_date_{i}",
            "depends_on": {
                **dict(rabbitmq_healthy),
                **filter_usd_depends,
                **avg_depends,
            },
            "environment": [
                "QUERY_NUMBER=3",
                "MOM_HOST=rabbitmq",
                "INPUT_QUEUE=q3_split_queue",
                "FIRST_PERIOD_EXCHANGE=first_period_exchange",
                f"FIRST_PERIOD_ROUTING_KEYS={avg_routing_keys}",
                "SECOND_PERIOD_QUEUE=second_period_queue",
                "FIRST_PERIOD_GE=2022-09-01",
                "FIRST_PERIOD_LE=2022-09-05",
                "SECOND_PERIOD_GE=2022-09-06",
                "SECOND_PERIOD_LE=2022-09-15",
                f"SPLIT_AMOUNT={n_split_date}",
            ],
        }

    # rabbitmq
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
    parser = argparse.ArgumentParser(description="Generate docker-compose-q3.yaml")
    parser.add_argument("--input-file", type=str, default="HI-Small_Trans.csv.gz")
    parser.add_argument("--output", type=str, default="docker-compose-q3.yaml")
    parser.add_argument("--send-rate-limit", type=float, default=0.001)
    parser.add_argument("--filter-usd", type=int, default=1)
    parser.add_argument("--split-date", type=int, default=1)
    parser.add_argument("--avg", type=int, default=1)
    parser.add_argument("--avg-joiner", type=int, default=1)
    args = parser.parse_args()

    compose = generate_compose_q3(
        input_file=args.input_file,
        send_rate_limit=args.send_rate_limit,
        n_filter_usd=args.filter_usd,
        n_split_date=args.split_date,
        n_avg=args.avg,
        n_avg_joiner=args.avg_joiner,
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
    print(f"  input_file:    {args.input_file}")
    print(f"  filter_usd:    {args.filter_usd}")
    print(f"  split_date:    {args.split_date}")
    print(f"  avg:           {args.avg}")
    print(f"  avg_joiner:    {args.avg_joiner}")
    print(f"  send_rate_limit: {args.send_rate_limit}")


if __name__ == "__main__":
    main()
