#!/usr/bin/env python3
import yaml
import argparse


def generate_compose(
    n_filter_fmt: int,
    n_converter: int,
    n_filter_amount: int,
    input_file: str,
    send_rate_limit: float = 0.001,
):
    services = {}

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
    fmt_routing_keys = ",".join(f"filter_q5_fmt_{i}" for i in range(n_filter_fmt))
    services["gateway"] = {
        "build": {"context": "./src/", "dockerfile": "gateway/Dockerfile"},
        "container_name": "gateway",
        "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
        "environment": [
            "INPUT_EXCHANGE_NAME=input_gateway_exchange",
            f"INPUT_ROUTING_KEYS={fmt_routing_keys}",
            "MOM_HOST=rabbitmq",
            "OUTPUT_QUEUE=results_queue",
            f"NUM_EXPECTED_EOFS={n_filter_amount}",
            "PYTHONUNBUFFERED=1",
            "SERVER_HOST=gateway",
            "SERVER_PORT=5678",
            f"SEND_RATE_LIMIT={send_rate_limit}",
        ],
    }

    # filter_q5_fmt
    for i in range(n_filter_fmt):
        services[f"filter_q5_fmt_{i}"] = {
            "build": {
                "context": "./src/",
                "dockerfile": "business/common_workers/filter/Dockerfile",
            },
            "container_name": f"filter_q5_fmt_{i}",
            "depends_on": ["gateway"],
            "environment": [
                f"ID={i}",
                "QUERY_NUMBER=5",
                "MOM_HOST=rabbitmq",
                "INPUT_EXCHANGE_NAME=input_gateway_exchange",
                f"INPUT_ROUTING_KEYS=filter_q5_fmt_{i}",
                "OUTPUT_QUEUE=converter_queue",
                "FILTER_PREFIX=filter_q5_fmt",
                f"NUM_INSTANCES={n_filter_fmt}",
                "NUM_EXPECTED_EOFS=1",
                "GE_DATE=2022-09-01",
                "LE_DATE=2022-09-05",
                "PAY_FMTS=Wire,ACH",
            ],
        }

    # converter
    fmt_depends = [f"filter_q5_fmt_{i}" for i in range(n_filter_fmt)]
    for i in range(n_converter):
        services[f"converter_{i}"] = {
            "build": {
                "context": "./src/",
                "dockerfile": "business/common_workers/converter/Dockerfile",
                "network": "host",
            },
            "container_name": f"converter_{i}",
            "depends_on": fmt_depends,
            "environment": [
                f"ID={i}",
                "MOM_HOST=rabbitmq",
                "INPUT_QUEUE=converter_queue",
                "OUTPUT_QUEUE=filter_q5_amount_queue",
                "CONVERTER_PREFIX=converter",
                f"NUM_INSTANCES={n_converter}",
                f"NUM_EXPECTED_EOFS={n_filter_fmt}",
                "FRANKFURTER_BASE=https://api.frankfurter.dev/v2",
            ],
        }

    # filter_q5_amount
    converter_depends = [f"converter_{i}" for i in range(n_converter)]
    for i in range(n_filter_amount):
        services[f"filter_q5_amount_{i}"] = {
            "build": {
                "context": "./src/",
                "dockerfile": "business/common_workers/filter/Dockerfile",
            },
            "container_name": f"filter_q5_amount_{i}",
            "depends_on": converter_depends,
            "environment": [
                f"ID={i}",
                "QUERY_NUMBER=5",
                "ADD_QUERY_ID=True",
                "MOM_HOST=rabbitmq",
                "INPUT_QUEUE=filter_q5_amount_queue",
                "OUTPUT_QUEUE=results_queue",
                "FILTER_PREFIX=filter_q5_amount",
                f"NUM_INSTANCES={n_filter_amount}",
                f"NUM_EXPECTED_EOFS={n_converter}",
                "MAX_AMOUNT=1.0",
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
    parser = argparse.ArgumentParser(description="Generate docker-compose-q5.yaml")
    parser.add_argument("--filter-fmt", type=int, default=2)
    parser.add_argument("--converter", type=int, default=2)
    parser.add_argument("--filter-amount", type=int, default=2)
    parser.add_argument("--input-file", type=str, default="HI-Small_Trans.csv.gz")
    parser.add_argument("--output", type=str, default="docker-compose-q5.yaml")
    parser.add_argument("--send-rate-limit", type=float, default=0.001)
    args = parser.parse_args()

    compose = generate_compose(
        n_filter_fmt=args.filter_fmt,
        n_converter=args.converter,
        n_filter_amount=args.filter_amount,
        input_file=args.input_file,
    )

    with open(args.output, "w") as f:
        yaml.dump(compose, f, default_flow_style=False, sort_keys=False)

    print(f"Generated {args.output} with:")
    print(f"  filter_q5_fmt:    {args.filter_fmt}")
    print(f"  converter:        {args.converter}")
    print(f"  filter_q5_amount: {args.filter_amount}")


if __name__ == "__main__":
    main()
