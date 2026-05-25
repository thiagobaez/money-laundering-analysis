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
    rabbitmq_healthy = {"rabbitmq": {"condition": "service_healthy"}}

    # client
    amount_depends = {
        f"filter_q5_amount_{i}": {"condition": "service_started"}
        for i in range(n_filter_amount)
    }
    services["client0"] = {
        "container_name": "client",
        "build": {"context": "./src", "dockerfile": "client/Dockerfile"},
        "depends_on": amount_depends,
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
        "depends_on": rabbitmq_healthy,
        "environment": [
            "INPUT_QUEUE=filter_q5_fmt_queue",
            "MOM_HOST=rabbitmq",
            "OUTPUT_QUEUE=results_queue",
            "NUM_EXPECTED_EOFS=1",
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
            "depends_on": {
                **rabbitmq_healthy,
                "gateway": {"condition": "service_started"},
            },
            "environment": [
                "QUERY_NUMBER=5",
                "MOM_HOST=rabbitmq",
                "INPUT_QUEUE=filter_q5_fmt_queue",
                "OUTPUT_QUEUES=converter_queue",
                f"FILTER_AMOUNT={n_filter_fmt}",
                "GE_DATE=2022-09-01",
                "LE_DATE=2022-09-05",
                "PAY_FMTS=Wire,ACH",
            ],
        }

    # converter
    fmt_depends = {
        f"filter_q5_fmt_{i}": {"condition": "service_started"}
        for i in range(n_filter_fmt)
    }
    for i in range(n_converter):
        services[f"converter_{i}"] = {
            "build": {
                "context": "./src/",
                "dockerfile": "business/common_workers/converter/Dockerfile",
                "network": "host",
            },
            "container_name": f"converter_{i}",
            "depends_on": {**rabbitmq_healthy, **fmt_depends},
            "environment": [
                "MOM_HOST=rabbitmq",
                "INPUT_QUEUE=converter_queue",
                "OUTPUT_QUEUE=filter_q5_amount_queue",
                f"CONVERTER_AMOUNT={n_converter}",
                "FRANKFURTER_BASE=https://api.frankfurter.dev/v2",
            ],
        }

    # filter_q5_amount
    converter_depends = {
        f"converter_{i}": {"condition": "service_started"} for i in range(n_converter)
    }
    for i in range(n_filter_amount):
        services[f"filter_q5_amount_{i}"] = {
            "build": {
                "context": "./src/",
                "dockerfile": "business/common_workers/filter/Dockerfile",
            },
            "container_name": f"filter_q5_amount_{i}",
            "depends_on": {**rabbitmq_healthy, **converter_depends},
            "environment": [
                "QUERY_NUMBER=5",
                "ADD_QUERY_ID=True",
                "MOM_HOST=rabbitmq",
                "INPUT_QUEUE=filter_q5_amount_queue",
                "OUTPUT_QUEUES=results_queue",
                f"FILTER_AMOUNT={n_filter_amount}",
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
        send_rate_limit=args.send_rate_limit,
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
    print(f"  filter_q5_fmt:    {args.filter_fmt}")
    print(f"  converter:        {args.converter}")
    print(f"  filter_q5_amount: {args.filter_amount}")
    print(f"  send_rate_limit:  {args.send_rate_limit}")


if __name__ == "__main__":
    main()
