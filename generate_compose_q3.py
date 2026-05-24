#!/usr/bin/env python3
import yaml
import argparse


def generate_compose_q3(
    input_file: str,
    send_rate_limit: float = 0.001,
):
    services = {}

    rabbitmq_healthy = {"rabbitmq": {"condition": "service_healthy"}}

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
    services["filter_usd"] = {
        "build": {
            "context": "./src/",
            "dockerfile": "business/common_workers/filter/Dockerfile",
        },
        "container_name": "filter_usd",
        "depends_on": {**dict(rabbitmq_healthy), "gateway": {"condition": "service_started"}},
        "environment": [
            "QUERY_NUMBER=3",
            "MOM_HOST=rabbitmq",
            "INPUT_QUEUE=filter_usd_queue",
            "OUTPUT_QUEUES=q3_split_queue",
            "FILTER_AMOUNT=1",
            "USD_ONLY=True",
        ],
    }

    # split_date
    services["split_date"] = {
        "build": {
            "context": "./src/",
            "dockerfile": "business/query3/split_date/Dockerfile",
        },
        "container_name": "split_date",
        "depends_on": {**dict(rabbitmq_healthy), "filter_usd": {"condition": "service_started"}},
        "environment": [
            "QUERY_NUMBER=3",
            "MOM_HOST=rabbitmq",
            "INPUT_QUEUE=q3_split_queue",
            "FIRST_PERIOD_EXCHANGE=first_period_exchange",
            "FIRST_PERIOD_ROUTING_KEYS=avg_0",
            "SECOND_PERIOD_QUEUE=second_period_queue",
            "FIRST_PERIOD_GE=2022-09-01",
            "FIRST_PERIOD_LE=2022-09-05",
            "SECOND_PERIOD_GE=2022-09-06",
            "SECOND_PERIOD_LE=2022-09-15",
            "SPLIT_AMOUNT=1",
        ],
    }

    # avg
    services["avg_0"] = {
        "build": {
            "context": "./src/",
            "dockerfile": "business/query3/avg/Dockerfile",
        },
        "container_name": "avg_0",
        "depends_on": {**dict(rabbitmq_healthy), "split_date": {"condition": "service_started"}},
        "environment": [
            "QUERY_NUMBER=3",
            "MOM_HOST=rabbitmq",
            "INPUT_EXCHANGE_NAME=first_period_exchange",
            "INPUT_ROUTING_KEYS=avg_0",
            "OUTPUT_EXCHANGE=avg_exchange",
            "AVG_AMOUNT=1",
        ],
    }

    # avg_joiner
    services["avg_joiner_0"] = {
        "build": {
            "context": "./src/",
            "dockerfile": "business/query3/avg_joiner/Dockerfile",
        },
        "container_name": "avg_joiner_0",
        "depends_on": {
            **dict(rabbitmq_healthy),
            "avg_0": {"condition": "service_started"},
            "split_date": {"condition": "service_started"},
        },
        "volumes": ["./src/business/query3/spill_to_disk:/data"],
        "environment": [
            "QUERY_NUMBER=3",
            "MOM_HOST=rabbitmq",
            "SECOND_PERIOD_QUEUE=second_period_queue",
            "AVG_EXCHANGE=avg_exchange",
            "AVG_ROUTING_KEY=avg_joiner_0",
            "OUTPUT_QUEUE=results_queue",
            "AVG_JOINER_AMOUNT=1",
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
    args = parser.parse_args()

    compose = generate_compose_q3(
        input_file=args.input_file,
        send_rate_limit=args.send_rate_limit,
    )

    with open(args.output, "w") as f:
        yaml.dump(compose, f, default_flow_style=False, sort_keys=False, Dumper=yaml.SafeDumper)

    print(f"Generated {args.output} with:")
    print(f"  input_file:       {args.input_file}")
    print(f"  send_rate_limit:  {args.send_rate_limit}")


if __name__ == "__main__":
    main()