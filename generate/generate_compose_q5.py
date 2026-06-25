import yaml
import argparse


def generate_compose(
    n_filter_fmt: int = 7,
    n_converter: int = 3,
    n_filter_amount: int = 2,
    input_files: list = None,
    batch_size: int = 10000,
    chaos_monkey: bool = False,
    chaos_kill_interval: int = 30,
    chaos_exclude_clients: bool = True,
    chaos_exclude_gateway: bool = True,
    watchdog: bool = False,
    watchdog_timeout: int = 30,
    watchdog_count: int = 3,
):
    services = {}
    rabbitmq_healthy = {"rabbitmq": {"condition": "service_healthy"}}

    amount_depends = {
        f"filter_q5_amount_{i}": {"condition": "service_started"}
        for i in range(n_filter_amount)
    }
    if input_files is None:
        input_files = ["HI-Medium_Trans.csv"]
    for i, f in enumerate(input_files):
        services[f"client{i}"] = {
            "container_name": f"client{i}",
            "build": {
                "context": "./src",
                "dockerfile": "client/Dockerfile",
            },
            "depends_on": amount_depends,
            "environment": [
                "SERVER_HOST=gateway",
                "SERVER_PORT=5678",
                f"INPUT_FILE=/datasets/{f}",
                f"OUTPUT_FILE=/output/client{i}/output.csv",
                f"BATCH_SIZE={batch_size}",
            ],
            "volumes": [
                "./datasets:/datasets",
                "./output:/output",
            ],
        }

    services["gateway"] = {
        "build": {
            "context": "./src/",
            "dockerfile": "gateway/Dockerfile",
        },
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
            f"BATCH_SIZE={batch_size}",
            "CONTAINER_NAME=gateway",
        ],
    }

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
                f"BATCH_SIZE={batch_size}",
                f"CONTAINER_NAME=filter_q5_fmt_{i}",
            ],
        }

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
            "depends_on": {
                **rabbitmq_healthy,
                **fmt_depends,
            },
            "environment": [
                "MOM_HOST=rabbitmq",
                "INPUT_QUEUE=converter_queue",
                "OUTPUT_QUEUE=filter_q5_amount_queue",
                f"CONVERTER_AMOUNT={n_converter}",
                "FRANKFURTER_BASE=https://api.frankfurter.dev/v2",
                f"BATCH_SIZE={batch_size}",
                f"CONTAINER_NAME=converter_{i}",
            ],
        }

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
            "depends_on": {
                **rabbitmq_healthy,
                **converter_depends,
            },
            "environment": [
                "QUERY_NUMBER=5",
                "ADD_QUERY_ID=True",
                "MOM_HOST=rabbitmq",
                "INPUT_QUEUE=filter_q5_amount_queue",
                "OUTPUT_QUEUES=results_queue",
                f"FILTER_AMOUNT={n_filter_amount}",
                "MAX_AMOUNT=1.0",
                f"BATCH_SIZE={batch_size}",
                f"CONTAINER_NAME=filter_q5_amount_{i}",
            ],
        }

    services["rabbitmq"] = {
        "build": {
            "context": "./src/",
            "dockerfile": "rabbitmq/Dockerfile",
        },
        "container_name": "rabbitmq",
        "environment": [
            "RABBITMQ_LOG_LEVELS=error",
        ],
        "healthcheck": {
            "interval": "5s",
            "retries": 10,
            "start_period": "50s",
            "test": "rabbitmq-diagnostics check_port_connectivity",
            "timeout": "3s",
        },
        "ports": [
            "5672:5672",
            "15672:15672",
        ],
    }

    if watchdog:
        for svc in services.values():
            env = svc.get("environment", [])
            if any(e.startswith("CONTAINER_NAME=") for e in env):
                env.append(f"WATCHDOG_COUNT={watchdog_count}")

    if chaos_monkey:
        excluded = ["rabbitmq", "chaos_monkey"]
        if chaos_exclude_gateway:
            excluded.append("gateway")
        if chaos_exclude_clients:
            excluded.extend(f"client{i}" for i in range(len(input_files)))
        services["chaos_monkey"] = {
            "build": {"context": "./src/", "dockerfile": "chaos_monkey/Dockerfile"},
            "container_name": "chaos_monkey",
            "volumes": ["/var/run/docker.sock:/var/run/docker.sock"],
            "environment": [
                f"KILL_INTERVAL={chaos_kill_interval}",
                f"EXCLUDE_CONTAINERS={','.join(excluded)}",
            ],
            "depends_on": dict(rabbitmq_healthy),
        }

    if watchdog:
        for i in range(watchdog_count):
            services[f"watchdog_{i}"] = {
                "build": {"context": "./src/", "dockerfile": "watchdog/Dockerfile"},
                "container_name": f"watchdog_{i}",
                "volumes": ["/var/run/docker.sock:/var/run/docker.sock"],
                "environment": [
                    "MOM_HOST=rabbitmq",
                    f"HEARTBEAT_TIMEOUT={watchdog_timeout}",
                    f"WATCHDOG_ID={i}",
                    f"WATCHDOG_COUNT={watchdog_count}",
                    "WATCHDOG_HEARTBEAT_INTERVAL=3",
                    f"WATCHDOG_TIMEOUT={watchdog_timeout}",
                    "REVIVE_INTERVAL=5",
                    "WORKER_EXCHANGE=heartbeat_exchange",
                    "PEER_EXCHANGE=watchdog_exchange",
                ],
                "depends_on": dict(rabbitmq_healthy),
            }

    return {"services": services}


def main():
    parser = argparse.ArgumentParser(description="Generate docker-compose-q5.yaml")

    parser.add_argument("--filter-fmt", type=int, default=7)
    parser.add_argument("--converter", type=int, default=3)
    parser.add_argument("--filter-amount", type=int, default=2)

    parser.add_argument(
        "--input-files",
        nargs="+",
        default=["HI-Medium_Trans.csv"],
    )

    parser.add_argument(
        "--output",
        type=str,
        default="docker-compose-q5.yaml",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
    )

    parser.add_argument("--chaos-monkey", action="store_true", default=False)
    parser.add_argument("--chaos-kill-interval", type=int, default=30)
    parser.add_argument("--watchdog", action="store_true", default=False)
    parser.add_argument("--watchdog-timeout", type=int, default=30)
    parser.add_argument("--watchdog-count", type=int, default=3)

    args = parser.parse_args()

    compose = generate_compose(
        n_filter_fmt=args.filter_fmt,
        n_converter=args.converter,
        n_filter_amount=args.filter_amount,
        input_files=args.input_files,
        batch_size=args.batch_size,
        chaos_monkey=args.chaos_monkey,
        chaos_kill_interval=args.chaos_kill_interval,
        watchdog=args.watchdog,
        watchdog_timeout=args.watchdog_timeout,
        watchdog_count=args.watchdog_count,
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
    print(f"  batch_size:       {args.batch_size}")


if __name__ == "__main__":
    main()
