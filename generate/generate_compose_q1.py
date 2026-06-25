import yaml
import argparse


def generate_compose_q1(
    input_files: list,
    n_filter_usd: int = 3,
    n_filter_amount: int = 3,
    batch_size: int = 10000,
    chaos_monkey: bool = False,
    chaos_kill_interval: int = 30,
    watchdog: bool = False,
    watchdog_timeout: int = 30,
    watchdog_count: int = 3,
):
    services = {}
    rabbitmq_healthy = {"rabbitmq": {"condition": "service_healthy"}}

    for i in range(n_filter_amount):
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
                f"FILTER_AMOUNT={n_filter_amount}",
                "QUERY_NUMBER=1",
                "MOM_HOST=rabbitmq",
                "INPUT_QUEUE=q1_filter_amount",
                "OUTPUT_QUEUES=results_queue",
                "MAX_AMOUNT=50",
                "ADD_QUERY_ID=True",
                f"BATCH_SIZE={batch_size}",
                f"CONTAINER_NAME=q1_filter_amount_{i}",
            ],
        }

    filter_amount_depends = {
        f"q1_filter_amount_{i}": {"condition": "service_started"}
        for i in range(n_filter_amount)
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
                **filter_amount_depends,
            },
            "environment": [
                f"FILTER_AMOUNT={n_filter_usd}",
                "QUERY_NUMBER=1",
                "INPUT_EXCHANGE_NAME=input_gateway_exchange",
                "INPUT_ROUTING_KEYS=filter_usd",
                "MOM_HOST=rabbitmq",
                "OUTPUT_QUEUES=q1_filter_amount",
                "USD_ONLY=True",
                f"BATCH_SIZE={batch_size}",
                f"CONTAINER_NAME=filter_usd_{i}",
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
            "NUM_EXPECTED_EOFS=1",
            "PYTHONUNBUFFERED=1",
            "SERVER_HOST=gateway",
            "SERVER_PORT=5678",
            "CONTAINER_NAME=gateway",
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

    if watchdog:
        for svc in services.values():
            env = svc.get("environment", [])
            if any(e.startswith("CONTAINER_NAME=") for e in env):
                env.append(f"WATCHDOG_COUNT={watchdog_count}")

    if chaos_monkey:
        client_names = ",".join(f"client{i}" for i in range(len(input_files)))
        services["chaos_monkey"] = {
            "build": {"context": "./src/", "dockerfile": "chaos_monkey/Dockerfile"},
            "container_name": "chaos_monkey",
            "volumes": ["/var/run/docker.sock:/var/run/docker.sock"],
            "environment": [
                f"KILL_INTERVAL={chaos_kill_interval}",
                f"EXCLUDE_CONTAINERS=rabbitmq,chaos_monkey,gateway,{client_names}",
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
    parser = argparse.ArgumentParser(description="Generate docker-compose-q1.yaml")
    parser.add_argument("--input-files", nargs="+", default=["HI-Medium_Trans.csv"])
    parser.add_argument("--output", type=str, default="docker-compose-q1.yaml")
    parser.add_argument("--batch-size", type=int, default=10000)
    parser.add_argument("--filter-usd", type=int, default=3)
    parser.add_argument("--filter-amount", type=int, default=3)
    parser.add_argument("--chaos-monkey", action="store_true", default=False)
    parser.add_argument("--chaos-kill-interval", type=int, default=30)
    parser.add_argument("--watchdog", action="store_true", default=False)
    parser.add_argument("--watchdog-timeout", type=int, default=30)
    parser.add_argument("--watchdog-count", type=int, default=3)
    args = parser.parse_args()

    compose = generate_compose_q1(
        input_files=args.input_files,
        n_filter_usd=args.filter_usd,
        n_filter_amount=args.filter_amount,
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
    print(f"  input_files:    {args.input_files}")
    print(f"  filter_usd:     {args.filter_usd}")
    print(f"  filter_amount:  {args.filter_amount}")
    print(f"  batch_size:     {args.batch_size}")


if __name__ == "__main__":
    main()
