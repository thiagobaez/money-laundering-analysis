#!/usr/bin/env python3
import yaml
import argparse


def generate_compose_q3(
    input_files: list,
    n_filter_usd: int = 1,
    n_split_date: int = 1,
    n_avg: int = 1,
    n_avg_joiner: int = 1,
    batch_size: int = 100,
    chaos_monkey: bool = False,
    chaos_kill_interval: int = 30,
    watchdog: bool = False,
    watchdog_timeout: int = 30,
    watchdog_count: int = 3,
):
    services = {}
    rabbitmq_healthy = {"rabbitmq": {"condition": "service_healthy"}}

    avg_joiner_routing_keys = ",".join([f"avg_joiner_{i}" for i in range(n_avg_joiner)])
    avg_routing_keys = ",".join([f"avg_{i}" for i in range(n_avg)])

    for i, f in enumerate(input_files):
        services[f"client{i}"] = {
            "container_name": f"client{i}",
            "build": {"context": "./src", "dockerfile": "client/Dockerfile"},
            "depends_on": ["gateway"],
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
            "INPUT_QUEUE=filter_usd_queue",
            "MOM_HOST=rabbitmq",
            "OUTPUT_QUEUE=results_queue",
            "NUM_EXPECTED_EOFS=1",
            f"AVG_JOINER_AMOUNT={n_avg_joiner}",
            "PYTHONUNBUFFERED=1",
            "SERVER_HOST=gateway",
            "SERVER_PORT=5678",
            f"BATCH_SIZE={batch_size}",
            "CONTAINER_NAME=gateway",
        ],
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
            },
            "environment": [
                "QUERY_NUMBER=3",
                "MOM_HOST=rabbitmq",
                "INPUT_QUEUE=filter_usd_queue",
                "OUTPUT_QUEUES=q3_split_queue",
                f"FILTER_AMOUNT={n_filter_usd}",
                "USD_ONLY=True",
                f"BATCH_SIZE={batch_size}",
                f"CONTAINER_NAME=filter_usd_{i}",
            ],
        }

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
                f"AVG_QUEUE=avg_joiner_{i}",
                "OUTPUT_QUEUE=results_queue",
                f"AVG_JOINER_AMOUNT={n_avg_joiner}",
                f"AVG_AMOUNT={n_avg}",
                f"DATA_DIR=/data/joiner_{i}",
                f"BATCH_SIZE={batch_size}",
                f"CONTAINER_NAME=avg_joiner_{i}",
            ],
        }

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
                f"INPUT_QUEUE=avg_queue_{i}",
                f"OUTPUT_QUEUES={avg_joiner_routing_keys}",
                f"AVG_AMOUNT={n_avg}",
                f"CONTAINER_NAME=avg_{i}",
            ],
        }

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
                f"FIRST_PERIOD_QUEUES={','.join([f'avg_queue_{i}' for i in range(n_avg)])}",
                "SECOND_PERIOD_QUEUE=second_period_queue",
                "FIRST_PERIOD_GE=2022-09-01",
                "FIRST_PERIOD_LE=2022-09-05",
                "SECOND_PERIOD_GE=2022-09-06",
                "SECOND_PERIOD_LE=2022-09-14",
                f"SPLIT_AMOUNT={n_split_date}",
                f"BATCH_SIZE={batch_size}",
                f"CONTAINER_NAME=split_date_{i}",
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
    parser = argparse.ArgumentParser(description="Generate docker-compose-q3.yaml")
    parser.add_argument("--input-files", nargs="+", default=["HI-Small_Trans.csv.gz"])
    parser.add_argument("--output", type=str, default="docker-compose-q3.yaml")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--filter-usd", type=int, default=1)
    parser.add_argument("--split-date", type=int, default=1)
    parser.add_argument("--avg", type=int, default=1)
    parser.add_argument("--avg-joiner", type=int, default=1)
    parser.add_argument("--chaos-monkey", action="store_true", default=False)
    parser.add_argument("--chaos-kill-interval", type=int, default=30)
    parser.add_argument("--watchdog", action="store_true", default=False)
    parser.add_argument("--watchdog-timeout", type=int, default=30)
    parser.add_argument("--watchdog-count", type=int, default=3)
    args = parser.parse_args()

    compose = generate_compose_q3(
        input_files=args.input_files,
        n_filter_usd=args.filter_usd,
        n_split_date=args.split_date,
        n_avg=args.avg,
        n_avg_joiner=args.avg_joiner,
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
    print(f"  input_files:   {args.input_files}")
    print(f"  filter_usd:    {args.filter_usd}")
    print(f"  split_date:    {args.split_date}")
    print(f"  avg:           {args.avg}")
    print(f"  avg_joiner:    {args.avg_joiner}")
    print(f"  batch_size:    {args.batch_size}")


if __name__ == "__main__":
    main()
