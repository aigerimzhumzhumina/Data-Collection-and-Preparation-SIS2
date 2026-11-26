"""Run the whole pipeline:  python main.py        (or one stage:  python main.py --stage validate)

Stages (in order): collect -> produce -> consume -> validate -> clean -> load -> analyze -> verify
The Kafka producer and consumer are also separate scripts:  python -m src.producer  /  python -m src.consumer
"""
from __future__ import annotations

import argparse
import logging
import sys
import time

from src import analysis, api_client, cleaner, consumer, loader, producer, validator, verification
from src.kafka_utils import BrokerUnavailable
from src.report import reset_report

STAGES = ["collect", "produce", "consume", "validate", "clean", "load", "analyze", "verify"]


def run_stage(name: str, args) -> None:
    if name == "collect":
        api_client.collect()
    elif name == "produce":
        producer.run(include_test_records=not args.no_test_records)
    elif name == "consume":
        consumer.run()
    elif name == "validate":
        validator.run()
    elif name == "clean":
        cleaner.run()
    elif name == "load":
        loader.run()
    elif name == "analyze":
        analysis.run()
    elif name == "verify":
        if not verification.run():
            raise RuntimeError("pipeline verification failed")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Volume vs volatility pipeline (FMP -> Kafka -> Pandera -> pandas -> SQLite -> EDA)")
    parser.add_argument("--stage", choices=STAGES + ["all"], default="all", help="run a single stage (default: all)")
    parser.add_argument("--skip-collect", action="store_true", help="reuse data/raw_data.csv instead of calling the API again")
    parser.add_argument("--no-test-records", action="store_true", help="do not inject the 3 invalid test records")
    args = parser.parse_args()

    stages = STAGES if args.stage == "all" else [args.stage]
    if args.stage == "all":
        reset_report()  
        if args.skip_collect:
            stages = [s for s in stages if s != "collect"]
            import pandas as pd
            from src.config import SETTINGS
            from src.report import update_stage
            update_stage("collect", {"total_records": len(pd.read_csv(SETTINGS.raw_csv)), "note": "reused existing raw_data.csv"})

    for name in stages:
        print(f"\n{'=' * 70}\n>>> STAGE: {name}\n{'=' * 70}")
        started = time.time()
        try:
            run_stage(name, args)
        except BrokerUnavailable as exc:
            logging.error("%s", exc)
            return 2
        except Exception as exc:  
            logging.error("Stage '%s' failed: %s", name, exc)
            return 1
        print(f"[{name}] finished in {time.time() - started:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
