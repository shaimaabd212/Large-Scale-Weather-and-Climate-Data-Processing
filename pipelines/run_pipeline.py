#!/usr/bin/env python3
"""
Pipeline driver. Normally launched by scripts/run_pipeline.sh, which picks the
right --base and spark-submit options. Direct use:

  # local laptop (pip install pyspark), synthetic sample data
  python3 pipelines/run_pipeline.py --local --base file://$PWD/data/sample

  # on the cluster (master node)
  spark-submit --py-files build/weather_pipeline.zip pipelines/run_pipeline.py \
      --base hdfs://bd-master:9000/weather --stage all

Stages (run in this order):  ingest -> clean -> aggregate -> extremes -> spatial -> export
"""
import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from weather_pipeline import aggregate, clean, export, extremes, ingest, spatial  # noqa: E402
from weather_pipeline.config import Paths, load_config  # noqa: E402
from weather_pipeline.spark_session import cluster_info, get_spark  # noqa: E402

STAGES = {
    "ingest": ingest.run,
    "clean": clean.run,
    "aggregate": aggregate.run,
    "extremes": extremes.run,
    "spatial": spatial.run,
    "export": export.run,
}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base", required=True, help="Data root, e.g. hdfs://bd-master:9000/weather")
    ap.add_argument("--stage", default="all",
                    help="all | comma list of: " + ", ".join(STAGES))
    ap.add_argument("--config", default=str(REPO / "config" / "pipeline.json"))
    ap.add_argument("--local", action="store_true", help="Run with local[*] instead of the cluster")
    ap.add_argument("--count", action="store_true", help="Also count rows per stage (slower; for the report)")
    ap.add_argument("--tag", default="", help="Label stored in the metrics file (e.g. '1-worker')")
    args = ap.parse_args()

    stages = list(STAGES) if args.stage == "all" else [s.strip() for s in args.stage.split(",")]
    unknown = [s for s in stages if s not in STAGES]
    if unknown:
        ap.error(f"unknown stage(s): {unknown}")

    cfg = load_config(args.config)
    paths = Paths(args.base)
    spark = get_spark(local=args.local)

    run_log = {"started": datetime.now().isoformat(timespec="seconds"), "tag": args.tag,
               "base": args.base, "cluster": cluster_info(spark), "stages": {}}
    print(f"\n=== Weather pipeline | {run_log['cluster']} ===\n")

    total = time.time()
    for name in stages:
        metrics = {"_count": args.count}
        t0 = time.time()
        print(f"--> {name} ...", flush=True)
        STAGES[name](spark, paths, cfg, metrics)
        metrics.pop("_count")
        metrics["seconds"] = round(time.time() - t0, 1)
        run_log["stages"][name] = metrics
        print(f"    {name} done in {metrics['seconds']} s  {json.dumps({k: v for k, v in metrics.items() if k != 'seconds'})}")
    run_log["total_seconds"] = round(time.time() - total, 1)

    log_dir = REPO / "logs"
    log_dir.mkdir(exist_ok=True)
    out = log_dir / f"run_{datetime.now():%Y%m%d_%H%M%S}.json"
    out.write_text(json.dumps(run_log, indent=2, default=str))
    print(f"\n=== Finished in {run_log['total_seconds']} s — metrics saved to {out} ===")
    spark.stop()


if __name__ == "__main__":
    main()
