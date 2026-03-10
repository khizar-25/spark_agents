#!/usr/bin/env python3
# main.py — Spark Agent Entry Point
import argparse, json, logging, sys, os
from pathlib import Path

# Load .env automatically
env_file = Path(".env")
if env_file.exists():
    for line in env_file.read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

def build_parser():
    p = argparse.ArgumentParser(prog="spark-agent", description="⚡ Autonomous Spark Pipeline Agent")
    sub = p.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="Run a Spark pipeline agent")
    run_p.add_argument("--goal",          required=True)
    run_p.add_argument("--source",        required=True)
    run_p.add_argument("--target",        required=True)
    run_p.add_argument("--input-format",  default="csv",   choices=["parquet","delta","json","csv","orc","jdbc"])
    run_p.add_argument("--output-format", default="delta", choices=["parquet","delta","json","csv","orc"])
    run_p.add_argument("--mode",          default="append",choices=["append","overwrite","merge","upsert"])
    run_p.add_argument("--partition-by",  nargs="+",       default=["date"], metavar="COL")
    run_p.add_argument("--enable-dq",     action="store_true", default=True)
    run_p.add_argument("--output-json",   help="Save result JSON to file")

    pre_p = sub.add_parser("preset", help="Run with a pre-built template")
    pre_p.add_argument("--type",   required=True, choices=["etl","streaming","dq"])
    pre_p.add_argument("--source", required=True)
    pre_p.add_argument("--target", required=True)
    pre_p.add_argument("--output-json")

    srv_p = sub.add_parser("serve", help="Start FastAPI server")
    srv_p.add_argument("--host",   default="0.0.0.0")
    srv_p.add_argument("--port",   type=int, default=8000)
    srv_p.add_argument("--reload", action="store_true")

    sub.add_parser("setup", help="Run the auto-setup wizard")
    return p

def handle_run(args):
    from config.goal_state import SparkGoalState, PipelineGoalType
    goal = SparkGoalState(
        goal_type=PipelineGoalType.ETL_BATCH,
        description=args.goal,
        source_path=args.source, target_path=args.target,
        input_format=args.input_format, output_format=args.output_format,
        write_mode=args.mode, partition_cols=args.partition_by,
        transformations=["deduplicate","cast_types","handle_nulls","add_audit_cols"],
        dq_rules={"null_threshold":0.01,"dupe_check":True} if args.enable_dq else {},
    )
    return _run_and_report(goal, getattr(args,"output_json",None))

def handle_preset(args):
    from config.goal_state import etl_goal, streaming_goal, dq_goal
    presets = {
        "etl":       etl_goal(args.source, args.target),
        "streaming": streaming_goal("kafka://localhost:9092","events",args.target),
        "dq":        dq_goal(args.source, args.target),
    }
    return _run_and_report(presets[args.type], getattr(args,"output_json",None))

def handle_serve(args):
    import uvicorn
    uvicorn.run("api.app:app", host=args.host, port=args.port, reload=args.reload)

def _run_and_report(goal, output_json=None):
    from agent.agent import SparkAgent

    print(f"""
╔══════════════════════════════════════════════════════════════╗
║              ⚡  SPARK AGENT  ⚡                              ║
╚══════════════════════════════════════════════════════════════╝
  Goal   : {goal.description}
  Source : {goal.source_path}
  Target : {goal.target_path}
  Format : {goal.input_format} → {goal.output_format}
  DQ     : {"Enabled" if goal.dq_rules else "Disabled"}
{'═'*64}
""")

    agent = SparkAgent()
    result = agent.run(goal)

    status_icon = "✅" if result.success else "❌"
    status_text = "SUCCESS" if result.success else "FAILED"

    print(f"""
╔══════════════════════════════════════════════════════════════╗
║  {status_icon}  {status_text:<56}║
╠══════════════════════════════════════════════════════════════╣
║  Iterations   : {result.iterations_used:<2} / 15{' '*38}║
║  Completion   : {result.final_completion_score:.0%}{' '*41}║
║  Duration     : {result.metrics.get('elapsed_str','N/A'):<44}║
║  Rows Written : {str(result.metrics.get('rows_written',0)):>12}{' '*31}║
║  DQ Score     : {result.metrics.get('dq_score',0):.1f}%{' '*41}║
║  Transforms   : {', '.join(result.metrics.get('transformations',[]) or ['none'])[:44]:<44}║""")

    if result.error:
        print(f"║  Error        : {result.error[:44]:<44}║")

    print("╚══════════════════════════════════════════════════════════════╝\n")

    if result.action_history:
        print("📋 Action History:")
        for h in result.action_history:
            icon = "✅" if h["result_status"] == "success" else "❌"
            print(f"   {icon} [{h['iteration']:02d}] {h['tool']:<30} → {h['result_status']}")
        print()

    if output_json:
        with open(output_json, "w") as f:
            json.dump(result.to_dict(), f, indent=2, default=str)
        print(f"💾 Result saved to: {output_json}")

    return 0 if result.success else 1

def main():
    parser = build_parser()
    args   = parser.parse_args()

    if args.command == "setup":
        import subprocess
        subprocess.run([sys.executable, "setup.py"])
    elif args.command == "run":
        sys.exit(handle_run(args))
    elif args.command == "preset":
        sys.exit(handle_preset(args))
    elif args.command == "serve":
        handle_serve(args)

if __name__ == "__main__":
    main()
