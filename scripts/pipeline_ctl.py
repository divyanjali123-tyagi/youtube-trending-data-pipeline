"""Deploy, run and tear down the pipeline in your AWS account (works on Windows/macOS/Linux).

    python scripts/pipeline_ctl.py deploy --api-key YOUR_YOUTUBE_KEY [--alert-email you@example.com]
    python scripts/pipeline_ctl.py run            # start a pipeline execution and wait for it
    python scripts/pipeline_ctl.py status         # show recent executions
    python scripts/pipeline_ctl.py destroy        # delete the stack and the data lake bucket

Requirements: AWS CLI credentials configured (``aws configure``), AWS SAM CLI and boto3.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "infrastructure" / "template.yaml"
GLUE_SCRIPTS = sorted((ROOT / "src" / "glue").glob("*.py"))
DEFAULT_STACK = "youtube-trending-pipeline"
DEFAULT_PARAM = "/youtube-trending/api-key"


def _boto(service: str, region: str | None):
    try:
        import boto3
    except ImportError:
        sys.exit("boto3 is required: pip install boto3")
    return boto3.client(service, region_name=region)


def stack_outputs(stack: str, region: str | None) -> dict[str, str]:
    cfn = _boto("cloudformation", region)
    try:
        desc = cfn.describe_stacks(StackName=stack)["Stacks"][0]
    except Exception as exc:  # noqa: BLE001
        sys.exit(f"Stack {stack!r} not found ({exc}). Run the deploy command first.")
    return {o["OutputKey"]: o["OutputValue"] for o in desc.get("Outputs", [])}


# ---------------------------------------------------------------------- deploy


def ensure_api_key(param: str, api_key: str | None, region: str | None) -> None:
    ssm = _boto("ssm", region)
    if api_key:
        ssm.put_parameter(Name=param, Value=api_key, Type="SecureString", Overwrite=True)
        print(f"Stored YouTube API key in SSM SecureString {param}")
        return
    try:
        ssm.get_parameter(Name=param)
        print(f"Using existing YouTube API key from SSM {param}")
    except ssm.exceptions.ParameterNotFound:
        sys.exit(f"SSM parameter {param} not found. Re-run with --api-key YOUR_KEY.")


def upload_glue_scripts(bucket: str, region: str | None) -> None:
    s3 = _boto("s3", region)
    for script in GLUE_SCRIPTS:
        key = f"artifacts/glue/{script.name}"
        s3.upload_file(str(script), bucket, key)
        print(f"Uploaded {script.name} -> s3://{bucket}/{key}")


def deploy(args: argparse.Namespace) -> None:
    sam = shutil.which("sam")
    if not sam:
        sys.exit(
            "AWS SAM CLI not found: https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html"
        )

    identity = _boto("sts", args.region).get_caller_identity()
    print(f"Deploying to account {identity['Account']} as {identity['Arn']}")
    ensure_api_key(args.api_key_parameter, args.api_key, args.region)

    overrides = [f"ApiKeyParameterName={args.api_key_parameter}", f"ScheduleState={args.schedule}"]
    if args.alert_email:
        overrides.append(f"AlertEmail={args.alert_email}")

    cmd = [
        sam,
        "deploy",
        "--template-file",
        str(TEMPLATE),
        "--stack-name",
        args.stack_name,
        "--resolve-s3",
        "--capabilities",
        "CAPABILITY_IAM",
        "--no-confirm-changeset",
        "--no-fail-on-empty-changeset",
        "--parameter-overrides",
        *overrides,
    ]
    if args.region:
        cmd += ["--region", args.region]
    print("Running:", " ".join(cmd[1:]))
    subprocess.run(cmd, check=True, cwd=ROOT)

    outputs = stack_outputs(args.stack_name, args.region)
    upload_glue_scripts(outputs["DataLakeBucketName"], args.region)
    print("\nDeployed. Stack outputs:")
    for key, value in outputs.items():
        print(f"  {key:<22} {value}")
    if args.alert_email:
        print(f"\nCheck {args.alert_email} and confirm the SNS subscription to receive alerts.")
    if args.run_now:
        run(args)


# ------------------------------------------------------------------------- run


def run(args: argparse.Namespace) -> None:
    outputs = stack_outputs(args.stack_name, args.region)
    sfn = _boto("stepfunctions", args.region)
    execution = sfn.start_execution(stateMachineArn=outputs["StateMachineArn"])
    arn = execution["executionArn"]
    print(f"Started execution {arn.rsplit(':', 1)[-1]}")
    if args.no_wait:
        return
    last_state = None
    while True:
        desc = sfn.describe_execution(executionArn=arn)
        if desc["status"] != "RUNNING":
            break
        history = sfn.get_execution_history(executionArn=arn, reverseOrder=True, maxResults=20)["events"]
        entered = next((e for e in history if e["type"].endswith("StateEntered")), None)
        state = entered["stateEnteredEventDetails"]["name"] if entered else "starting"
        if state != last_state:
            print(f"  -> {state}")
            last_state = state
        time.sleep(10)
    print(f"Execution finished: {desc['status']}")
    if desc["status"] == "SUCCEEDED":
        print(
            f"Query the results in Athena: workgroup {outputs['AthenaWorkGroup']}, "
            f"database {outputs['GlueDatabase']} (saved queries are under 'Saved queries')."
        )
    else:
        print(desc.get("cause") or desc.get("error") or "See the Step Functions console for details.")
        sys.exit(1)


def status(args: argparse.Namespace) -> None:
    outputs = stack_outputs(args.stack_name, args.region)
    sfn = _boto("stepfunctions", args.region)
    runs = sfn.list_executions(stateMachineArn=outputs["StateMachineArn"], maxResults=10)["executions"]
    if not runs:
        print("No executions yet.")
    for r in runs:
        print(f"{r['startDate']:%Y-%m-%d %H:%M}  {r['status']:<10} {r['name']}")


# --------------------------------------------------------------------- destroy


def destroy(args: argparse.Namespace) -> None:
    outputs = stack_outputs(args.stack_name, args.region)
    bucket = outputs.get("DataLakeBucketName")
    if not args.yes:
        answer = input(f"Delete stack {args.stack_name} and ALL data in s3://{bucket}? [y/N] ")
        if answer.strip().lower() != "y":
            print("Aborted.")
            return
    cfn = _boto("cloudformation", args.region)
    cfn.delete_stack(StackName=args.stack_name)
    print("Deleting stack...")
    cfn.get_waiter("stack_delete_complete").wait(StackName=args.stack_name)
    if bucket:
        import boto3

        b = boto3.resource("s3", region_name=args.region).Bucket(bucket)
        b.object_versions.delete()
        b.delete()
        print(f"Deleted bucket {bucket}")
    print("Done.")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stack-name", default=DEFAULT_STACK)
    parser.add_argument("--region", help="AWS region (default: your AWS CLI default)")
    sub = parser.add_subparsers(dest="command", required=True)

    p_deploy = sub.add_parser("deploy", help="Deploy/update the stack and upload Glue scripts")
    p_deploy.add_argument("--api-key", help="YouTube Data API key to store in SSM (first deploy)")
    p_deploy.add_argument("--api-key-parameter", default=DEFAULT_PARAM)
    p_deploy.add_argument("--alert-email", help="E-mail address for failure/data quality alerts")
    p_deploy.add_argument("--schedule", choices=["ENABLED", "DISABLED"], default="ENABLED")
    p_deploy.add_argument("--run-now", action="store_true", help="Start a pipeline run after deploying")
    p_deploy.add_argument("--no-wait", action="store_true")
    p_deploy.set_defaults(func=deploy)

    p_run = sub.add_parser("run", help="Start a pipeline execution")
    p_run.add_argument("--no-wait", action="store_true")
    p_run.set_defaults(func=run)

    sub.add_parser("status", help="List recent executions").set_defaults(func=status)

    p_destroy = sub.add_parser("destroy", help="Delete the stack and the data lake bucket")
    p_destroy.add_argument("--yes", action="store_true", help="Skip confirmation")
    p_destroy.set_defaults(func=destroy)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
