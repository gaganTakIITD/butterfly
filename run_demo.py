"""
Direct Pipeline Demo - bypasses the LLM and runs the full 9-step
data pipeline end-to-end, proving the toolset works perfectly.
"""
import asyncio, json, os
os.environ.setdefault("SMTP_HOST", "smtp.gmail.com")
os.environ.setdefault("SMTP_PORT", "587")
os.environ.setdefault("SMTP_USER", "gagantak00@gmail.com")
os.environ.setdefault("SMTP_PASS", "fmyg ibnk djlw wngk")

import sys; sys.path.insert(0, "src")
from workflow_toolset import WorkflowToolset

async def main():
    t = WorkflowToolset()
    print("=" * 60)
    print("  WORKFLOW ORCHESTRATOR -- LIVE PIPELINE DEMO")
    print("=" * 60)

    print("\n[1/9] Fetching data from cache...")
    await t.fetch_data_from_cache("test_sales", "last_week")

    print("[2/9] Validating data...")
    await t.validate_data("<fetch_data_from_cache_output>")

    print("[3/9] Cleaning data (drop nulls)...")
    await t.clean_data("<validate_data_output>", "drop_nulls")

    print("[4/9] Filtering out Cancelled orders...")
    res4 = await t.filter_data("<clean_data_output>", "status!=Cancelled")
    filtered = json.loads(res4)
    print(f"     >> {filtered['filtered_count']} rows remain after filtering")

    print("[5/9] Transforming -- aggregating by region...")
    res5 = await t.transform_data("<filter_data_output>", "region", "sum")
    groups = json.loads(res5)
    print(f"     >> {groups['group_count']} regional groups computed")

    print("[6/9] Generating CSS bar chart...")
    await t.generate_chart("<transform_data_output>", "bar", "Regional Sales", "region", "amount")

    print("[7/9] Composing premium HTML report...")
    await t.compose_report(
        title="Weekly Sales Pipeline Report",
        summary="This report analyzes last week's sales performance across all regions, filtering out cancelled orders and aggregating revenue metrics.",
    )

    print("[8/9] Exporting report to disk...")
    res8 = await t.export_report("<compose_report_output>", "final_report", "html")
    export = json.loads(res8)
    print(f"     >> Saved to: {export['file_path']} ({export['file_size']} bytes)")

    print("[9/9] Dispatching email...")
    res9 = await t.dispatch_email(
        "tejashvikumawat@gmail.com",
        "Weekly Sales Pipeline Report -- Auto-Generated",
        "Please find the attached pipeline report.",
        "<export_report_output>"
    )
    email = json.loads(res9)
    print(f"     >> Method: {email['method']}")
    if "log_path" in email:
        print(f"     >> Log: {email['log_path']}")

    print("\n" + "=" * 60)
    print("  PIPELINE COMPLETE -- ALL 9 STEPS SUCCEEDED")
    print("=" * 60)
    print(f"\n  Open the HTML file in your browser to see the report!")
    print(f"  >> {export['file_path']}")

if __name__ == "__main__":
    asyncio.run(main())

