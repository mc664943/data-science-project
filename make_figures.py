"""Generate the two portfolio figures from results/summary.csv.

Run after project_01_template.py (or use the summary.csv shipped in the repo):
    python make_figures.py
Outputs:
    figures/fig1_issue_counts.png
    figures/fig2_flagged_share.png
"""
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

BASE = Path(__file__).resolve().parent
summary = pd.read_csv(BASE / "results" / "summary.csv").set_index("metric")["value"]

issues = pd.DataFrame(
    {
        "issue": ["Excess refund", "Refund without payment",
                  "Duplicate creation report", "Payment after cancellation"],
        "count": [summary["excess_refund"], summary["refund_no_payment"],
                  summary["dup_creation"], summary["pay_after_cancel"]],
    }
).sort_values("count")

# --- Figure 1: business-logic issue counts -------------------------------
fig, ax = plt.subplots(figsize=(8, 4.2))
bars = ax.barh(issues["issue"], issues["count"], color="#4472C4")
ax.bar_label(bars, fmt="{:,.0f}", padding=4)
ax.set_title("Business-logic issues found in 9.89 million order events")
ax.set_xlabel("Number of problematic events")
ax.spines[["top", "right"]].set_visible(False)
ax.margins(x=0.12)
fig.tight_layout()
fig.savefig(BASE / "figures" / "fig1_issue_counts.png", dpi=150)
plt.close(fig)

# --- Figure 2: flagged share of the event log ----------------------------
flagged = int(summary["flagged_unique_events"])
total = int(summary["total_rows"])
fig, ax = plt.subplots(figsize=(6, 4.6))
ax.pie([flagged, total - flagged],
       labels=["Flagged as problematic", "Passed all checks"],
       colors=["#C0504D", "#9BBB59"],
       autopct=lambda p: f"{p:.1f}%\n({p * total / 100:,.0f})",
       startangle=90, counterclock=False, wedgeprops={"width": 0.42})
ax.set_title(f"Share of flagged events among {total:,} rows")
fig.tight_layout()
fig.savefig(BASE / "figures" / "fig2_flagged_share.png", dpi=150)
plt.close(fig)

print("Saved figures/fig1_issue_counts.png and figures/fig2_flagged_share.png")
