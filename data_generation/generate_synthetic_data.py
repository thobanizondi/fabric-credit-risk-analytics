"""
Synthetic Credit Risk Data Generator
Generates realistic (but messy) South African banking data for the
fabric-credit-risk-analytics project.

Outputs 4 CSVs: customers.csv, loans.csv, repayments.csv, defaults.csv
Intentionally injects nulls, duplicates, and inconsistent date formats
so the Silver layer cleaning step has real work to do.
"""

import random
import uuid
from datetime import datetime, timedelta
import pandas as pd
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

SA_PROVINCES = [
    "Gauteng", "Western Cape", "KwaZulu-Natal", "Eastern Cape",
    "Free State", "Limpopo", "Mpumalanga", "North West", "Northern Cape"
]

EMPLOYMENT_STATUS = ["Employed", "Self-Employed", "Unemployed", "Retired"]
LOAN_TYPES = ["Personal", "Home", "Vehicle"]

N_CUSTOMERS = 3000
N_LOANS = 4200          # some customers have >1 loan
N_MISSING_RATE = 0.03   # 3% null injection rate
N_DUPLICATE_CUSTOMERS = 25


def random_date_messy(start_year=1960, end_year=2005):
    """Return a date string in one of several inconsistent formats."""
    d = fake.date_between(start_date=f"-{2026-start_year}y", end_date=f"-{2026-end_year}y")
    fmt = random.choice(["%Y-%m-%d", "%d/%m/%Y", "%m-%d-%Y", "%d %b %Y"])
    return d.strftime(fmt)


def maybe_null(value, rate=N_MISSING_RATE):
    return None if random.random() < rate else value


# ---------------------------------------------------------------
# 1. CUSTOMERS
# ---------------------------------------------------------------
customers = []
for i in range(1, N_CUSTOMERS + 1):
    cust_id = f"CUST{i:06d}"
    customers.append({
        "CustomerID": cust_id,
        "FirstName": fake.first_name(),
        "LastName": fake.last_name(),
        "DOB": random_date_messy(1955, 2003),
        "Province": maybe_null(random.choice(SA_PROVINCES)),
        "MonthlyIncome": maybe_null(round(random.uniform(4500, 85000), 2)),
        "EmploymentStatus": random.choice(EMPLOYMENT_STATUS),
        "CreditScore": maybe_null(random.randint(300, 850)),
    })

# Inject duplicate customer rows (same CustomerID re-inserted with minor diffs)
for _ in range(N_DUPLICATE_CUSTOMERS):
    dup = random.choice(customers).copy()
    dup["MonthlyIncome"] = round(random.uniform(4500, 85000), 2)  # slightly different
    customers.append(dup)

df_customers = pd.DataFrame(customers)

# ---------------------------------------------------------------
# 2. LOANS
# ---------------------------------------------------------------
loans = []
customer_ids = df_customers["CustomerID"].unique().tolist()

for i in range(1, N_LOANS + 1):
    loan_id = f"LOAN{i:06d}"
    loan_type = random.choice(LOAN_TYPES)
    amount = {
        "Personal": random.uniform(5000, 150000),
        "Home": random.uniform(300000, 2000000),
        "Vehicle": random.uniform(80000, 600000),
    }[loan_type]
    term = random.choice([12, 24, 36, 48, 60, 120, 240] if loan_type != "Home" else [120, 180, 240, 360])
    disb_date = fake.date_between(start_date="-5y", end_date="-30d")

    loans.append({
        "LoanID": loan_id,
        "CustomerID": random.choice(customer_ids),
        "LoanType": loan_type,
        "Amount": round(amount, 2),
        "InterestRate": round(random.uniform(7.5, 24.9), 2),
        "TermMonths": term,
        "DisbursementDate": disb_date.strftime(random.choice(["%Y-%m-%d", "%d/%m/%Y"])),
    })

df_loans = pd.DataFrame(loans)

# ---------------------------------------------------------------
# 3. REPAYMENTS (monthly schedule per loan, with realistic late/missed behaviour)
# ---------------------------------------------------------------
repayments = []
repayment_counter = 1

for _, loan in df_loans.iterrows():
    # parse disbursement date back out regardless of format
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            disb = datetime.strptime(loan["DisbursementDate"], fmt)
            break
        except ValueError:
            continue

    monthly_installment = round(loan["Amount"] / loan["TermMonths"] * (1 + loan["InterestRate"] / 100 / 12), 2)

    # simulate risk: lower credit score customers -> more missed/late payments
    cust_row = df_customers[df_customers["CustomerID"] == loan["CustomerID"]]
    credit_score = cust_row["CreditScore"].values[0] if len(cust_row) else 600
    credit_score = credit_score if pd.notna(credit_score) else 600
    risk_factor = max(0.03, (750 - credit_score) / 550)  # higher risk if lower score

    months_elapsed = min(loan["TermMonths"], random.randint(1, 36))

    for m in range(months_elapsed):
        due_date = disb + timedelta(days=30 * (m + 1))
        status_roll = random.random()

        if status_roll < risk_factor * 0.4:
            status = "Missed"
            paid_date = None
            amount_paid = 0.0
        elif status_roll < risk_factor:
            status = "Late"
            paid_date = due_date + timedelta(days=random.randint(1, 25))
            amount_paid = monthly_installment
        else:
            status = "OnTime"
            paid_date = due_date - timedelta(days=random.randint(0, 3))
            amount_paid = monthly_installment

        repayments.append({
            "RepaymentID": f"REP{repayment_counter:07d}",
            "LoanID": loan["LoanID"],
            "DueDate": due_date.strftime(random.choice(["%Y-%m-%d", "%d/%m/%Y"])),
            "PaidDate": paid_date.strftime("%Y-%m-%d") if paid_date else None,
            "AmountDue": monthly_installment,
            "AmountPaid": maybe_null(amount_paid, rate=0.01),
            "Status": status,
        })
        repayment_counter += 1

df_repayments = pd.DataFrame(repayments)

# ---------------------------------------------------------------
# 4. DEFAULTS (derived: 3+ consecutive missed payments = default)
# ---------------------------------------------------------------
defaults = []
for loan_id, group in df_repayments.groupby("LoanID"):
    group_sorted = group.sort_values("DueDate")
    consecutive_missed = 0
    max_consecutive = 0
    for status in group_sorted["Status"]:
        if status == "Missed":
            consecutive_missed += 1
            max_consecutive = max(max_consecutive, consecutive_missed)
        else:
            consecutive_missed = 0

    default_flag = "Yes" if max_consecutive >= 3 else "No"
    default_date = None
    if default_flag == "Yes":
        missed_rows = group_sorted[group_sorted["Status"] == "Missed"]
        if len(missed_rows) >= 3:
            default_date = missed_rows.iloc[2]["DueDate"]

    defaults.append({
        "LoanID": loan_id,
        "DefaultFlag": default_flag,
        "DefaultDate": default_date,
    })

df_defaults = pd.DataFrame(defaults)

# ---------------------------------------------------------------
# SAVE
# ---------------------------------------------------------------
df_customers.to_csv("customers.csv", index=False)
df_loans.to_csv("loans.csv", index=False)
df_repayments.to_csv("repayments.csv", index=False)
df_defaults.to_csv("defaults.csv", index=False)

print("Generated files:")
print(f"  customers.csv   -> {len(df_customers):,} rows")
print(f"  loans.csv       -> {len(df_loans):,} rows")
print(f"  repayments.csv  -> {len(df_repayments):,} rows")
print(f"  defaults.csv    -> {len(df_defaults):,} rows")
print(f"\nDefault rate: {(df_defaults['DefaultFlag']=='Yes').mean()*100:.1f}%")
