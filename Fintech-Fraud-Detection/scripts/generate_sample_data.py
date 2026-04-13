"""
scripts/generate_sample_data.py
Generates large-scale synthetic transaction data for local testing.
Produces realistic UK payment data matching the TRANSACTION_SCHEMA.

FIX: Original sample_data had only 50 rows. This script generates 100,000+
     rows with realistic distributions for proper pipeline testing.

Usage:
    python scripts/generate_sample_data.py --rows 100000 --output sample_data/
    python scripts/generate_sample_data.py --rows 1000 --output sample_data/ --seed 42
"""

import argparse
import csv
import os
import random
import uuid
from datetime import datetime, timedelta


# Realistic UK payment distributions
TRANSACTION_TYPES = ["CARD", "FPS", "BACS", "CHAPS"]
TRANSACTION_TYPE_WEIGHTS = [0.65, 0.20, 0.10, 0.05]   # CARD most common

MERCHANT_CATEGORIES = [
    "GROCERY", "RETAIL", "FUEL", "RESTAURANT", "TRAVEL",
    "UTILITIES", "HEALTHCARE", "EDUCATION", "ENTERTAINMENT",
    "CRYPTO", "GAMBLING", "WIRE_TRANSFER", "MONEY_TRANSFER",  # high-risk
    "PREPAID_CARDS", "JEWELLERY",
]
MERCHANT_WEIGHTS = [
    0.20, 0.18, 0.10, 0.12, 0.08,
    0.07, 0.05, 0.04, 0.06,
    0.02, 0.02, 0.02, 0.02,
    0.01, 0.01,
]

UK_REGIONS = ["SW", "NW", "SE", "NE", "YH", "EM", "WM", "EA", "LN"]

HIGH_RISK_COUNTRIES = ["KP", "IR", "MM", "RU", "SY"]
NORMAL_COUNTRIES = ["GB", "US", "DE", "FR", "ES", "NL", "BE", "IT"]

TARIFF_TYPES = ["STANDARD", "GREEN", "RENEWABLE", "EV", "SOLAR"]


def random_iban(country_code: str = "GB") -> str:
    """Generate a synthetic IBAN-shaped string (not cryptographically valid)."""
    check = random.randint(10, 99)
    bank = "".join([str(random.randint(0, 9)) for _ in range(8)])
    account = "".join([str(random.randint(0, 9)) for _ in range(10)])
    return f"{country_code}{check}{bank}{account}"


def random_amount(txn_type: str, category: str) -> float:
    """
    Generate realistic transaction amounts with heavy-tail distribution.
    CHAPS is typically large (corporate payments).
    CRYPTO/GAMBLING/WIRE_TRANSFER skewed higher.
    """
    if txn_type == "CHAPS":
        return round(random.lognormvariate(9.0, 1.2), 2)  # £8k median
    elif category in ["CRYPTO", "WIRE_TRANSFER", "MONEY_TRANSFER", "GAMBLING"]:
        return round(random.lognormvariate(7.5, 1.5), 2)  # £1.8k median
    elif txn_type == "BACS":
        return round(random.lognormvariate(6.5, 1.0), 2)  # £600 median
    else:
        return round(random.lognormvariate(4.5, 1.2), 2)  # £90 median CARD


def generate_transactions(n_rows: int, seed: int = None) -> list:
    """Generate n_rows synthetic transactions."""
    if seed is not None:
        random.seed(seed)

    transactions = []
    base_time = datetime(2024, 1, 1, 0, 0, 0)

    for i in range(n_rows):
        txn_type = random.choices(TRANSACTION_TYPES, weights=TRANSACTION_TYPE_WEIGHTS)[0]
        category = random.choices(MERCHANT_CATEGORIES, weights=MERCHANT_WEIGHTS)[0]
        amount = random_amount(txn_type, category)

        # ~1% of transactions are international to high-risk countries
        is_high_risk = random.random() < 0.01
        if is_high_risk:
            country = random.choice(HIGH_RISK_COUNTRIES)
            ip_country = country
            is_international = True
        else:
            country = random.choices(NORMAL_COUNTRIES, weights=[0.85, 0.05, 0.03, 0.02, 0.02, 0.01, 0.01, 0.005, 0.005])[0]
            # 2% geo mismatch (IP vs card country)
            if random.random() < 0.02:
                ip_country = random.choice(NORMAL_COUNTRIES)
            else:
                ip_country = country
            is_international = country != "GB"

        # Counterparty IBAN only for bank transfers
        has_counterparty = txn_type in ["FPS", "BACS", "CHAPS"]
        counterparty_iban = random_iban(country) if has_counterparty else ""

        event_time = base_time + timedelta(seconds=i * 36)   # ~2400 txns/hour

        transactions.append({
            "transaction_id":    f"TXN-{str(uuid.uuid4())[:8].upper()}",
            "account_id":        f"ACC-{random.randint(10000, 99999)}",
            "iban":              random_iban("GB"),
            "counterparty_iban": counterparty_iban,
            "amount_gbp":        amount,
            "currency":          "GBP",
            "transaction_type":  txn_type,
            "merchant_category": category,
            "country_code":      country,
            "event_timestamp":   event_time.strftime("%Y-%m-%d %H:%M:%S"),
            "device_id":         f"DEV-{random.randint(1000, 9999)}",
            "ip_country":        ip_country,
            "is_international":  str(is_international).lower(),
        })

    return transactions


def write_csv(transactions: list, output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fieldnames = list(transactions[0].keys())
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(transactions)
    print(f"Written {len(transactions):,} rows to {output_path}")


def write_malformed_json(output_path: str, n: int = 100) -> None:
    """Generate malformed event samples for dead-letter testing."""
    import json
    malformed = []
    for i in range(n):
        choice = i % 5
        if choice == 0:
            malformed.append({"account_id": "ACC-001", "amount_gbp": 500.0})  # missing txn_id
        elif choice == 1:
            malformed.append({"transaction_id": f"TXN-{i}", "account_id": "ACC-002"})  # missing amount
        elif choice == 2:
            malformed.append({"transaction_id": f"TXN-{i}", "amount_gbp": "NOT_A_NUMBER"})  # bad type
        elif choice == 3:
            malformed.append({})  # completely empty
        else:
            malformed.append({"transaction_id": None, "amount_gbp": -999.0})  # nulls + negative

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        for record in malformed:
            f.write(json.dumps(record) + "\n")
    print(f"Written {len(malformed)} malformed events to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic transaction data")
    parser.add_argument("--rows",   type=int, default=100000, help="Number of rows to generate")
    parser.add_argument("--output", type=str, default="sample_data/", help="Output directory")
    parser.add_argument("--seed",   type=int, default=None,   help="Random seed for reproducibility")
    args = parser.parse_args()

    print(f"Generating {args.rows:,} synthetic transactions (seed={args.seed})...")
    transactions = generate_transactions(args.rows, seed=args.seed)

    csv_path = os.path.join(args.output, "sample_transactions.csv")
    write_csv(transactions, csv_path)

    malformed_path = os.path.join(args.output, "sample_malformed.json")
    write_malformed_json(malformed_path, n=200)

    # Stats summary
    high_value = sum(1 for t in transactions if float(t["amount_gbp"]) >= 5000)
    international = sum(1 for t in transactions if t["is_international"] == "true")
    print(f"\nStats:")
    print(f"  Total rows      : {len(transactions):,}")
    print(f"  High value (>=5k): {high_value:,} ({high_value/len(transactions)*100:.1f}%)")
    print(f"  International   : {international:,} ({international/len(transactions)*100:.1f}%)")
    print(f"\nTo run pipeline locally:")
    print(f"  pytest tests/ -v --cov=src --cov-report=html")
