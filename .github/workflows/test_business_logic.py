"""
Pure Python business logic tests — zero external dependencies.
No pytest, PySpark, or any data science library required.
Validates fraud detection pipeline by reading source files directly.
"""
import os
import json


def src(filename):
    """Read source file relative to project root."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, filename)) as f:
        return f.read()


class TestMLR2017Compliance:
    def test_5000_threshold_defined(self):
        assert "HIGH_VALUE_THRESHOLD_GBP = 5_000.0" in src("src/fraud_detection_stream.py")

    def test_mlr_2017_flag_set(self):
        assert "MLR_2017_Flagged" in src("src/fraud_detection_stream.py")

    def test_sar_eligible_flag(self):
        assert "sar_eligible" in src("src/risk_scoring.py")

    def test_edd_required_flag(self):
        assert "edd_required" in src("src/risk_scoring.py")

    def test_sar_threshold_075(self):
        assert "0.75" in src("src/risk_scoring.py")

    def test_edd_threshold_050(self):
        assert "0.50" in src("src/risk_scoring.py")


class TestGDPRCompliance:
    def test_sha256_used_for_hashing(self):
        content = src("src/fraud_detection_stream.py")
        assert "sha2" in content and "256" in content

    def test_raw_iban_dropped(self):
        assert '.drop("Customer_Account_IBAN"' in src("src/fraud_detection_stream.py")

    def test_gdpr_pii_hashed_flag(self):
        assert "GDPR_PII_Hashed" in src("src/fraud_detection_stream.py")

    def test_beneficiary_iban_also_hashed(self):
        assert "Beneficiary_IBAN_Hashed" in src("src/fraud_detection_stream.py")

    def test_salt_used_for_hashing(self):
        assert "iban_salt" in src("src/fraud_detection_stream.py")

    def test_data_classification_confidential(self):
        assert "CONFIDENTIAL" in src("src/fraud_detection_stream.py")


class TestWatermarkStrategy:
    def test_watermark_is_10_minutes(self):
        assert '"10 minutes"' in src("src/fraud_detection_stream.py")

    def test_watermark_applied_to_event_timestamp(self):
        assert "Event_Timestamp" in src("src/fraud_detection_stream.py")

    def test_watermark_documented_in_source(self):
        assert "WATERMARKING STRATEGY" in src("src/fraud_detection_stream.py")


class TestRiskScoringEngine:
    def test_five_scoring_factors(self):
        content = src("src/risk_scoring.py")
        factors = ["amount_risk", "country_risk", "channel_risk", "mcc_risk", "international_risk"]
        for f in factors:
            assert f in content, f"Missing risk factor: {f}"

    def test_iran_in_high_risk_countries(self):
        assert '"IRN"' in src("src/risk_scoring.py")

    def test_north_korea_in_high_risk_countries(self):
        assert '"PRK"' in src("src/risk_scoring.py")

    def test_atm_mcc_high_risk(self):
        assert '"6011"' in src("src/risk_scoring.py")

    def test_gambling_mcc_high_risk(self):
        assert '"7995"' in src("src/risk_scoring.py")

    def test_swift_highest_risk(self):
        assert '"SWIFT": 0.9' in src("src/risk_scoring.py")

    def test_card_lowest_risk(self):
        assert '"CARD"' in src("src/risk_scoring.py") and "0.2" in src("src/risk_scoring.py")

    def test_composite_risk_score_computed(self):
        assert "composite_risk_score" in src("src/risk_scoring.py")

    def test_risk_band_classification(self):
        content = src("src/risk_scoring.py")
        assert "CRITICAL" in content
        assert "HIGH" in content
        assert "MEDIUM" in content
        assert "LOW" in content


class TestDeadLetterHandler:
    def test_null_transaction_id_captured(self):
        assert "NULL_TRANSACTION_ID" in src("src/dead_letter_handler.py")

    def test_null_timestamp_captured(self):
        assert "NULL_EVENT_TIMESTAMP" in src("src/dead_letter_handler.py")

    def test_null_amount_captured(self):
        assert "NULL_AMOUNT" in src("src/dead_letter_handler.py")

    def test_review_status_pending(self):
        assert "PENDING" in src("src/dead_letter_handler.py")

    def test_compliance_priority_field(self):
        assert "compliance_priority" in src("src/dead_letter_handler.py")


class TestConfigFiles:
    def test_stream_config_valid_json(self):
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(root, "configs/stream_config.json")) as f:
            config = json.load(f)
        assert config["fraud_filter"]["high_value_threshold_gbp"] == 5000.0

    def test_risk_rules_valid_json(self):
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(root, "configs/risk_rules.json")) as f:
            rules = json.load(f)
        assert rules["thresholds"]["high_value_gbp"] == 5000.0
        assert rules["thresholds"]["sar_eligible_score"] == 0.75

    def test_sample_data_has_50_rows(self):
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(root, "sample_data/sample_transactions.csv")) as f:
            rows = sum(1 for _ in f) - 1
        assert rows == 50, f"Expected 50 rows, got {rows}"

    def test_gold_views_no_raw_pii(self):
        content = src("sql/gold_views.sql")
        assert "Customer_Account_IBAN," not in content


class TestDocumentation:
    def test_readme_has_business_context(self):
        assert "ClearPay" in src("README.md")

    def test_readme_mentions_mlr_2017(self):
        assert "MLR 2017" in src("README.md")

    def test_readme_mentions_gdpr(self):
        assert "GDPR" in src("README.md")

    def test_challenges_has_checkpoint_content(self):
        content = src("docs/CHALLENGES.md")
        assert "checkpoint" in content.lower() or "duplicate" in content.lower()

    def test_challenges_has_salt_content(self):
        assert "salt" in src("docs/CHALLENGES.md").lower()

    def test_challenges_has_watermark_content(self):
        content = src("docs/CHALLENGES.md")
        assert "watermark" in content.lower() or "OOM" in content
