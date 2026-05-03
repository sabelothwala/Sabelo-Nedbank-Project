"""Pandera schemas for every layer boundary.

Schemas are derived directly from docs/data_dictionary.md and
docs/output_schema_spec.md. They are the single contract enforced at every
write — a Pandera failure crashes the pipeline non-zero rather than emit an
off-contract Delta table.

Layer conventions:
  Bronze: raw bytes from source, all source columns kept as Utf8 (CSV is read
          with infer_schema_length=0). JSON-typed transactions keep their JSON
          types (Float64 for amount, Boolean for retry_flag) since round-tripping
          to string would *modify* the data and Bronze is supposed to be raw.
          Audit columns are appended.
  Silver: typed and standardised. Dates are pl.Date, money is pl.Decimal(18,2),
          timestamps are pl.Datetime. Audit columns preserved (POPIA §19 trail).
  Gold:   strict + ordered. Field counts and positions are checked by the
          scoring harness. dim_accounts has customer_id at position 3 (GAP-026).
          fact_transactions has 15 fields including merchant_subcategory (null
          in Stage 1) and dq_flag (null for clean records).

Why pandera.polars and not pandera.pandas: we never touch pandas in this
pipeline — round-tripping to pandas costs a copy and kills our streaming-write
guarantees on the transactions frame.
"""

from __future__ import annotations

import polars as pl
from pandera.polars import Column, DataFrameSchema

# ────────────────────────────────────────────────────────────────────────────
# Audit metadata — POPIA §19 processing trail. Added in Bronze, preserved
# through Silver, dropped in Gold (except `ingestion_timestamp` which becomes
# fact_transactions.ingestion_timestamp at Gold time).
# ────────────────────────────────────────────────────────────────────────────
_AUDIT_COLUMNS: dict[str, Column] = {
    "_ingested_at":     Column(pl.Datetime("us", "UTC"), nullable=False),
    "_source_system":   Column(pl.Utf8, nullable=False),
    "_batch_id":        Column(pl.Utf8, nullable=False),
    "_source_row_hash": Column(pl.Utf8, nullable=False),
}

# Canonical SA province values used in dim_customers.province and
# fact_transactions.province (the latter via location.province in the source).
SA_PROVINCES = (
    "Eastern Cape", "Free State", "Gauteng", "KwaZulu-Natal",
    "Limpopo", "Mpumalanga", "Northern Cape", "North West", "Western Cape",
)
GENDER_VALUES = ("M", "F", "NB", "UNKNOWN")
INCOME_BANDS = ("LOW", "LOWER_MIDDLE", "MIDDLE", "UPPER_MIDDLE", "HIGH")
SEGMENTS = ("MASS", "EMERGING", "MIDDLE_MKT", "PROFESSIONAL", "PRIVATE")
KYC_STATUSES = ("VERIFIED", "PENDING", "FAILED")
ACCOUNT_TYPES = ("SAVINGS", "TRANSACTIONAL", "CREDIT")
ACCOUNT_STATUSES = ("ACTIVE", "DORMANT", "CLOSED", "SUSPENDED")
PRODUCT_TIERS = ("BASIC", "STANDARD", "PREMIUM")
DIGITAL_CHANNELS = ("APP", "USSD", "WEB")
TRANSACTION_TYPES = ("DEBIT", "CREDIT", "FEE", "REVERSAL")
TRANSACTION_CHANNELS = ("POS", "APP", "ATM", "EFT", "USSD", "INTERNAL")
MERCHANT_CATEGORIES = (
    "GROCERY", "FUEL", "RESTAURANT", "RETAIL", "HEALTHCARE", "UTILITIES",
    "TRANSPORT", "ENTERTAINMENT", "EDUCATION", "INSURANCE", "RENT", "SALARY",
    "ATM_WITHDRAWAL", "TRANSFER_IN", "TRANSFER_OUT", "REVERSAL_CREDIT",
    "REVERSAL_DEBIT", "FEE_SERVICE", "FEE_MONTHLY", "FEE_TRANSACTION",
)
AGE_BANDS = ("18-25", "26-35", "36-45", "46-55", "56-65", "65+")
DQ_FLAG_VALUES = (
    "ORPHANED_ACCOUNT", "DUPLICATE_DEDUPED", "TYPE_MISMATCH",
    "DATE_FORMAT", "CURRENCY_VARIANT", "NULL_REQUIRED",
)

# ════════════════════════════════════════════════════════════════════════════
# BRONZE
# ════════════════════════════════════════════════════════════════════════════

BronzeCustomersSchema = DataFrameSchema(
    columns={
        "customer_id":   Column(pl.Utf8, nullable=False, unique=True),
        "id_number":     Column(pl.Utf8, nullable=False),
        "first_name":    Column(pl.Utf8, nullable=False),
        "last_name":     Column(pl.Utf8, nullable=False),
        "dob":           Column(pl.Utf8, nullable=False),
        "gender":        Column(pl.Utf8, nullable=False),
        "province":      Column(pl.Utf8, nullable=False),
        "income_band":   Column(pl.Utf8, nullable=False),
        "segment":       Column(pl.Utf8, nullable=False),
        "risk_score":    Column(pl.Utf8, nullable=False),
        "kyc_status":    Column(pl.Utf8, nullable=False),
        "product_flags": Column(pl.Utf8, nullable=False),
        **_AUDIT_COLUMNS,
    },
    strict=True,
    name="BronzeCustomersSchema",
)

BronzeAccountsSchema = DataFrameSchema(
    columns={
        "account_id":         Column(pl.Utf8, nullable=False, unique=True),
        "customer_ref":       Column(pl.Utf8, nullable=False),
        "account_type":       Column(pl.Utf8, nullable=False),
        "account_status":     Column(pl.Utf8, nullable=False),
        "open_date":          Column(pl.Utf8, nullable=False),
        "product_tier":       Column(pl.Utf8, nullable=False),
        "mobile_number":      Column(pl.Utf8, nullable=True),
        "digital_channel":    Column(pl.Utf8, nullable=False),
        "credit_limit":       Column(pl.Utf8, nullable=True),
        "current_balance":    Column(pl.Utf8, nullable=False),
        "last_activity_date": Column(pl.Utf8, nullable=True),
        **_AUDIT_COLUMNS,
    },
    strict=True,
    name="BronzeAccountsSchema",
)

# Bronze transactions: JSON types preserved (Bronze = raw). nested location/
# metadata are flattened by the ingest layer before the schema check (Polars
# struct unnesting is a no-op transformation; the values are unchanged).
BronzeTransactionsSchema = DataFrameSchema(
    columns={
        "transaction_id":       Column(pl.Utf8, nullable=False),
        "account_id":           Column(pl.Utf8, nullable=False),
        "transaction_date":     Column(pl.Utf8, nullable=False),
        "transaction_time":     Column(pl.Utf8, nullable=False),
        "transaction_type":     Column(pl.Utf8, nullable=False),
        "merchant_category":    Column(pl.Utf8, nullable=True),
        # Stage 1 source: key absent. ingest must add the column as null Utf8.
        "merchant_subcategory": Column(pl.Utf8, nullable=True),
        "amount":               Column(pl.Float64, nullable=False),
        "currency":             Column(pl.Utf8, nullable=False),
        "channel":              Column(pl.Utf8, nullable=False),
        "location_province":    Column(pl.Utf8, nullable=True),
        "location_city":        Column(pl.Utf8, nullable=True),
        "location_coordinates": Column(pl.Utf8, nullable=True),
        "metadata_device_id":   Column(pl.Utf8, nullable=True),
        "metadata_session_id":  Column(pl.Utf8, nullable=True),
        "metadata_retry_flag":  Column(pl.Boolean, nullable=False),
        **_AUDIT_COLUMNS,
    },
    strict=True,
    name="BronzeTransactionsSchema",
)

# ════════════════════════════════════════════════════════════════════════════
# SILVER — typed, standardised, deduplicated. Audit columns preserved.
# ════════════════════════════════════════════════════════════════════════════

SilverCustomersSchema = DataFrameSchema(
    columns={
        "customer_id":   Column(pl.Utf8, nullable=False, unique=True),
        "id_number":     Column(pl.Utf8, nullable=False),
        "first_name":    Column(pl.Utf8, nullable=False),
        "last_name":     Column(pl.Utf8, nullable=False),
        "dob":           Column(pl.Date, nullable=False),
        "gender":        Column(pl.Utf8, nullable=False),
        "province":      Column(pl.Utf8, nullable=False),
        "income_band":   Column(pl.Utf8, nullable=False),
        "segment":       Column(pl.Utf8, nullable=False),
        "risk_score":    Column(pl.Int32, nullable=False),
        "kyc_status":    Column(pl.Utf8, nullable=False),
        "product_flags": Column(pl.Utf8, nullable=False),
        **_AUDIT_COLUMNS,
    },
    strict=True,
    name="SilverCustomersSchema",
)

SilverAccountsSchema = DataFrameSchema(
    columns={
        "account_id":         Column(pl.Utf8, nullable=False, unique=True),
        "customer_ref":       Column(pl.Utf8, nullable=False),
        "account_type":       Column(pl.Utf8, nullable=False),
        "account_status":     Column(pl.Utf8, nullable=False),
        "open_date":          Column(pl.Date, nullable=False),
        "product_tier":       Column(pl.Utf8, nullable=False),
        "mobile_number":      Column(pl.Utf8, nullable=True),
        "digital_channel":    Column(pl.Utf8, nullable=False),
        "credit_limit":       Column(pl.Decimal(18, 2), nullable=True),
        "current_balance":    Column(pl.Decimal(18, 2), nullable=False),
        "last_activity_date": Column(pl.Date, nullable=True),
        **_AUDIT_COLUMNS,
    },
    strict=True,
    name="SilverAccountsSchema",
)

SilverTransactionsSchema = DataFrameSchema(
    columns={
        "transaction_id":       Column(pl.Utf8, nullable=False, unique=True),
        "account_id":           Column(pl.Utf8, nullable=False),
        "transaction_date":     Column(pl.Date, nullable=False),
        "transaction_time":     Column(pl.Utf8, nullable=False),
        "transaction_timestamp": Column(pl.Datetime("us"), nullable=False),
        "transaction_type":     Column(pl.Utf8, nullable=False),
        "merchant_category":    Column(pl.Utf8, nullable=True),
        "merchant_subcategory": Column(pl.Utf8, nullable=True),
        "amount":               Column(pl.Decimal(18, 2), nullable=False),
        "currency":             Column(pl.Utf8, nullable=False),
        "channel":              Column(pl.Utf8, nullable=False),
        "location_province":    Column(pl.Utf8, nullable=True),
        "location_city":        Column(pl.Utf8, nullable=True),
        "location_coordinates": Column(pl.Utf8, nullable=True),
        "metadata_device_id":   Column(pl.Utf8, nullable=True),
        "metadata_session_id":  Column(pl.Utf8, nullable=True),
        "metadata_retry_flag":  Column(pl.Boolean, nullable=False),
        "dq_flag":              Column(pl.Utf8, nullable=True),
        **_AUDIT_COLUMNS,
    },
    strict=True,
    name="SilverTransactionsSchema",
)

# ════════════════════════════════════════════════════════════════════════════
# GOLD — the scoring contract. strict=True, ordered=True, exact field counts.
# ════════════════════════════════════════════════════════════════════════════

# 9 fields per output_schema_spec §4.
DimCustomersSchema = DataFrameSchema(
    columns={
        "customer_sk":  Column(pl.Int64,  nullable=False, unique=True),
        "customer_id":  Column(pl.Utf8,   nullable=False, unique=True),
        "gender":       Column(pl.Utf8,   nullable=False),
        "province":     Column(pl.Utf8,   nullable=False),
        "income_band":  Column(pl.Utf8,   nullable=False),
        "segment":      Column(pl.Utf8,   nullable=False),
        "risk_score":   Column(pl.Int32,  nullable=False),
        "kyc_status":   Column(pl.Utf8,   nullable=False),
        "age_band":     Column(pl.Utf8,   nullable=False),
    },
    strict=True,
    ordered=True,
    name="DimCustomersSchema",
)

# 11 fields per output_schema_spec §3, customer_id AT POSITION 3 (GAP-026).
# Re-ordering this dict re-orders the schema — DO NOT touch without updating
# the validation contract.
DimAccountsSchema = DataFrameSchema(
    columns={
        "account_sk":         Column(pl.Int64,         nullable=False, unique=True),
        "account_id":         Column(pl.Utf8,          nullable=False, unique=True),
        "customer_id":        Column(pl.Utf8,          nullable=False),  # POSITION 3 — GAP-026
        "account_type":       Column(pl.Utf8,          nullable=False),
        "account_status":     Column(pl.Utf8,          nullable=False),
        "open_date":          Column(pl.Date,          nullable=False),
        "product_tier":       Column(pl.Utf8,          nullable=False),
        "digital_channel":    Column(pl.Utf8,          nullable=False),
        "credit_limit":       Column(pl.Decimal(18, 2), nullable=True),
        "current_balance":    Column(pl.Decimal(18, 2), nullable=False),
        "last_activity_date": Column(pl.Date,          nullable=True),
    },
    strict=True,
    ordered=True,
    name="DimAccountsSchema",
)

# 15 fields per output_schema_spec §2.
FactTransactionsSchema = DataFrameSchema(
    columns={
        "transaction_sk":        Column(pl.Int64,          nullable=False, unique=True),
        "transaction_id":        Column(pl.Utf8,           nullable=False, unique=True),
        "account_sk":            Column(pl.Int64,          nullable=False),
        "customer_sk":           Column(pl.Int64,          nullable=False),
        "transaction_date":      Column(pl.Date,           nullable=False),
        "transaction_timestamp": Column(pl.Datetime("us"), nullable=False),
        "transaction_type":      Column(pl.Utf8,           nullable=False),
        "merchant_category":     Column(pl.Utf8,           nullable=True),
        "merchant_subcategory":  Column(pl.Utf8,           nullable=True),  # all-null in Stage 1
        "amount":                Column(pl.Decimal(18, 2), nullable=False),
        "currency":              Column(pl.Utf8,           nullable=False),
        "channel":               Column(pl.Utf8,           nullable=False),
        "province":              Column(pl.Utf8,           nullable=True),
        "dq_flag":               Column(pl.Utf8,           nullable=True),  # null in Stage 1
        "ingestion_timestamp":   Column(pl.Datetime("us", "UTC"), nullable=False),
    },
    strict=True,
    ordered=True,
    name="FactTransactionsSchema",
)

# Registry lookup used by ingest/transform/provision. Keys match values in
# pipeline_config.yaml → sources.<source>.schema_<layer>.
SCHEMA_REGISTRY: dict[str, DataFrameSchema] = {
    "BronzeCustomersSchema":    BronzeCustomersSchema,
    "BronzeAccountsSchema":     BronzeAccountsSchema,
    "BronzeTransactionsSchema": BronzeTransactionsSchema,
    "SilverCustomersSchema":    SilverCustomersSchema,
    "SilverAccountsSchema":     SilverAccountsSchema,
    "SilverTransactionsSchema": SilverTransactionsSchema,
    "DimCustomersSchema":       DimCustomersSchema,
    "DimAccountsSchema":        DimAccountsSchema,
    "FactTransactionsSchema":   FactTransactionsSchema,
}
