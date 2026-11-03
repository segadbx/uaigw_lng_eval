# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Raw landing zone: mock source-system extracts (medallion stage **raw**)
# MAGIC Writes **fictional** extract files into the UC volume `<catalog>.<raw_schema>.<landing_volume>`, as a
# MAGIC contract-management system and a counterparty master would drop them. The Lakeflow pipeline
# MAGIC `lngdemo-medallion` picks them up with Auto Loader (bronze) → validated + Auto CDC (silver) → governed gold.
# MAGIC
# MAGIC | File | Content |
# MAGIC |---|---|
# MAGIC | `contracts/batch_0001_initial.json` | 48 contracts, `op = UPSERT` |
# MAGIC | `contracts/batch_0002_amendments.json` | 6 price renegotiations, 2 cancellations (`op = DELETE`), 3 deliberately invalid records (silver expectations drop them) |
# MAGIC | `counterparties/counterparties.csv` | Counterparty A–H reference data (region, rating, relationship contact) |
# MAGIC
# MAGIC Counterparties are aliases, prices are invented formulas, contacts use `example.com` / 555 numbers.
# MAGIC Idempotent: fixed random seed, and a file that already exists is not rewritten (Auto Loader ingests each
# MAGIC file once). Also writes the `desk_entitlements` reference table used by the row filter.

# COMMAND ----------

# MAGIC %pip install -q "databricks-sdk>=0.157.0" pyyaml requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os, sys, json, csv, io, random, datetime as dt
sys.path.insert(0, os.path.abspath("../lib"))
import lngdemo

p = lngdemo.get_params(dbutils)
CAT, TS = p["catalog"], p["trading_schema"]
LANDING = f"/Volumes/{CAT}/{p['raw_schema']}/{p['landing_volume']}"
print("landing zone:", LANDING)

def write_once(rel_path: str, text: str) -> None:
    path = f"{LANDING}/{rel_path}"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):
        print("exists, not rewritten:", rel_path)
        return
    with open(path, "w") as f:
        f.write(text)
    print("wrote", rel_path, f"({len(text)} bytes)")

# COMMAND ----------

rng = random.Random(20261028)
COUNTERPARTIES = [f"Counterparty {c}" for c in "ABCDEFGH"]
DESKS = ["Atlantic Desk", "Pacific Desk", "Portfolio Desk"]
INDEXES = {
    "HH":    lambda: (round(rng.uniform(1.10, 1.20), 2), round(rng.uniform(2.25, 3.40), 2)),
    "JKM":   lambda: (round(rng.uniform(0.85, 1.00), 2), round(rng.uniform(0.10, 0.60), 2)),
    "TTF":   lambda: (round(rng.uniform(0.90, 1.00), 2), round(rng.uniform(-0.40, 0.30), 2)),
    "Brent": lambda: (round(rng.uniform(0.110, 0.135), 4), round(rng.uniform(0.30, 0.90), 2)),
}
FIRST = ["avery", "jordan", "riley", "casey", "morgan", "quinn", "taylor", "drew"]
LAST = ["chen", "lee", "patel", "garcia", "novak", "okafor", "silva", "berg"]
T0 = dt.datetime(2026, 10, 1, 6, 0, 0)

def formula(index, slope, const):
    sign = "+" if const >= 0 else "-"
    s = f"{slope * 100:.2f}% Brent" if index == "Brent" else f"{slope:.2f} x {index}"
    return f"{s} {sign} {abs(const):.2f} USD/MMBtu"

def event(c: dict, op: str, ts: dt.datetime, batch: str) -> str:
    return json.dumps({**c, "op": op, "event_ts": ts.isoformat() + "Z", "source_system": "ctrm-mock", "batch_id": batch},
                      default=str)

contracts = []
for i in range(48):
    idx = rng.choice(list(INDEXES))
    slope, const = INDEXES[idx]()
    start = dt.date(2024 + rng.randint(0, 3), rng.choice([1, 4, 7, 10]), 1)
    end = start.replace(year=start.year + rng.choice([5, 10, 15, 20]))
    contracts.append(dict(
        contract_id=f"LNG-SPA-{1000 + i}",
        counterparty_alias=COUNTERPARTIES[i % len(COUNTERPARTIES)],
        desk=DESKS[(i // 2) % len(DESKS)],
        contract_type=rng.choice(["SPA", "MSPA", "Tolling", "Spot-linked"]),
        price_index=idx, slope=float(slope), constant_usd_mmbtu=float(const),
        price_formula=formula(idx, slope, const),
        volume_mtpa=round(rng.uniform(0.3, 2.5), 2),
        start_date=start.isoformat(), end_date=end.isoformat(),
        contact_email=f"{rng.choice(FIRST)}.{rng.choice(LAST)}@example.com",
        contact_phone=f"+1 202 555 01{rng.randint(10, 99)}",
    ))

# Batch 1: initial load
write_once("contracts/batch_0001_initial.json",
           "\n".join(event(c, "UPSERT", T0 + dt.timedelta(minutes=i), "0001") for i, c in enumerate(contracts)) + "\n")

# Batch 2: amendments two weeks later
T1 = T0 + dt.timedelta(days=14)
amend = []
for c in contracts[3:9]:  # price renegotiations -> new silver version, SCD2 history row
    new_const = round(c["constant_usd_mmbtu"] + rng.choice([-0.25, -0.15, 0.10, 0.20]), 2)
    amend.append(event({**c, "constant_usd_mmbtu": new_const,
                        "price_formula": formula(c["price_index"], c["slope"], new_const)}, "UPSERT", T1, "0002"))
for c in contracts[40:42]:  # cancellations
    amend.append(event(c, "DELETE", T1 + dt.timedelta(hours=1), "0002"))
bad = contracts[10]  # invalid records: expectations in silver drop these
amend.append(event({**bad, "contract_id": "LNG-SPA-9001", "desk": None}, "UPSERT", T1, "0002"))
amend.append(event({**bad, "contract_id": "LNG-SPA-9002", "volume_mtpa": -1.0}, "UPSERT", T1, "0002"))
amend.append(event({**bad, "contract_id": "LNG-SPA-9003", "price_index": "XYZ"}, "UPSERT", T1, "0002"))
write_once("contracts/batch_0002_amendments.json", "\n".join(amend) + "\n")

# Counterparty master (CSV)
buf = io.StringIO()
wr = csv.writer(buf)
wr.writerow(["counterparty_alias", "region", "credit_rating", "relationship_contact", "updated_at"])
for k, alias in enumerate(COUNTERPARTIES):
    wr.writerow([alias, ["Asia", "Europe", "Americas"][k % 3], ["A", "A-", "BBB+", "BBB"][k % 4],
                 f"rm.{alias[-1].lower()}@example.com", T0.date().isoformat()])
write_once("counterparties/counterparties.csv", buf.getvalue())

# COMMAND ----------

# Desk entitlements: which account group may see which desk's rows (row-filter input). Reference data
# owned by governance, not by the source system, so it is not part of the medallion pipeline.
ent = [(p["trader_group"], "Atlantic Desk"), (p["trader_group"], "Portfolio Desk"),
       (p["unentitled_trader_group"], "Pacific Desk")]
spark.createDataFrame(ent, "group_name STRING, desk STRING").createOrReplaceTempView("ent_src")
spark.sql(f"""
CREATE OR REPLACE TABLE {lngdemo.fq(CAT, TS, 'desk_entitlements')}
COMMENT 'LNG demo: group -> desk entitlement map used by desk_filter()'
AS SELECT * FROM ent_src
""")
display(dbutils.fs.ls(f"{LANDING}/contracts"))
