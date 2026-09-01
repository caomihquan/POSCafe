#!/usr/bin/env python3
"""Validate repository event schemas and their compatibility declarations."""

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCHEMAS = ROOT / "schemas"


def load(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"{path}: invalid JSON: {exc}") from exc


def main() -> int:
    errors = []
    schema_files = sorted(SCHEMAS.glob("*.schema.json"))
    if not schema_files:
        errors.append("schemas/: no *.schema.json files found")

    for schema_path in schema_files:
        schema = load(schema_path)
        if schema.get("type") != "object":
            errors.append(f"{schema_path}: root type must be object")
        if not schema.get("$id"):
            errors.append(f"{schema_path}: missing $id")
        if not schema.get("required"):
            errors.append(f"{schema_path}: required must not be empty")
        if schema.get("additionalProperties") is not False:
            errors.append(f"{schema_path}: additionalProperties must be false")

        compatibility_path = schema_path.with_name(schema_path.name.replace(".schema.json", ".compatibility.json"))
        if not compatibility_path.exists():
            errors.append(f"{schema_path}: missing {compatibility_path.name}")
            continue
        compatibility = load(compatibility_path)
        expected_id = schema_path.name.removesuffix(".schema.json")
        if compatibility.get("schemaId") != expected_id:
            errors.append(f"{compatibility_path}: schemaId must be {expected_id}")
        if compatibility.get("compatibility") not in {"backward", "forward", "full"}:
            errors.append(f"{compatibility_path}: unsupported compatibility policy")
        if compatibility.get("schemaVersion") != expected_id.rsplit(".v", 1)[-1]:
            errors.append(f"{compatibility_path}: schemaVersion does not match schemaId")
        required_headers = set(compatibility.get("requiredHeaders", []))
        if not {"event-id", "event-type", "correlation-id", "schema-version"}.issubset(required_headers):
            errors.append(f"{compatibility_path}: requiredHeaders is missing mandatory event headers")

    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1

    print(f"Validated {len(schema_files)} event schema(s) and compatibility declaration(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
