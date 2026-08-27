#!/usr/bin/env python3
"""Pre-flight: every column named in an expectation must exist in the staged view.

Run before `bundle run`. Free Edition has a daily compute quota, and this class
of bug is only caught by pipeline graph analysis — which means a cluster start,
a failed update, and quota spent to learn about a typo.

The bug this was written for: SPRINT_RULES was copied from RESULT_RULES and kept
`position IS NOT NULL`, but stg_sprint_result aliases that column
`sprint_position`. Expectation rule dicts are shared between a fact and its
quarantine view, so one stale name fails two datasets.

    python3 scripts/check_expectations.py     # exit 1 if any rule is unresolvable

It used to read 02_silver_facts.py alone, which meant the laps, pit stop and
weather rule sets — three of the six rule dicts in the pipeline — were never
checked at all. It now walks every Silver file, and prints the per-file tally so
a file silently contributing nothing is visible rather than assumed clean.
"""
import ast
import glob
import re
import sys

PATHS = sorted(glob.glob("src/pipeline/0[23]*.py"))

SQL_WORDS = {
    "IS", "NOT", "NULL", "OR", "AND", "BETWEEN", "IN", "LIKE", "WHEN", "CASE",
    "THEN", "ELSE", "END", "TRUE", "FALSE", "CAST", "AS", "INT", "DOUBLE",
    "STRING", "DATE", "LENGTH", "SIZE", "COALESCE",
}


def decorator_calls(node):
    for d in node.decorator_list:
        if isinstance(d, ast.Call):
            yield d


def dec_name(call):
    f = call.func
    return f.attr if isinstance(f, ast.Attribute) else getattr(f, "id", "")


def collect_aliases(fn):
    """Every column name stg_* emits: .alias("x"), bare "x" selects, withColumn("x")."""
    names = set()
    for n in ast.walk(fn):
        if isinstance(n, ast.Call):
            name = dec_name(n)
            if name == "alias" and n.args and isinstance(n.args[0], ast.Constant):
                names.add(n.args[0].value)
            elif name == "withColumn" and n.args and isinstance(n.args[0], ast.Constant):
                names.add(n.args[0].value)
            elif name in ("select", "drop"):
                for a in n.args:
                    if isinstance(a, ast.Constant) and isinstance(a.value, str):
                        names.add(a.value)
    return names


def sql_identifiers(expr):
    toks = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", expr)
    return {t for t in toks if t.upper() not in SQL_WORDS and not t.isdigit()}


def analyse(tree):
    """Return [(dataset_fn, stg_source, [(rule_name, expr)])] for one file."""
    # stg view name -> emitted columns
    staged = {}
    # module-level rules dicts
    rules = {}
    # MV function -> (stg source, [(rule_name, expr)])
    checks = []

    for node in tree.body:
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Dict):
            target = node.targets[0]
            if isinstance(target, ast.Name):
                rules[target.id] = {
                    k.value: v.value
                    for k, v in zip(node.value.keys, node.value.values)
                }
        if isinstance(node, ast.FunctionDef):
            for call in decorator_calls(node):
                if dec_name(call) == "temporary_view":
                    for kw in call.keywords:
                        if kw.arg == "name":
                            staged[kw.value.value] = collect_aliases(node)

    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        # which stg_* does this dataset read?
        source = None
        for n in ast.walk(node):
            if isinstance(n, ast.Call) and dec_name(n) == "table" and n.args:
                if isinstance(n.args[0], ast.Constant) and str(n.args[0].value).startswith("stg_"):
                    source = n.args[0].value
        if not source:
            continue

        exprs = []
        for call in decorator_calls(node):
            name = dec_name(call)
            if name in ("expect_all_or_drop", "expect_all") and call.args:
                arg = call.args[0]
                if isinstance(arg, ast.Name):
                    exprs += list(rules.get(arg.id, {}).items())
                elif isinstance(arg, ast.Dict):
                    exprs += [(k.value, v.value) for k, v in zip(arg.keys, arg.values)]
            elif name in ("expect", "expect_or_fail") and len(call.args) == 2:
                exprs.append((call.args[0].value, call.args[1].value))
        # quarantine views apply the same dicts via invalid_predicate(RULES)
        for n in ast.walk(node):
            if isinstance(n, ast.Call) and dec_name(n) in ("invalid_predicate", "quarantine_reason"):
                if n.args and isinstance(n.args[0], ast.Name):
                    exprs += list(rules.get(n.args[0].id, {}).items())
        if exprs:
            checks.append((node.name, source, exprs))

    return checks, staged


bad = 0
total = 0
for path in PATHS:
    checks, staged = analyse(ast.parse(open(path).read()))
    total += len(checks)
    for fn, source, exprs in checks:
        cols = staged.get(source, set())
        if not cols:
            print(f"?? {path}: {fn} reads {source}, which this file does not define")
            continue
        for rule_name, expr in exprs:
            missing = sql_identifiers(expr) - cols
            if missing:
                bad += 1
                print(f"FAIL {path}: {fn} [{rule_name}] -> "
                      f"{sorted(missing)} not in {source}")
    print(f"  {path:38s} {len(checks):2d} dataset(s)")

print(f"\n{total} datasets checked across {len(PATHS)} files, "
      f"{bad} bad expectation(s)")
sys.exit(1 if bad else 0)
