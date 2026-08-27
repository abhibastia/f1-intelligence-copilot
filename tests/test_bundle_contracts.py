"""Bundle checks that need no workspace, and therefore run on every fork.

`databricks bundle validate` cannot run without credentials. In `mode:
development` it resolves `root_path` from `${workspace.current_user.userName}`,
which is a live SCIM call — point it at a dummy host and it fails with a DNS
error, not a validation result. So CI cannot validate the bundle on a pull
request from a fork, and pretending otherwise gives a green tick that means
nothing.

These assertions cover the part that is checkable offline: every path a
resource points at exists, and every `${var.*}` it interpolates is declared.
Both are wrong far more often than the schema is, and both are silent —
a `python_file` typo deploys fine and fails at run time.
"""

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = yaml.safe_load((ROOT / "databricks.yml").read_text())
RESOURCE_FILES = sorted((ROOT / "resources").glob("*.yml"))

# Keys whose value is a path relative to the file that declares it.
PATH_KEYS = {"python_file", "file_path", "include"}


def _walk(node, path_keys=PATH_KEYS):
    """Yield (key, value) for every string-valued key of interest, recursively."""
    if isinstance(node, dict):
        for key, value in node.items():
            if key in path_keys and isinstance(value, str):
                yield key, value
            yield from _walk(value, path_keys)
    elif isinstance(node, list):
        for item in node:
            yield from _walk(item, path_keys)


@pytest.mark.parametrize("path", RESOURCE_FILES, ids=lambda p: p.name)
def test_resource_file_parses(path):
    assert yaml.safe_load(path.read_text()), f"{path.name} is empty or invalid YAML"


@pytest.mark.parametrize("path", RESOURCE_FILES, ids=lambda p: p.name)
def test_resource_paths_exist(path):
    """A `python_file` that does not exist deploys cleanly and fails at run time."""
    resource = yaml.safe_load(path.read_text())
    for key, value in _walk(resource):
        # `include` under a libraries glob ends in `**`; check the directory.
        target = value[:-3] if value.endswith("/**") else value
        resolved = (path.parent / target).resolve()
        assert resolved.exists(), (
            f"{path.name}: {key} -> {value} does not exist ({resolved})"
        )


@pytest.mark.parametrize("path", RESOURCE_FILES, ids=lambda p: p.name)
def test_variables_are_declared(path):
    """Every ${var.x} a resource uses must exist in databricks.yml."""
    declared = set(BUNDLE.get("variables", {}))
    used = set(re.findall(r"\$\{var\.([a-z_]+)\}", path.read_text()))
    undeclared = sorted(used - declared)
    assert not undeclared, f"{path.name} uses undeclared variable(s): {undeclared}"


def test_declared_variables_are_used():
    """A variable nobody reads is a default that silently stops applying."""
    declared = set(BUNDLE.get("variables", {}))
    blob = "".join(p.read_text() for p in RESOURCE_FILES)
    blob += (ROOT / "databricks.yml").read_text()
    unused = sorted(v for v in declared if f"${{var.{v}}}" not in blob)
    assert not unused, f"declared but never referenced: {unused}"


def test_resource_references_resolve():
    """`${resources.pipelines.x.id}` must name a pipeline this bundle defines."""
    defined = set()
    for path in RESOURCE_FILES:
        resources = (yaml.safe_load(path.read_text()) or {}).get("resources", {})
        for kind, items in resources.items():
            defined |= {f"{kind}.{name}" for name in items}

    for path in RESOURCE_FILES:
        for kind, name in re.findall(
            r"\$\{resources\.(\w+)\.(\w+)\.", path.read_text()
        ):
            assert f"{kind}.{name}" in defined, (
                f"{path.name} references {kind}.{name}, which no resource defines"
            )


def test_every_resource_file_is_included():
    """`include:` must actually reach the resources directory."""
    patterns = BUNDLE.get("include", [])
    assert any("resources/" in p for p in patterns), \
        "databricks.yml does not include resources/*.yml"
