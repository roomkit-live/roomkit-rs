//! Python's generated YAML oracle; regenerate with scripts/build-skill-gating-cases.py.

use serde::Deserialize;

#[derive(Deserialize)]
struct Oracle {
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    frontmatter: String,
    gates: Vec<String>,
}

// Python: tests/test_skill_frontmatter_differential.py::
// test_generated_frontmatters_never_gate_less_on_the_line_reader (allowed_tools only).
#[test]
fn generated_frontmatters_never_gate_less_on_the_line_reader() {
    compare(
        include_str!("../../../tests/skills/frontmatter_cases.json"),
        false,
    );
}

// Python: tests/test_skill_frontmatter_differential.py::
// test_generated_frontmatters_never_gate_less_on_the_line_reader (requires).
#[test]
fn generated_frontmatters_never_require_less_on_the_line_reader() {
    compare(
        include_str!("../../../tests/skills/requirements_cases.json"),
        true,
    );
}

fn compare(text: &str, requires: bool) {
    let oracle: Oracle = serde_json::from_str(text).unwrap();
    let mut compared = 0;
    for case in oracle.cases {
        let Ok(fields) = super::read(&case.frontmatter) else {
            continue;
        };
        let value = if requires {
            fields.requires
        } else {
            fields.allowed_tools
        };
        let names = value.map(|value| value.names()).unwrap_or_default();
        for gate in case.gates {
            assert!(
                names.contains(&gate),
                "lost {gate:?} in {:?}: {names:?}",
                case.frontmatter
            );
        }
        compared += 1;
    }
    assert!(compared > 100, "only {compared} accepted cases compared");
}
