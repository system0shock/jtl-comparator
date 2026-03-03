# Code Graph: JTL Comparator

## 1) High-Level Map

```text
[Browser / UI]
  templates/index.html
     |
     | POST /compare (multipart/form-data)
     v
[Flask API]
  app.py
     |
     | parse_jtl(file1), parse_jtl(file2)
     | compare(df1, df2, name1, name2, rules)
     v
[Analyzer Core]
  analyzers/jtl_analyzer.py
     |
     | JSON result: rows + summary + active rules
     v
[Browser / UI]
  renderTable(), export Markdown/CSV, delta tooltips
```

## 2) Backend Graph

```text
app.py
  index() -> render_template("index.html")
  compare_runs()
    - validate uploaded files
    - read run names
    - read delta rules from form
    - save temp files
    - parse_jtl(tmp1/tmp2)
    - compare(df1, df2, name1, name2, rules=delta_rules)
    - return jsonify(result)
```

```text
analyzers/jtl_analyzer.py
  parse_jtl(filepath)
    -> read CSV -> validate required columns
    -> parent transaction filtering (URL empty/null/None)
    -> type conversion + cleanup

  aggregate(df)
    -> group by label
    -> samples, avg, p50/p90/p95/p99, min/max, throughput, error_rate

  compare(df1, df2, name1, name2, rules=None)
    -> _normalize_delta_rules(rules)
    -> aggregate both runs
    -> merge by label (outer)
    -> compute d_avg / d_p95 / d_p99 / d_rps
    -> classify via rule-aware:
       _time_css_class(), _rps_css_class(), _err_css_class()
    -> _build_summary(rows, rules)
    -> return {name1, name2, rules, rows, summary}
```

## 3) Delta Rules (Current Runtime Contract)

`compare(..., rules=...)` accepts:

- `time_warning_pct`
- `time_critical_pct`
- `time_improved_pct`
- `rps_warning_drop_pct`
- `rps_critical_drop_pct`
- `rps_improved_gain_pct`
- `err_warning_increase_pct`
- `err_critical_increase_pct`
- `err_improved_decrease_pct`

Validation:

- values must be non-negative numbers
- `critical >= warning` for time / rps / error-rate families

If no custom values are passed, defaults are used.

## 4) Frontend Graph

```text
templates/index.html
  Upload/UI layer:
    setupDropZone()
    run names + 2 file inputs

  Rules UI:
    <details class="rules-panel"> with 9 numeric fields
    collectDeltaRules()
    resetDeltaRules()

  Compare flow:
    runCompare()
      -> FormData(file1, file2, name1, name2, rules...)
      -> fetch('/compare')
      -> renderTable(data)

  Render/export:
    renderTable()
    copyMarkdown()
    downloadCSV()

  Tooltips:
    deltaTooltip() builds explanation text for Δ cells
    custom tooltip engine:
      ensureTooltip(), showTooltip(), positionTooltip(), hideTooltip()
      handlers: mouseover/mousemove/mouseout/focusin/focusout
```

## 5) UI/UX Additions Reflected

- Help panel `Как пользоваться инструментом`.
- Configurable delta rules panel with per-field hints.
- Custom styled tooltips for:
  - delta cells (`Δ Avg`, `Δ p95`, `Δ p99`, `Δ RPS`)
  - rule fields (`?` hints)
- Tooltip text includes:
  - formula
  - concrete Run1/Run2 values
  - better/worse interpretation
  - active thresholds for time and RPS.

## 6) Test Graph

```text
tests/test_jtl_analyzer.py
  test_summary_uses_weighted_average_by_samples
  test_missing_run_error_rate_is_none_and_neutral
  test_custom_delta_rules_are_applied
```

Coverage focus:

- summary aggregation correctness
- missing-run error-rate semantics
- custom rule application path
