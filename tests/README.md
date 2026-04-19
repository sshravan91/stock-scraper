# Tests Layout

- `tests/scripts/test_consolidation.py`: runs consolidation against XLS fixtures.
- `tests/scripts/test_advisor_parser_new.py`: runs advisor parser using fixture XLS inputs.
- `tests/fixtures/*.xls`: isolated copies of required XLS files for test runs.
- `resources/`: project data resources (JSON/YAML), including `funds_and_categories_with_mftools.json`.

Run examples:

```bash
python tests/scripts/test_consolidation.py
python tests/scripts/test_advisor_parser_new.py
```
