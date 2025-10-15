# Consensus Bug Isolation

Statistical fault localization tool for analyzing distributed consensus failures in Ripple protocol. Processes validator logs from a 7-node network to identify message patterns that correlate with consensus failures.

## Installation

```bash
pip install tqdm rich
```

## Usage

### Main Analysis Pipeline

```bash
python3 scripts/main.py
```

Performs two-phase analysis:
1. **Cache Generation**: Evaluates predicates on message pairs from validator logs and caches results
2. **Statistical Fault Localization**: Aggregates observations across successful/failed runs, identifies suspicious predicates

Set `REGENERATE_CACHE = False` in `scripts/main.py` to skip cache regeneration and load from pickle (faster).

### Statistics & Evaluation

```bash
python3 scripts/analyze.py              # Print confusion matrix for all configurations
python3 scripts/analyze.py cap 100      # Limit to first 100 runs per configuration
```

### Legacy Approach

```bash
python3 legacy/isolate/main.py
```

Baseline approach that reads predicate observations directly from PRED annotations in validator logs.

## Project Structure

- `src/` - Core library code (models, analysis, utilities)
- `scripts/` - Executable entry points (main.py, analyze.py)
- `config/` - Configuration files (runs.py with run IDs)
- `legacy/` - Legacy PRED-based analysis approach
- `cache/` - Generated pickle caches (gitignored)
- `data/` - Validator logs and results
