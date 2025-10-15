# Consensus Bug Isolation

Statistical fault localization tool for analyzing distributed consensus failures in Ripple protocol. Processes validator logs from a 7-node network to identify patterns that correlate with consensus failures.

## Installation

```bash
pip install tqdm rich
```

## Usage

This repository contains two equal, first-class approaches for consensus bug isolation:

### 1. Message-Based Approach (Consensus-Aware)

```bash
python3 scripts/main.py
```

Performs two-phase analysis:
1. **Cache Generation**: Evaluates predicates on message pairs from validator logs and caches results
2. **Statistical Fault Localization**: Aggregates observations across successful/failed runs, identifies suspicious predicates

Set `REGENERATE_CACHE = False` in `scripts/main.py` to skip cache regeneration and load from pickle (faster).

### 2. Baseline Approach (PRED-Based)

```bash
python3 scripts/baseline.py
```

Reads predicate observations directly from PRED annotations in validator logs and applies the same statistical fault localization algorithm.

### Statistics & Evaluation

```bash
python3 scripts/analyze.py              # Print confusion matrix for all configurations
```

Used by both approaches to evaluate prediction accuracy on failure modes (Incompatible, Insufficient, Agreement).

## Project Structure

- `src/` - Core library code shared by both approaches
  - `models/` - Message types and predicate definitions
  - `analysis/` - Statistical fault localization algorithms
  - `utils/` - Network topology and configuration
- `scripts/` - Analysis pipeline entry points
  - `main.py` - Message-based (consensus-aware) approach
  - `baseline.py` - Baseline (PRED-based) approach
  - `analyze.py` - Shared evaluation and statistics
- `cache/` - Generated pickle caches (gitignored)
- `data/` - Validator logs and results
