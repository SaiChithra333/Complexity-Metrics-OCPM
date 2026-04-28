# OCPN Complexity Metrics System

A comprehensive Python framework for analyzing and quantifying the complexity of Object-Centric Petri Net (OCPN) models. This system computes structural and behavioral complexity metrics, visualizes process models, and enables comparative analysis across multiple datasets.

## Overview

Object-Centric Petri Nets (OCPNs) are advanced process modeling notations that capture the interactions between multiple object types and activities in complex business processes. This framework provides:

- **Complexity Metrics**: Multiple normalized metrics to quantify model complexity
- **Pipeline Processing**: End-to-end data ingestion, validation, and OCPN generation
- **Real-world Data Support**: Processing logs from multiple formats (JSON, CSV, XML)
- **Visualization**: Dashboard generation and comparative analysis charts
- **Comparative Analysis**: Side-by-side model comparison and reporting

## Features

### Core Metrics

The system computes four normalized complexity metrics (each normalized to [0, 1]):

1. **OID (Object Interaction Density)** - Structural metric measuring interconnectedness of object types
2. **SAR (Shared Activity Ratio)** - Interaction-based metric for activity complexity
3. **AFO (Activity Fan-Out)** - Distribution of activity involvement across object types
4. **RE (Relation Entropy)** - Distribution-aware metric capturing relation diversity
5. **C(G)** - Composite complexity score (equal-weight average of normalized metrics)

### Informational Metrics

- **OTC** - Object-Type Count (context information)
- **AORC** - Activity-Object Relation Count
- **OC** - Object Coupling (reference metric)

### Advanced Features

- **OII** - Object Isolation Index (diagnostic)
- **DIAM** - Interaction Graph Diameter (structural)

## Project Structure

```
ocpn_complexity/
├── core/                          # Core data structures
│   ├── __init__.py
│   └── model.py                   # OCPNModel class definition
├── metrics/                       # Complexity computation
│   ├── __init__.py
│   ├── complexity.py              # Metric implementations
│   └── comparator.py              # Model comparison utilities
├── pipeline/                      # Data processing pipeline
│   ├── __init__.py
│   ├── pipeline.py                # Main pipeline orchestrator
│   ├── parser.py                  # Data format parsing
│   ├── validator.py               # Schema validation
│   ├── extractor.py               # Event/relation extraction
│   ├── cleaner.py                 # Data cleaning
│   ├── schema.py                  # Data schema definitions
│   └── real_world/                # Real-world data handlers
│       ├── downloader.py
│       └── ocel2_parser.py
├── visualization/                 # Output generation
│   ├── __init__.py
│   └── plotter.py                 # Dashboard and chart generation
├── data/                          # Sample data generation
│   └── synthetic_datasets.py      # Synthetic dataset generators
├── examples/                      # Sample models
│   └── sample_models.py           # Pre-defined OCPN examples
├── tests/                         # Unit tests
│   ├── test_metrics.py
│   └── test_pipeline.py
├── real_world_data/               # Real-world datasets
│   ├── angular_github_commits_ocel.csv
│   ├── enron_top_10000.json
│   ├── hospital_log_high.json
│   ├── ocel2-p2p.json
│   └── order-management.json
├── output/                        # Generated outputs
│   └── [various_output_directories]
├── main.py                        # Main entry point
├── pipeline_demo.py               # Pipeline demonstration
├── real_world_runner.py           # Real-world data analysis
└── requirements.txt               # Python dependencies
```

## Installation

### Prerequisites

- Python 3.8 or higher
- pip package manager

### Setup

1. Clone or download the project:

```bash
cd ocpn_complexity
```

2. Create a virtual environment (recommended):

```bash
python -m venv venv
```

3. Activate the virtual environment:

**Windows:**

```bash
venv\Scripts\activate
```

**macOS/Linux:**

```bash
source venv/bin/activate
```

4. Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Quick Start — Single Model Analysis

### Main Command
### Real-World Data Analysis

Analyze real-world datasets:

```bash
python real_world_runner.py
```
### Extra (To Understand the better)

Run the main demo to analyze pre-defined OCPN models:

```bash
python main.py
```

This demonstrates:

- Computing all metrics for a single OCPN model
- Formatting comprehensive reports
- Comparing multiple models side-by-side
- Generating per-dataset dashboard visualizations
- Creating cross-model summary dashboards

### Pipeline Demo — End-to-End Processing

Process different data formats and generate models:

```bash
python pipeline_demo.py
```

This includes four demonstrations:

1. **Minimal dict input** - Simple hand-crafted model
2. **E-Commerce log** - Realistic noisy log with full report
3. **Hospital CSV data** - CSV processing with activity frequency filtering
4. **Logistics OCEL JSON** - Multi-dataset comparison with dashboards

## Usage Examples

### Creating a Custom OCPN Model

```python
from core.model import OCPNModel
from metrics.complexity import ComplexityMetrics

# Define model components
model = OCPNModel(
    objects=['Order', 'Item', 'Vehicle'],
    activities=['Create Order', 'Load Cargo', 'Start Route'],
    relations=[
        ('Create Order', 'Order'),
        ('Load Cargo', 'Item'),
        ('Load Cargo', 'Vehicle'),
        ('Start Route', 'Vehicle'),
    ],
    name="Logistics Process"
)

# Compute metrics
metrics = ComplexityMetrics(model)
report = metrics.report()
print(report)
```

### Using the Pipeline for Data Processing

```python
from pipeline.pipeline import OCPNPipeline

# Initialize pipeline
pipeline = OCPNPipeline(verbose=True)

# Process raw event data
raw_data = [
    {"activity": "Create Order", "object_type": "Order"},
    {"activity": "Load Cargo", "object_type": "Item"},
    # ... more events
]

# Generate OCPN model and metrics
result = pipeline.run(raw_data, name="My Process")
result.print_full_report()
```

### Comparing Multiple Models

```python
from metrics.comparator import ModelComparator
from examples.sample_models import ALL_MODELS

# Create comparator with multiple models
comparator = ModelComparator(ALL_MODELS)

# Generate comparison report
comparison = comparator.compare_all()
print(comparison)
```

## Dependencies

- **networkx** (≥3.0) - Graph data structures and algorithms
- **matplotlib** (≥3.7) - Visualization and plotting
- **numpy** (≥1.24) - Numerical computing

Install all dependencies:

```bash
pip install -r requirements.txt
```

## Key Components

### OCPNModel

Central data structure representing an Object-Centric Petri Net as G = (O, A, R):

- **O** — Set of object types
- **A** — Set of activities
- **R** — Relations (activity-object pairs)

### ComplexityMetrics

Computes all normalized complexity metrics and generates formatted reports with:

- Individual metric values
- Composite complexity score
- Diagnostic information
- Structured output for comparison

### OCPNPipeline

End-to-end data processing pipeline handling:

- Format parsing (JSON, CSV, dict)
- Schema validation
- Event extraction and normalization
- Relation construction
- OCPN model generation

### ModelComparator

Enables comparative analysis across multiple models:

- Side-by-side metric comparison
- Ranking by complexity
- Statistical summaries
- Dashboard visualization

### Visualization

Generates publication-ready charts and dashboards:

- Per-dataset metric visualizations
- Cross-model comparison charts
- Interaction graph diagrams

## Output

The system generates:

1. **Console Reports** - Formatted text output with metric tables
2. **PNG Dashboards** - Visual analysis of individual datasets
3. **Comparison Charts** - Cross-model analysis visualizations
4. **Data Files** - CSV/JSON export of computed metrics

Outputs are saved to the `output/` directory organized by dataset.

## Testing

Run the test suite:

```bash
python -m pytest tests/
```

Test coverage includes:

- Metric computation accuracy
- Pipeline data processing
- Model validation
- Comparative analysis

## Contributing

To extend the system:

1. Add new metrics in `metrics/complexity.py`
2. Implement new data parsers in `pipeline/parser.py`
3. Create new visualizations in `visualization/plotter.py`
4. Add test cases in `tests/`

## Performance Considerations

- **Large models** (100+ object types): Metric computation is O(|O|² + |A| × |O|)
- **Real-world datasets**: Process logs are streamed to minimize memory footprint
- **Visualization**: Dashboard generation may take 10-30 seconds depending on complexity

## Limitations

- Current implementation optimized for models with < 500 object types
- OCEL2 format support is partial (focus on process discovery aspects)
- Visualization assumes matplotlib backend availability

## License

This project is part of academic research in Process Mining and Analysis (PMA). Usage restrictions apply per institutional guidelines.

## Contact & Support

For questions or issues:

- Review example scripts in `examples/` and `pipeline_demo.py`
- Check test files for usage patterns
- Examine real-world data processing in `real_world_runner.py`

## References

- Object-Centric Process Mining: Concepts and Applications
- Petri Net Theory and Applications
- Process Mining: Data Science in Action

---

**Last Updated:** 2026
**Version:** 4.0
