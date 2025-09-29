# Insider Threat Detection using LLM

This project aims to detect insider threats from system logs using Large Language Models (LLMs). It uses the CERT Insider Threat Dataset for training and evaluation.

## Project Structure

```
.
├── .gitignore
├── data/
│   ├── processed/
│   └── raw/
│       ├── r1/
│       └── ...
├── notebooks/
├── requirements.txt
├── src/
│   ├── data_processing/
│   ├── evaluation/
│   ├── feature_engineering/
│   ├── models/
│   └── training/
└── README.md
```

## Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/944sahil/insider-threat-detection.git
   cd insider-threat-detection
   ```

2. **Create a virtual environment and activate it:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. **Install the dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## Usage

- **Data**: Place the raw CERT dataset files in the `data/raw/` directory. Processed data will be saved in `data/processed/`.
- **Notebooks**: Use the Jupyter notebooks in the `notebooks/` directory for exploratory data analysis and experiments.
- **Source Code**: The main source code for data processing, feature engineering, model training, and evaluation is in the `src/` directory.
