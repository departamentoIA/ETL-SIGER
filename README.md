# ETL-Polars
ETL process with Polars.

## 🌎 Repository Structure
```
ETL-Polars/
│
├── main.py
├── .gitignore
├── env/                # Virtual enviroment
└── requirements.txt
└── pkg                 # Contains all nedded files
    └── __init__.py     # Specifies that folder 'pkg' is a Python package
    └── config.py       # Contains all configuration params
```


## ✨ Details

**main.py**

## 🚀 How to run locally
1. Clone this repository:
```
git clone https://github.com/departamentoIA/ETL-Polars.git
```
2. Set virtual environment and install dependencies.

For Windows:
```
python -m venv env
env/Scripts/activate
pip install -r requirements.txt
```
For Linux:
```
python -m venv env && source env/bin/activate && pip install -r requirements.txt
```
3. Run "main.py".