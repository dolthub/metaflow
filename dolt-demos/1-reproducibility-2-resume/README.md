## Usage:

1. Python3.8 installed
```which python3``

2. Poetry installed
```
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python3 - --version=1.1.0b2
poetry env use python3.8
```

3. Dependencies installed
```poetry install``

4. Start server
```
poetry run jupyter notebook .
```

## Notes

1. Instantiate `foo` dolt repository locally.
2. Run first flow that:
    1) depends on a certain version of a table.
    2) fails unless ran within 1 minute of second flow.
3. Run second flow.
4. Successfully run the first flow.
