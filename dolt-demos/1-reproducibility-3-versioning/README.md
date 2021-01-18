## Usage:

1. Python3.8 installed
```
which python3
```

2. Poetry installed
```
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python3 - --version=1.1.0b2
poetry env use python3.8
```

3. Dependencies installed
```
poetry install
```

4. Start server
```
poetry run jupyter notebook .
```

## Notes

1. Instantiate `foo` dolt repository locally with two
    versions of `bar` table.
2. Run two flows, respectively sourcing two different versions of `bar`
    and writing two different versions of `baz`.
