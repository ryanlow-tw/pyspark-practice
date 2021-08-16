This is an attempt to pyspark

to activate venv

```bash
source venv/bin/activate
```

to run tests

from the root folder

```bash
pytest
```

to submit spark job

```bash
spark-submit \
    --master local \
    --py-files dist/test_sparksubmit-0.1-py3.9.egg \
    run_book_analytics.py \
    <FILE_INPUT_PATH>
```

to lay the egg

```bash
python setup.py bdist_egg
```
