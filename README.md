This is an attempt to pyspark

to activate venv

```bash
source venv/bin/activate
```

to submit spark job

```bash
spark-submit \
    --master local \
    --py-files dist/test_sparksubmit-0.1-py3.9.egg \
    run_book_analytics.py \
    data/books3000.csv
```

to lay the egg

```bash
python setup.py bdist_egg
```
