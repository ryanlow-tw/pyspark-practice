This is an attempt to practice manipulating data in pyspark

to activate venv

```bash
source venv/bin/activate
```

```bash
spark-submit \
    --master local \
    --py-files dist/test_sparksubmit-0.1-py3.9.egg \
    run_book_analytics.py \
    data/books3000.csv
```

```bash
python setup.py bdist_egg
```
