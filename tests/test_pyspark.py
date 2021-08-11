import pytest
from tests import SPARK
from pyspark_scripts.pyspark_script import get_average_rating

@pytest.fixture(name="df_columns")
def fixture_columns():
    columns = [
        'id',
        'author',
        'title',
        'image_url',
        'small_image_url',
        'price',
        'books_count',
        'isbn',
        'isbn13',
        'original_publication_year',
        'original_title',
        'language_code',
        'average_rating'
    ]
    return columns

def test_should_return_average_rating(df_columns) -> None:

    test_data = [
        [51,"author1","book1","test_url","test_url",1461,178,1416914285,9.78142E+12,2007,"City of Bones","eng",2.00],
        [53,"author2","book2","test_url","test_url",1461,178,1416914285,9.78142E+12,2007,"City of Bones","eng",3.00],
        [54,"author3","book3","test_url","test_url",1461,178,1416914285,9.78142E+12,2007,"City of Bones","eng",4.00]
            ]

    test_dataframe = SPARK.createDataFrame(test_data, df_columns)

    actual = get_average_rating(test_dataframe)

    expected = 3.00

    assert actual == expected





