import pytest
from tests import SPARK
from pyspark_scripts.pyspark_script import get_formatted_average_rating, get_highly_rated_books, get_less_rated_books

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

    actual = get_formatted_average_rating(test_dataframe)

    expected = {"mean": "3.00"}

    assert actual == expected


def test_should_return_average_rating_format_to_2dp(df_columns) -> None:

    test_data = [
        [51,"author1","book1","test_url","test_url",1461,178,1416914285,9.78142E+12,2007,"City of Bones","eng",4.222],
        [53,"author2","book2","test_url","test_url",1461,178,1416914285,9.78142E+12,2007,"City of Bones","eng",4.222],
        [54,"author3","book3","test_url","test_url",1461,178,1416914285,9.78142E+12,2007,"City of Bones","eng",4.222]
            ]

    test_dataframe = SPARK.createDataFrame(test_data, df_columns)

    actual = get_formatted_average_rating(test_dataframe)

    expected = {"mean": "4.22"}

    assert actual == expected

def test_should_return_one_highly_rated_book(df_columns) -> None:

    test_data = [
        [51,"author1","low_rated_book1","test_url","test_url",1461,178,"1416914285",9.78142E+12,2007,"City of Bones","eng",1.0],
        [53,"author2","low_rated_book2","test_url","test_url",1461,178,"1416914285",9.78142E+12,2007,"City of Bones","eng",2.0],
        [54,"author3","high_rated_book3","test_url","test_url",1461,178,"1416914285",9.78142E+12,2007,"City of Bones","eng",5.0]
            ]

    test_dataframe = SPARK.createDataFrame(test_data, df_columns)

    actual = get_highly_rated_books(test_dataframe)

    result = {}
    expected_data = test_data[2]

    for i, col in enumerate(df_columns):
        result[col] = expected_data[i]

    expected = {"highly_rated":[result]}

    assert actual == expected

def test_should_return_multiple_highly_rated_books(df_columns) -> None:

    test_data = [
        [51,"author1","low_rated_book1","test_url","test_url",1461,178,"1416914285",9.78142E+12,2007,"City of Bones","eng",1.0],
        [53,"author2","low_rated_book2","test_url","test_url",1461,178,"1416914285",9.78142E+12,2007,"City of Bones","eng",1.0],
        [54,"author3","high_rated_book3","test_url","test_url",1461,178,"1416914285",9.78142E+12,2007,"City of Bones","eng",5.0],
        [54,"author3","high_rated_book3","test_url","test_url",1461,178,"1416914285",9.78142E+12,2007,"City of Bones","eng",5.0]
            ]

    test_dataframe = SPARK.createDataFrame(test_data, df_columns)

    actual = get_highly_rated_books(test_dataframe)

    result1 = {}
    result2 = {}
    expected_data1 = test_data[2]
    expected_data2 = test_data[3]

    for i, col in enumerate(df_columns):
        result1[col] = expected_data1[i]
        result2[col] = expected_data2[i]

    expected = {"highly_rated":[result1, result2]}

    assert actual == expected

def test_should_return_one_less_rated_book(df_columns) -> None:

    test_data = [
        [51,"author1","low_rated_book1","test_url","test_url",1461,178,"1416914285",9.78142E+12,2007,"City of Bones","eng",1.0],
        [53,"author2","low_rated_book2","test_url","test_url",1461,178,"1416914285",9.78142E+12,2007,"City of Bones","eng",5.0],
        [54,"author3","high_rated_book3","test_url","test_url",1461,178,"1416914285",9.78142E+12,2007,"City of Bones","eng",5.0]
            ]

    test_dataframe = SPARK.createDataFrame(test_data, df_columns)

    actual = get_less_rated_books(test_dataframe)

    result = {}
    expected_data = test_data[0]

    for i, col in enumerate(df_columns):
        result[col] = expected_data[i]

    expected = {"less_rated":[result]}

    assert actual == expected


def test_should_return_multiple_less_rated_books(df_columns) -> None:

    test_data = [
        [51,"author1","low_rated_book1","test_url","test_url",1461,178,"1416914285",9.78142E+12,2007,"City of Bones","eng",1.0],
        [53,"author2","low_rated_book2","test_url","test_url",1461,178,"1416914285",9.78142E+12,2007,"City of Bones","eng",1.0],
        [54,"author3","high_rated_book3","test_url","test_url",1461,178,"1416914285",9.78142E+12,2007,"City of Bones","eng",5.0],
        [54,"author3","high_rated_book3","test_url","test_url",1461,178,"1416914285",9.78142E+12,2007,"City of Bones","eng",5.0]
            ]

    test_dataframe = SPARK.createDataFrame(test_data, df_columns)

    actual = get_less_rated_books(test_dataframe)

    result1 = {}
    result2 = {}
    expected_data1 = test_data[0]
    expected_data2 = test_data[1]

    for i, col in enumerate(df_columns):
        result1[col] = expected_data1[i]
        result2[col] = expected_data2[i]

    expected = {"less_rated":[result1, result2]}

    assert actual == expected
