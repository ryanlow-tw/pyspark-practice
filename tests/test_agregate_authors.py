import pytest
from tests import SPARK
from pyspark_scripts.aggregate_authors import get_aggregated_authors

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

def test_should_return_aggregated_authors_if_no_params_given(df_columns) -> None:

    test_data = [
        [51,"author1","book1","test_url","test_url",1461,178,1416914285,9.78142E+12,1970,"City of Bones","eng",2.00],
        [53,"author1","book2","test_url","test_url",1461,178,1416914285,9.78142E+12,1970,"City of Bones","eng",3.00],
        [54,"author2","book3","test_url","test_url",1461,178,1416914285,9.78142E+12,2000,"City of Bones","eng",4.00],
        [55,"author2","book3","test_url","test_url",1461,178,1416914285,9.78142E+12,2000,"City of Bones","eng",4.00]
            ]

    test_dataframe = SPARK.createDataFrame(test_data, df_columns)

    actual = get_aggregated_authors(test_dataframe)

    result1 = {}
    result2 = {}
    result3 = {}
    result4 = {}
    expected_data1 = test_data[0]
    expected_data2 = test_data[1]
    expected_data3 = test_data[2]
    expected_data4 = test_data[3]

    for i, col in enumerate(df_columns):
        result1[col] = expected_data1[i]
        result2[col] = expected_data2[i]
        result3[col] = expected_data3[i]
        result4[col] = expected_data4[i]

    expected = [
        {"author": "author1",
        "book_details": [result1, result2]},
        {"author": "author2",
        "book_details": [result3, result4]},
    ]
    assert actual == expected

def test_should_return_json_by_author(df_columns) -> None:

    test_data = [
        [51,"author1","book1","test_url","test_url",1461,178,1416914285,9.78142E+12,1970,"City of Bones","eng",2.00],
        [53,"author1","book2","test_url","test_url",1461,178,1416914285,9.78142E+12,1970,"City of Bones","eng",3.00],
        [54,"author2","book3","test_url","test_url",1461,178,1416914285,9.78142E+12,2000,"City of Bones","eng",4.00],
        [55,"author2","book3","test_url","test_url",1461,178,1416914285,9.78142E+12,2000,"City of Bones","eng",4.00]
            ]

    test_dataframe = SPARK.createDataFrame(test_data, df_columns)

    actual = get_aggregated_authors(test_dataframe, "author1")

    result1 = {}
    result2 = {}
    expected_data1 = test_data[0]
    expected_data2 = test_data[1]

    for i, col in enumerate(df_columns):
        result1[col] = expected_data1[i]
        result2[col] = expected_data2[i]

    expected = [
        {"author": "author1",
        "book_details": [result1, result2]}
    ]

    assert actual == expected