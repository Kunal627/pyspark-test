import pytest, sys
import pyspark

from pathlib import Path # if you haven't already done so
file = Path(__file__).resolve()
parent, root = file.parent, file.parents[1]
sys.path.append(str(root))

from src import sparkcode


@pytest.fixture(scope="session")
def spark_context(request):
    """ fixture for creating a spark context
    Args:
        request: pytest.FixtureRequest object
    """
    spark_context = pyspark.SparkContext('local[*]')

#    testdata = spark_context.textFile(r'C:\Users\chandk10\OneDrive - Medtronic PLC\Work\pyspark-test\sourcedata\testdata.txt')   
    request.addfinalizer(lambda: spark_context.stop())

    return spark_context


def test_processfile(spark_context):
    srcpath = r'C:\Users\chandk10\OneDrive - Medtronic PLC\Work\pyspark-test\sourcedata\testfile.txt'
    results = sparkcode.processfile(spark_context,srcpath)
    
    expected_results = ['this is a test file. this is a python test data']
    assert results.collect() == expected_results
