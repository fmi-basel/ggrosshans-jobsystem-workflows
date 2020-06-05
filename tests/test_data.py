import glob
import os

TEST_DATA = {
    key: os.path.join(os.path.dirname(__file__), 'data', 'worms_from_dic', key)
    for key in ['img', 'segm']
}

NUM_EXPECTED = 5


def test_data_available():
    '''verify if the test data is available.
    '''
    assert len(glob.glob(os.path.join(TEST_DATA['img'],
                                      '*.stk'))) == NUM_EXPECTED
    assert len(glob.glob(os.path.join(TEST_DATA['segm'],
                                      '*.tif'))) == NUM_EXPECTED
