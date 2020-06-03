import pytest

import numpy as np

from ggjw.tasks.segmentation.preprocessor import ProjectResampleNormalizePreprocessor


@pytest.mark.parametrize('model_input_size', [(10, 24), (32, 32)])
def test_preprocessor(model_input_size, n_channels=1):
    '''
    '''
    preprocessor = ProjectResampleNormalizePreprocessor(
        model_input_size=model_input_size, n_channels=n_channels)
    test_image = np.random.randn(30, 50, 50, n_channels)
    prep_image = preprocessor.preprocess(test_image)
    assert prep_image.shape == (1, ) + model_input_size + (n_channels, )
