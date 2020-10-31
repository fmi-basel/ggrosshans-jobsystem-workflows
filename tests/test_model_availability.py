import os
import glob
import pytest

from ggjw.workflows.worm_segmentation import MODEL_FOLDER_FOR_VERSION


@pytest.mark.parametrize('version', list(MODEL_FOLDER_FOR_VERSION.keys()))
def test_model_availability(version):
    '''check if model versions are available.
    '''
    folder = MODEL_FOLDER_FOR_VERSION.get(version, False)
    assert folder  # make sure the version is registered
    # make sure the folder exists:
    assert os.path.exists(folder)  
    # make sure it's not an empty folder:
    assert glob.glob(os.path.join(folder, '*'))  
