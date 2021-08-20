from ggjw.tasks.data_management.stack_files import StackFilesTask
from ggjw.workflows.stack_files import StackFilesWorkflow

import pytest
import os
import luigi
import glob
import numpy as np
from skimage import io
from tifffile import imwrite
import shutil

# Test data for workflow
s = 5
t = 11
z = 7
x = 1365
y = 1987

cwd = os.path.join(os.path.dirname(__file__), "data")
os.getcwd()
os.mkdir(cwd + "/test_data")
test_data=cwd+"/test_data"

# create images
for s in np.linspace(1, s, s):
    for t in np.linspace(1, t, t):
        image_to_save = np.ones(z, x, y)
        img_name = test_data+"/w1Marit-488-BF-Cam0_s"+str(s)+"_t"+str(t)+".stk"
        imwrite(img_name, image_to_save)

# TEST_DATA = {
#     # create images with size 8*1200*1200 with naming _s..._t.... in a tmpdir
#     key: os.path.join(os.path.dirname(__file__), 'data', 'worms_from_dic', key)
#     for key in [
#         'img',
#     ]
# }


@pytest.mark.parametrize(
    'workflow', [StackFilesTask, StackFilesWorkflow])  # workflows you want to test
def test_stacking_task(tmpdir, workflow):

    input_folder = test_data
    test_dir = tmpdir

    # runns the workflow noted in the pytest.mark.parameterize with given input folder
    result = luigi.build([
        workflow(image_folder=input_folder,
                 image_file_pattern='*.stk',
                 data_format='st',
                 output_folder=str(test_dir))
    ],
        local_scheduler=True,
        detailed_summary=True)

    # TEST if the worfklow has ran
    if result.status not in [
        luigi.execution_summary.LuigiStatusCode.SUCCESS,
        luigi.execution_summary.LuigiStatusCode.SUCCESS_WITH_RETRY
    ]:
        raise RuntimeError(
            'Luigi failed to run the workflow! Exit code: {}'.format(result))

    # TEST if there is an output folder testdir+"/temp"
    assert os.path.exists(test_dir+"/temp")

    # TEST if gere are _s number of files in the folder
    img_list = glob.glob(test_dir+"/temp/s*.tiff")
    assert np.size(img_list) == s

    # TEST if the images that are in there, has size TZCXY
    for image in img_list:
        img = io.imread(image)
        assert np.shape(img) == [t, z, 1, x, y]


    if os.path.exists(...):
        shutil.rmtree(..)