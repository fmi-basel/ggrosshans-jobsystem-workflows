from ggjw.tasks.data_management.stack_files import StackFilesTask
from ggjw.workflows.stack_files import StackFilesWorkflow

import pytest
import os
import luigi
import glob
import numpy as np
from skimage import io
from tifffile import imwrite


@pytest.mark.parametrize(
    'workflow', [StackFilesTask, StackFilesWorkflow])  # workflows you want to test
def test_stacking_task(tmpdir, workflow):
    # Test data for workflow
    s = 3
    t = 5
    z = 7
    x = 435
    y = 432

    # create data
    test_dir = tmpdir
    print(test_dir)
    output_folder_test = os.path.join(test_dir, "results")
    os.mkdir(output_folder_test)

    # create images in test_dir
    for s in range(1, s + 1):
        for t in range(1, t + 1):
            image_to_save = np.ones([z, x, y], dtype="uint16")
            img_name = os.path.join(
                test_dir, "w1Marit-488-BF-Cam0_s" + str(int(s)) + "_t" + str(int(t)) + ".tiff")
            imwrite(img_name, image_to_save)

    print("images are created")
    # runs the workflow noted in the pytest.mark.parameterize with given input folder
    result = luigi.build([
        workflow(image_folder=test_dir,
                 image_file_pattern='*w1Marit-488-BF-Cam0*.tiff',
                 data_format='st',
                 output_folder=output_folder_test)
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
    assert os.path.exists(os.path.join(output_folder_test, "temp"))

    # TEST if gere are _s number of tiff files in the folder
    img_list = glob.glob(os.path.join(output_folder_test, "temp", "s*.tiff"))
    assert np.size(img_list) == s

    # TEST if the images that are in there, has size TZCXY
    for image in img_list:
        img = io.imread(image)
        # Saving imagej compatible drops singleton channel dimension. Hence we don't check for it.
        assert np.shape(img) == (t, z, x, y)
