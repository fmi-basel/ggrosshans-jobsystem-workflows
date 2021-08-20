import glob
import os
from os.path import join

import luigi
import numpy as np
import pandas as pd
import pytest
from tifffile import imwrite

from ggjw.tasks.data_management.delete_files import DeleteFilesTask
from ggjw.workflows.delete_files import DeleteFilesWorkflow


# run test
@pytest.mark.parametrize(
    'workflow', [DeleteFilesTask, DeleteFilesWorkflow])  # workflows you want to test
def test_delete_task(tmpdir, workflow):
    test_dir = tmpdir

    csv = {'Position': [1, 2, 3], 'Quality': [1, 0, 1], "Hatch": [2, 2, 0], "Escape": [4, 2, 6]}
    csv = pd.DataFrame(csv)
    csv.to_csv(join(test_dir, "goodworms.csv"))

    # create images
    n_original = 15
    n_deleted = 8
    for s in [1, 2, 3]:
        for t in [1, 2, 3, 4, 5]:
            image_to_save = np.ones([5, 20, 20])
            img_name = join(test_dir, "w1Marit-488-BF-Cam0_s" + str(s) + "_t" + str(t) + ".stk")
            imwrite(img_name, image_to_save)

    # TEST the the test files are created
    img_list = glob.glob(join(test_dir, "*.stk"))
    assert np.size(img_list) == n_original

    # runns the workflow noted in the pytest.mark.parameterize with given input folder
    result = luigi.build([
        workflow(image_folder=test_dir,  # where the images are
                 csv_document=join(test_dir, "goodworms.csv"),
                 image_file_pattern='*.stk',
                 data_format='st')
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

    # TEST if original and deleted files csv is there
    assert os.path.exists(join(test_dir, "original_files.csv"))
    assert os.path.exists(join(test_dir, "deleted_files.csv"))

    # TEST if gere are _s number of files in the folder
    img_list = glob.glob(test_dir + "*.stk")
    assert np.size(img_list) == n_deleted
