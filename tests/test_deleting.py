import glob
import os

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

    # create testdata
    test_dir = tmpdir

    csv = {'Position': [1, 2, 3], 'Quality': [1, 0, 1],
           "Hatch": [2, 2, 0], "Escape": [4, 2, 6]}
    csv = pd.DataFrame(csv)
    csv.to_csv(os.path.join(test_dir, "goodworms.csv"))

    # create images
    n_original = 15
    n_deleted = 8
    for s in [1, 2, 3]:
        for t in [1, 2, 3, 4, 5]:
            image_to_save = np.ones([5, 20, 20])
            img_name = os.path.join(
                test_dir, "w1Marit-488-BF-Cam0_s" + str(s) + "_t" + str(t) + ".stk")
            imwrite(img_name, image_to_save)
    
    files_to_be_there=[os.path.join(test_dir, "w1Marit-488-BF-Cam0_s" + str(1) + "_t" + str(2) + ".stk"),
                       os.path.join(test_dir, "w1Marit-488-BF-Cam0_s" + str(1) + "_t" + str(3) + ".stk"),
                       os.path.join(test_dir, "w1Marit-488-BF-Cam0_s" + str(1) + "_t" + str(4) + ".stk"),
                       os.path.join(test_dir, "w1Marit-488-BF-Cam0_s" + str(3) + "_t" + str(1) + ".stk"),
                       os.path.join(test_dir, "w1Marit-488-BF-Cam0_s" + str(3) + "_t" + str(2) + ".stk"),
                       os.path.join(test_dir, "w1Marit-488-BF-Cam0_s" + str(3) + "_t" + str(3) + ".stk"),
                       os.path.join(test_dir, "w1Marit-488-BF-Cam0_s" + str(3) + "_t" + str(4) + ".stk"),
                       os.path.join(test_dir, "w1Marit-488-BF-Cam0_s" + str(3) + "_t" + str(5) + ".stk")]
     

    # TEST if the files are created
    img_list = glob.glob(os.path.join(test_dir, "*.stk"))
    print(str(np.size(img_list)))
    print(test_dir)

    assert np.size(img_list) == n_original

    # runs the workflow noted in the pytest.mark.parameterize with given input folder
    result = luigi.build([
        workflow(image_folder=test_dir,  # where the images are
                 csv_document=os.path.join(test_dir, "goodworms.csv"),
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
    assert os.path.exists(os.path.join(test_dir, "original_files.csv"))
    assert os.path.exists(os.path.join(test_dir, "deleted_files.csv"))

    original_files = pd.read_csv(os.path.join(test_dir, "original_files.csv"))
    deleted_files = pd.read_csv(os.path.join(test_dir, "deleted_files.csv"))

    remaining_files = pd.concat([original_files, deleted_files]).drop_duplicates(keep=False)
    for file_path in remaining_files['File']:
        assert os.path.exists(file_path)
        assert file_path in files_to_be_there

    # TEST if gere are _s number of files in the folder
    img_list = glob.glob(os.path.join(test_dir, "*.stk"))
    assert np.size(img_list) == n_deleted
