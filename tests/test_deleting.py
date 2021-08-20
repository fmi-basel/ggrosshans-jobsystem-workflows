from tifffile.tifffile import imwrite
from ggjw.tasks.data_management.delete_files import DeleteFilesTask
from ggjw.workflows.delete_files import DeleteFilesWorkflow

import pytest
import os
import luigi
import glob
import numpy as np
from skimage import io
from tifffile import imwrite
import pandas as pd


# create goodworms CSV file
cwd = os.getcwd()
os.mkdir(cwd + "/test_data")
test_data=cwd+"/test_data"

csv = {'Position': [1,2,3], 'Quality': [1,0,1], "Hatch":[2,2,0],"Escape":[4,2,6]}
csv = pd.DataFrame(csv)
csv.to_csv(test_data + "/goodworms.csv")

#create images
n_original=15
n_deleted=8
for s in [1,2,3]:
    for t in [1,2,3,4,5]:
        image_to_save=np.ones([5,20,20])
        img_name = test_data+"/w1Marit-488-BF-Cam0_s"+str(s)+"_t"+str(t)+".stk"
        imwrite(img_name, image_to_save)


#run test
@pytest.mark.parametrize(
    'workflow', [DeleteFilesTask, DeleteFilesWorkflow]) #workflows you want to test
def test_delete_task(tmpdir, workflow):
    print('hello')
    input_folder = test_data
    test_dir = tmpdir
    print(test_dir)
    return

    #TEST the the test files are created
    img_list = glob.glob(test_dir+"*.stk")
    assert np.size(img_list)==n_original

    #runns the workflow noted in the pytest.mark.parameterize with given input folder
    result = luigi.build([
    workflow(image_folder=input_folder,  # where the images are
            csv_document=input_folder+"/goodworms.csv",
            image_file_pattern='*.stk',  
            data_format='st')
    ],
        local_scheduler=True,
        detailed_summary=True)
        

    #TEST if the worfklow has ran
    if result.status not in [
        luigi.execution_summary.LuigiStatusCode.SUCCESS,
        luigi.execution_summary.LuigiStatusCode.SUCCESS_WITH_RETRY
    ]:
        raise RuntimeError(
            'Luigi failed to run the workflow! Exit code: {}'.format(result))


    #TEST if original and deleted files csv is there
    assert os.path.exists(test_dir+"/original_files.csv")
    assert os.path.exists(test_dir+"/deleted_files.csv")

    #TEST if gere are _s number of files in the folder
    img_list = glob.glob(test_dir+"*.stk")
    assert np.size(img_list)==n_deleted


    

    