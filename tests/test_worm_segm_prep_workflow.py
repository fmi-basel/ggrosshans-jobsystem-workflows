import os
from glob import glob

import luigi
import numpy as np
import pandas

from faim_luigi.targets.image_target import TiffImageTarget
from ggjw.workflows import PrepareRandomlySelectedImagesForAnnotationWorkflow
from ggjw.workflows import PrepareManuallySelectedImagesForAnnotationWorkflow

TEST_DATA = os.path.join(os.path.dirname(__file__), 'data', 'worms_from_dic',
                         'img')


def test_random_prep_workflow(tmpdir, num_samples=3):
    '''
    '''
    input_folder = TEST_DATA
    output_folder = tmpdir

    result = luigi.build([
        PrepareRandomlySelectedImagesForAnnotationWorkflow(
            input_folder=input_folder,
            output_folder=str(output_folder),
            file_pattern='*stk',
            seed=7,
            num_samples=num_samples)
    ],
                         local_scheduler=True,
                         detailed_summary=True)

    if result.status not in [
            luigi.execution_summary.LuigiStatusCode.SUCCESS,
            luigi.execution_summary.LuigiStatusCode.SUCCESS_WITH_RETRY
    ]:
        raise RuntimeError(
            'Luigi failed to run the workflow! Exit code: {}'.format(result))

    # collect output images and make sure they are num_samples
    output_paths = glob(os.path.join(output_folder, 'img', '*'))
    assert len(output_paths) == num_samples

    for output_path in output_paths:
        output_image = TiffImageTarget(output_path).load()

        # check against original image.
        reference_image = TiffImageTarget(
            os.path.join(
                input_folder,
                os.path.splitext(os.path.basename(output_path))[0] +
                '.stk')).load()
        assert np.all(output_image == reference_image.min(axis=0))

        # and check shape against segmentation.
        output_segm = TiffImageTarget(
            os.path.join(output_folder, 'segm',
                         os.path.basename(output_path))).load()
        assert output_segm.shape == output_image.shape



def test_selected_prep_workflow(tmpdir):
    '''
    '''
    input_folder = os.path.dirname(TEST_DATA)
    test_csv = tmpdir / 'test-selection.csv'
    output_folder = tmpdir / 'prep'

    pandas.DataFrame({'timepoint': ['t21', 't334']})\
                     .assign(folder='img', position='s37')\
                     .to_csv(test_csv, index=False)

    result = luigi.build([
        PrepareManuallySelectedImagesForAnnotationWorkflow(
            input_folder=input_folder,
            input_file=test_csv,
            output_folder=str(output_folder),
            file_pattern='*')
    ],
                         local_scheduler=True,
                         detailed_summary=True)

    if result.status not in [
            luigi.execution_summary.LuigiStatusCode.SUCCESS,
            luigi.execution_summary.LuigiStatusCode.SUCCESS_WITH_RETRY
    ]:
        raise RuntimeError(
            'Luigi failed to run the workflow! Exit code: {}'.format(result))

    # collect output images and make sure they are num_samples
    output_paths = glob(os.path.join(output_folder, 'img', '*'))
    assert len(output_paths) == 2

    for output_path in output_paths:
        output_image = TiffImageTarget(output_path).load()

        # check against original image.
        reference_image = TiffImageTarget(
            os.path.join(
                input_folder, 'img',
                os.path.splitext(os.path.basename(output_path))[0] +
                '.stk')).load()
        assert np.all(output_image == reference_image.min(axis=0))

        # and check shape against segmentation.
        output_segm = TiffImageTarget(
            os.path.join(output_folder, 'segm',
                         os.path.basename(output_path))).load()
        assert output_segm.shape == output_image.shape
