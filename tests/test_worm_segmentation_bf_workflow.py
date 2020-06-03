import os
from glob import glob

import pytest
import luigi
import numpy as np

from faim_luigi.targets.image_target import TiffImageTarget
from ggjw.workflows.worm_segmentation import WormSegmentationFromBrightFieldWorkflow

# Test data for workflow
TEST_DATA = {
    key: os.path.join(
        os.path.dirname(__file__), 'data', 'worms_from_dic', key)
    for key in ['img', 'segm']
}


def binary_intersection_over_union(first, second):
    '''calculate the intersection over union for two images
    of identical shape.

    '''
    return np.logical_and(
        first, second).sum() / (np.logical_or(first, second).sum() + 1.)


def test_workflow(tmpdir):
    '''test the workflow for worm segmentation from BrightField image stacks
    on a few test images.

    '''
    input_folder = TEST_DATA['img']
    test_dir = tmpdir / 'worms_from_dic'
    test_dir.mkdir()

    result = luigi.build(
        [
            WormSegmentationFromBrightFieldWorkflow(
                output_folder=str(test_dir),
                input_folder=input_folder,
                file_pattern='*.stk')
        ],
        local_scheduler=True,
        detailed_summary=True)

    if result.status not in [
            luigi.execution_summary.LuigiStatusCode.SUCCESS,
            luigi.execution_summary.LuigiStatusCode.SUCCESS_WITH_RETRY
    ]:
        raise RuntimeError(
            'Luigi failed to run the workflow! Exit code: {}'.format(result))

    references = list(
        TiffImageTarget(path)
        for path in sorted(glob(os.path.join(TEST_DATA['segm'], '*tif'))))
    assert len(references) >= 5

    for reference in references:
        target = TiffImageTarget(
            os.path.join(str(test_dir), os.path.basename(reference.path)))
        assert target.exists()

        pred = target.load().squeeze()
        ref_segm = reference.load()

        assert np.all(pred.shape == ref_segm.shape)
        assert pred.min() >= 0
        assert pred.max() <= 255

        # test lower bound on iou.
        iou = binary_intersection_over_union(pred >= 127, ref_segm)
        assert iou >= 0.5


def test_workflow_error_on_no_input(tmpdir):
    '''test if the workflow raises an error if there are no images
    found that match the file pattern.
    '''
    input_folder = tmpdir / 'empty'
    input_folder.mkdir()

    result = luigi.build(
        [
            WormSegmentationFromBrightFieldWorkflow(
                output_folder=str(input_folder),
                input_folder=str(input_folder),
                file_pattern='stuff.stk')
        ],
        local_scheduler=True,
        detailed_summary=True)
    assert result.status == luigi.execution_summary.LuigiStatusCode.SCHEDULING_FAILED
