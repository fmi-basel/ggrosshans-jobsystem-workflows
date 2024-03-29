import os
from glob import glob
import filecmp

import luigi
import numpy as np
import pytest
import tifffile

from faim_luigi.targets.image_target import TiffImageTarget
from ggjw.tasks.compression.compression_simple import StkToCompressedTifTask
from ggjw.workflows import StkToTifImageCompressionWorkflow

# Test data for workflow
TEST_DATA = {
    key: os.path.join(os.path.dirname(__file__), 'data', 'worms_from_dic', key)
    for key in [
        'img',
    ]
}


@pytest.mark.parametrize(
    'workflow', [StkToCompressedTifTask, StkToTifImageCompressionWorkflow])
def test_compression_task(tmpdir, workflow):
    '''
    '''
    input_folder = TEST_DATA['img']
    test_dir = tmpdir

    result = luigi.build([
        workflow(output_folder=str(test_dir),
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

    references = [
        TiffImageTarget(path)
        for path in sorted(glob(os.path.join(TEST_DATA['img'], '*stk')))
    ]
    compressed = [
        TiffImageTarget(path)
        for path in sorted(glob(os.path.join(str(test_dir), '*tif')))
    ]

    assert len(references) >= 1
    assert len(references) == len(compressed)

    for ref, compr in zip(references, compressed):
        ref = ref.load()
        assert ref.ndim == 3
        compr = compr.load()
        assert np.all(ref == compr)


@pytest.mark.parametrize(
    'workflow', [StkToCompressedTifTask, StkToTifImageCompressionWorkflow])
def test_compression_task_with_binning(tmpdir, workflow):
    '''
    '''
    input_folder = TEST_DATA['img']
    test_dir = tmpdir

    result = luigi.build([
        workflow(output_folder=str(test_dir),
                 input_folder=input_folder,
                 binning=2,
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

    references = [
        TiffImageTarget(path)
        for path in sorted(glob(os.path.join(TEST_DATA['img'], '*stk')))
    ]
    compressed = [
        TiffImageTarget(path)
        for path in sorted(glob(os.path.join(str(test_dir), '*tif')))
    ]

    assert len(references) >= 1
    assert len(references) == len(compressed)

    for ref, compr in zip(references, compressed):
        ref = ref.load()
        assert ref.ndim == 3
        compr = compr.load()
        # assert binned dimensions
        assert ref.shape == tuple(b * d
                                  for b, d in zip((1, 2, 2), compr.shape))


@pytest.mark.parametrize(
    'workflow', [StkToCompressedTifTask, StkToTifImageCompressionWorkflow])
def test_ndfile_backup(tmpdir, workflow):
    '''
    '''
    input_folder = TEST_DATA['img']
    # make target a subdir of tmpdir to check proper handling of
    # folder creation.
    test_dir = tmpdir / 'stuff'

    def _create_fake_file(folder, fname):
        path = os.path.join(folder, fname)
        with open(path, 'w') as fout:
            fout.write('something')
        return path

    original = _create_fake_file(input_folder, 'xy.nd')
    expected = os.path.join(str(test_dir), 'xy.nd.backup')

    result = luigi.build([
        workflow(output_folder=str(test_dir),
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

    assert os.path.exists(expected)
    assert filecmp.cmp(original, expected, shallow=False)


@pytest.mark.parametrize(
    'workflow', [StkToCompressedTifTask, StkToTifImageCompressionWorkflow])
@pytest.mark.parametrize('binning', [1, 2])
def test_pixel_spacing_meta(tmpdir, workflow, binning):
    '''check if spacing is correctly propagated.
    '''
    input_folder = TEST_DATA['img']
    test_dir = tmpdir

    reference = [
        TiffImageTarget(path)
        for path in sorted(glob(os.path.join(TEST_DATA['img'], '*stk')))
    ][0]
    file_pattern = os.path.basename(reference.path)

    result = luigi.build(
        [
            workflow(output_folder=str(test_dir),
                     input_folder=input_folder,
                     binning=binning,
                     file_pattern=file_pattern)  # only one file is enough.
        ],
        local_scheduler=True,
        detailed_summary=True)

    if result.status not in [
            luigi.execution_summary.LuigiStatusCode.SUCCESS,
            luigi.execution_summary.LuigiStatusCode.SUCCESS_WITH_RETRY
    ]:
        raise RuntimeError(
            'Luigi failed to run the workflow! Exit code: {}'.format(result))

    compressed = [
        TiffImageTarget(path)
        for path in sorted(glob(os.path.join(str(test_dir), '*tif')))
    ]

    assert len(compressed) == 1

    def get_resolution(path):
        with tifffile.TiffFile(path) as fin:
            if fin.is_stk:
                return tuple(fin.stk_metadata[ax + 'Calibration']
                             for ax in 'XY')
            if fin.is_imagej:
                return tuple(
                    denom / num  # inverted to get spacing.
                    for num, denom in (fin.pages[0].tags[ax +
                                                         'Resolution'].value
                                       for ax in 'XY'))

    # compare spacing
    comp_res = get_resolution(compressed[0].path)
    ref_res = get_resolution(reference.path)

    assert all(lhs == pytest.approx(binning * rhs, abs=1e-4)
               for lhs, rhs in zip(comp_res, ref_res))
