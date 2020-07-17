import os
from glob import glob
import luigi
import numpy as np

from faim_luigi.targets.image_target import TiffImageTarget
from ggjw.tasks.compression.compression_simple import StkToCompressedTifTask

# Test data for workflow
TEST_DATA = {
    key: os.path.join(os.path.dirname(__file__), 'data', 'worms_from_dic', key)
    for key in [
        'img',
    ]
}


def test_compression_task(tmpdir):
    '''
    '''
    input_folder = TEST_DATA['img']
    test_dir = tmpdir

    result = luigi.build([
        StkToCompressedTifTask(output_folder=str(test_dir),
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
        for path in sorted(glob(os.path.join(TEST_DATA['img'], '*stk')))]
    compressed = [
        TiffImageTarget(path)
        for path in sorted(glob(os.path.join(str(test_dir), '*tif')))]

    assert len(references) >= 1
    assert len(references) == len(compressed)

    for ref, compr in zip(references, compressed):
        ref = ref.load()
        assert ref.ndim == 3
        compr = compr.load()
        assert np.all(ref == compr)