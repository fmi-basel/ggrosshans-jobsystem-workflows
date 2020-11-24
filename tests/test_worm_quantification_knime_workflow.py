import os

import luigi
import pytest
import pandas

from ggjw.workflows.worm_quantification import WormQuantificationWorkflow

# Test data for workflow
TEST_DATA = {
    key:
    os.path.join(os.path.join(os.path.dirname(__file__), 'data', 'quant', key))
    for key in ['img', 'segm']
}

TEST_DATA_EXISTS = all(os.path.exists(path) for path in TEST_DATA.values())


def assert_quant_output_exists(output_folder):
    assert (output_folder / 'kymograph_stats.csv').exists()
    assert (output_folder / 'kymograph_stats.table').exists()
    assert (output_folder / 'knime-workflow.log').exists()
    assert (output_folder / 'kymographs').exists()


# TODO consider adding check for knime availability
@pytest.mark.skipif(not TEST_DATA_EXISTS,
                    reason='Test data for quantification not available.')
@pytest.mark.parametrize('image_file_pattern', ['*w1*', '*w1*.stk'])
def test_quant_workflow(tmpdir, image_file_pattern):
    '''test the workflow for worm segmentation from BrightField image stacks
    on a few test images.

    '''
    test_dir = tmpdir / 'quantification-knime'
    test_dir.mkdir()

    result = luigi.build([
        WormQuantificationWorkflow(output_folder=str(test_dir),
                                   image_folder=TEST_DATA['img'],
                                   segm_folder=TEST_DATA['segm'],
                                   image_file_pattern=image_file_pattern,
                                   threshold=127.0)
    ],
                         local_scheduler=True,
                         detailed_summary=True)

    if result.status not in [
            luigi.execution_summary.LuigiStatusCode.SUCCESS,
            luigi.execution_summary.LuigiStatusCode.SUCCESS_WITH_RETRY
    ]:
        raise RuntimeError(
            'Luigi failed to run the workflow! Exit code: {}'.format(result))

    # check if outputs are created.
    assert_quant_output_exists(test_dir)

    # check the content of the table.
    result_data = pandas.read_csv(test_dir / 'kymograph_stats.csv')
    assert len(result_data) == 3  # expected number of timepoints
    assert result_data.Intensity_BGsub.between(2, 5).all()
    assert result_data.Frame.between(17, 19).all()


@pytest.mark.skipif(not TEST_DATA_EXISTS,
                    reason='Test data for quantification not available.')
@pytest.mark.parametrize('image_file_pattern', [
    '*t17*.stk',
])
def test_quant_workflow_filtered(tmpdir, image_file_pattern):
    '''test the workflow for worm segmentation from BrightField image stacks
    on where the given pattern matches only one file.

    '''
    test_dir = tmpdir / 'quantification-knime'
    test_dir.mkdir()

    result = luigi.build([
        WormQuantificationWorkflow(output_folder=str(test_dir),
                                   image_folder=TEST_DATA['img'],
                                   segm_folder=TEST_DATA['segm'],
                                   image_file_pattern=image_file_pattern,
                                   threshold=127.0)
    ],
                         local_scheduler=True,
                         detailed_summary=True)

    if result.status not in [
            luigi.execution_summary.LuigiStatusCode.SUCCESS,
            luigi.execution_summary.LuigiStatusCode.SUCCESS_WITH_RETRY
    ]:
        raise RuntimeError(
            'Luigi failed to run the workflow! Exit code: {}'.format(result))

    # check if outputs are created.
    assert_quant_output_exists(test_dir)

    # check the content of the table.
    result_data = pandas.read_csv(test_dir / 'kymograph_stats.csv')
    assert len(result_data) == 1  # expected number of timepoints
