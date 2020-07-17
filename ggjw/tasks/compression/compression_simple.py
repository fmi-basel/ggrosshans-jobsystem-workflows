import os
import luigi

from faim_luigi.targets.image_target import TiffImageTarget

from .compression_base import BaseCompressionTask


class StkToCompressedTifTask(BaseCompressionTask):
    '''implements the compression task with tifffile using
    the deflate compression.

    '''

    output_folder = luigi.Parameter()
    '''output folder to write compressed stacks.

    '''

    compression = ('deflate', 9)  # tifffile specific.

    def get_target(self, input_handle):
        '''
        '''
        fname = os.path.splitext(os.path.basename(
            input_handle.path))[0] + '.tif'
        return TiffImageTarget(os.path.join(self.output_folder, fname))

    def convert(self, input_handle, output_handle):
        '''
        '''
        img = input_handle.load()
        output_handle.save(img, compress=self.compression)
