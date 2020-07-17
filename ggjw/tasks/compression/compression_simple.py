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
        '''creates tif target in output_folder.
        '''
        fname = os.path.splitext(os.path.basename(
            input_handle.path))[0] + '.tif'
        return TiffImageTarget(os.path.join(self.output_folder, fname))

    def convert(self, input_target, output_target):
        '''writes tif
        '''
        img = input_target.load()
        output_target.save(img, compress=self.compression)
