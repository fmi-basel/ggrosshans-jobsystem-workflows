import os
from glob import glob
import shutil

import luigi
import tifffile

from skimage.transform import downscale_local_mean

from faim_luigi.targets.image_target import TiffImageTarget

from .compression_base import BaseCompressionTask, ConversionException


def load_stk_with_basic_meta(path):
    '''
    '''
    with tifffile.TiffFile(path) as fin:
        meta = stk_meta_to_ijtiff_meta(fin.stk_metadata) if fin.is_stk else {}
        data = fin.asarray()
        return data, meta


def stk_meta_to_ijtiff_meta(stk_metadata):
    '''extracts pixel spacing and creates the necessary fields
    to write it to an imagej compatible tiff.
    '''
    meta = {
        'imagej': True,
    }

    try:
        calibration_meta = tuple(
            stk_metadata.get(ax + 'Calibration') for ax in 'XY')
        meta['resolution'] = tuple(1. / cal for cal in calibration_meta)
        meta['metadata'] = {'unit': 'um'}
    except Exception:
        pass

    try:
        zdist = stk_metadata['ZDistance'][0]
        if all(zdist == val for val in stk_metadata['ZDistance']):
            meta['metadata']['spacing'] = zdist
    except Exception:
        pass
    return meta


class StkToCompressedTifTask(BaseCompressionTask):
    '''implements the compression task with tifffile using
    the deflate compression.

    '''

    output_folder = luigi.Parameter()
    '''output folder to write compressed stacks.

    '''

    binning = luigi.IntParameter(default=1)
    '''downsampling factor.
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
        try:
            img, meta = load_stk_with_basic_meta(input_target.path)
            #self.log_info('Metadata: ' + str(meta))
            #self.log_info(str(img.shape))
            if self.binning > 1:
                img = downscale_local_mean(
                    img, (1, self.binning, self.binning)).astype(img.dtype)
            if not meta or meta.get('resolution', None) is None:
                #self.log_warning('Could not read pixel spacing for {}'.format(
                #    input_target.path))
                pass
            else:
                if self.binning > 1:
                    meta['resolution'] = tuple(res / self.binning
                                               for res in meta['resolution'])

            output_target.save(img, compress=self.compression, **meta)
        except Exception as err:
            # Re-raise an error here to provide more information about
            # which file caused the error.
            raise ConversionException(
                'Compression of {} failed. Error: {}'.format(
                    input_target.path, err))

    def preconvert(self):
        '''
        '''
        def _get_dest(path):
            return os.path.join(self.output_folder,
                                os.path.basename(path) + '.backup')

        ndfiles = glob(os.path.join(self.input_folder, '*nd'))

        self.log_info('Found {} .nd file{} to copy.'.format(
            len(ndfiles), '' if len(ndfiles) == 1 else 's'))

        for source, dest in ((source, _get_dest(source))
                             for source in ndfiles):
            try:
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                shutil.copy(source, dest)
            except Exception as err:
                self.log_error('Could not copy {} to {}. Error: {}'.format(
                    source, dest, err))
