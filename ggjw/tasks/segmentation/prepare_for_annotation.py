'''Sampler tasks for preparing images to annotate.
'''
import abc
import os
import glob

import luigi
import pandas
import numpy as np

from skimage.external.tifffile import imread

from faim_luigi.targets.image_target import TiffImageTarget

from ggjw.tasks.logging import LGRunnerLoggingMixin, add_progress
from ggjw.tasks.lgrunner.stop import StoppableTaskMixin


class PrepareImagesForAnnotationBase(luigi.Task, LGRunnerLoggingMixin,
                                     StoppableTaskMixin):
    '''base class for data preparation for training data.
    '''

    accepts_messages = True

    input_folder = luigi.Parameter()
    '''input folder to collect from.
    '''

    file_pattern = luigi.Parameter()
    '''filename pattern matching files to be included.
    Allowed wildcards are *, ?, [seq] and [!seq] (see fnmatch)
    '''

    output_folder = luigi.Parameter()
    '''output folder to write projections into.
    '''

    @property
    def img_outdir(self):
        return os.path.join(self.output_folder, 'img')

    @property
    def segm_outdir(self):
        return os.path.join(self.output_folder, 'segm')

    def _prepare_sample(self, path, img_target, segm_target):
        '''this implements the preparation logic. At the moment,
        that means min projection along Z and creating an empty
        segmentation.
        '''
        img = np.expand_dims(imread(path).min(axis=0), 0)
        img_target.save(img)
        segm_target.save(np.zeros_like(img).astype(np.uint8), compress=9)

    @abc.abstractmethod
    def _get_candidates(self):
        '''yields pairs of image and segmentation targets.
        '''

    def run(self):
        '''generates pairs of prepared image and segmentation stacks
        for annotation.
        '''
        os.makedirs(self.img_outdir, exist_ok=True)
        os.makedirs(self.segm_outdir, exist_ok=True)

        candidates = list(self._get_candidates())
        self.log_info('Preparing {} images for annotation.'.format(len(candidates)))

        for path, (img_target, segm_target) in zip(candidates, self.output()):

            self.raise_if_interrupt_signal()

            if not (img_target.exists() and segm_target.exists()):
                try:
                    self._prepare_sample(path, img_target, segm_target)
                except Exception as err:
                    self.log_error(
                        'Could not prepare stacks for {}: {}'.format(
                            path, err))

            add_progress(self, 1. / len(candidates))
        self.log_info('Done.')

    def output(self):
        '''returns a list of image-segmentation target pairs.
        '''
        candidates = self._get_candidates()

        if not candidates:
            self.log_error('No input images found!')

        def _get_fname(img_path):
            return os.path.splitext(os.path.basename(img_path))[0] + '.tif'

        return [(TiffImageTarget(os.path.join(self.img_outdir, fname)),
                 TiffImageTarget(os.path.join(self.segm_outdir, fname)))
                for fname in (_get_fname(path) for path in candidates)]


class PrepareRandomlySelectedImagesForAnnotation(PrepareImagesForAnnotationBase
                                                 ):
    '''prepare randomly sampled images for annotation.
    '''

    num_samples = luigi.IntParameter()
    '''number of samples to draw.
    '''

    seed = luigi.IntParameter(default=13)
    '''seed for random sampling.
    '''

    def _get_candidates(self):
        '''
        '''
        candidates = pandas.DataFrame({
            "path":
            sorted(
                glob.glob(os.path.join(self.input_folder, self.file_pattern)))
        })
        if self.num_samples > len(candidates):
            raise RuntimeError(
                'Not enough images found to draw {}! Found: {}'.format(
                    self.num_samples, len(candidates)))
        for path in candidates.path.sample(n=self.num_samples,
                                           replace=False,
                                           random_state=self.seed):
            yield path


class PrepareManuallySelectedImagesForAnnotation(PrepareImagesForAnnotationBase
                                                 ):
    '''prepare images for annotation based on a manually generated
    list of positions and timepoints.
    '''

    input_file = luigi.Parameter()
    '''manually generated list of images to prepare.

    Must contain the following columns:

    folder, position, timepoint

    the files are expected at the following location:

    <input_folder>/<folder>/<file_pattern><delimiter><position><delimiter><timepoint><file_extension>
    '''

    file_extension = luigi.Parameter(default='.stk')
    '''file extension.
    '''
    delimiter = luigi.Parameter(default='_')
    '''delimiter between position and timepoint in filename.
    '''

    def _get_candidates(self):
        '''
        '''
        try:
            candidates = pandas.read_csv(self.input_file, dtype=str)
        except Exception as err:
            self.log_error('Could not parse {}: {}'.format(
                self.input_file, err))
            raise

        for _, row in candidates.iterrows():
            fname = os.path.join(
                self.input_folder, row['folder'],
                self.file_pattern + self.delimiter + row['position'] +
                self.delimiter + row['timepoint'] + self.file_extension)
            paths = sorted(glob.glob(fname))

            if not paths:
                err_msg = 'Did not find any match for: {}'.format(fname)
                self.log_error(err_msg)
                raise RuntimeError(err_msg)
                continue

            for path in paths:
                yield path
