import os
import sys
import luigi
import numpy as np
import pandas as pd
import glob

from ggjw.tasks.quantification.quant_utils import read_image
from ggjw.tasks.quantification.quant_utils import get_meta_info
from ggjw.tasks.quantification.quant_utils import get_image_path_pairs
#from ggjw.tasks.quantification.quant_utils import find_largest_area
from ggjw.tasks.quantification.quant_utils import select_zslides
from ggjw.tasks.quantification.quant_utils import calculate_worm_properties

from ggjw.tasks.logging import LGRunnerLoggingMixin, add_progress


class WormQuantificationTask(luigi.Task, LGRunnerLoggingMixin):
    '''should execute worm quantifications.'''

    #inputs from luigi
    image_folder = luigi.Parameter()  # where the images are
    image_file_pattern = luigi.Parameter(
    )  #which channel is the channel for quantification?
    segm_folder = luigi.Parameter()
    output_folder = luigi.Parameter()  #where the results need to be saved
    threshold = luigi.FloatParameter(default=127.0)

    def run(self):
        '''runs the code'''

        #dirpath_Segm= self.segm_folder
        self.log_info("Segmented images are taken from directory {}".format(
            self.segm_folder))

        #dirpath_GFP= self.image_folder
        #channel_GFP=self.image_file_pattern #GFP
        self.log_info(
            "took GFP images from directory {a} with channel {b}".format(
                a=self.image_folder, b=self.image_file_pattern))

        # TODO fill holes, skeletonize
        # TODO tests
        # TODO (Future) run in parallel

        results = []

        num_files_normalized = len(
            glob.glob(os.path.join(self.image_folder,
                                   self.image_file_pattern))) * 100.0

        for count, (bf_img_path, gfp_img_path) in enumerate(
                get_image_path_pairs(self.image_folder,
                                     self.image_file_pattern,
                                     self.segm_folder)):

            self.log_info("running image {}".format(bf_img_path))
            add_progress(self, count / num_files_normalized)

            segm = read_image(bf_img_path)
            img_GFP = read_image(gfp_img_path)

            #select good z-slides
            img_GFP = select_zslides(img_GFP)

            #making max int projection
            img_GFP_max = np.max(img_GFP, axis=0)

            #make segmented worm image binary
            img_bin = (segm > self.threshold)

            #regionprops of binary image
            area, mean_intensity, min_intensity = calculate_worm_properties(
                img_bin, img_GFP_max)

            #put results in results array
            current_res = get_meta_info(bf_img_path)
            current_res['intensity'] = mean_intensity - min_intensity
            current_res['size'] = area
            results.append(current_res)

        #saves the completed results array in a csv file
        df = pd.DataFrame(
            results)  #columns=["Position", "Frame", "Intensity"])
        with self.output().open('w') as fout:
            df.to_csv(fout, index=False)

        self.log_info("Quantification is completed")

    def output(self):
        return luigi.LocalTarget(
            os.path.join(self.output_folder, 'kymographs_stats.csv'))


## added to run the code from here
if __name__ == '__main__':
    luigi.build([
        WormQuantificationTask(
            image_folder="/Users/Marit/Documents/sample_exp/testimg",
            image_file_pattern="*w1Conf488Brightfield*",
            segm_folder="/Users/Marit/Documents/sample_exp/segmentations",
            output_folder="/Users/Marit/Documents/sample_exp/results2")
    ],
                local_scheduler=True)
