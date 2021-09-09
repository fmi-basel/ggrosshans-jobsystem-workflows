# testp
from os.path import join

import pandas as pd
import numpy as np
import glob
import re
from skimage import io
import os

# pip install tifffile
from tifffile import imwrite

import luigi
from ggjw.tasks.logging import LGRunnerLoggingMixin, add_progress
from ggjw.tasks.lgrunner.stop import StoppableTaskMixin


class StackFilesTask(luigi.Task, LGRunnerLoggingMixin, StoppableTaskMixin):
    '''should execute stacking the files.'''

    # inputs from luigi
    image_folder = luigi.Parameter()  
    image_file_pattern = luigi.Parameter()
    data_format = luigi.ChoiceParameter(choices=["st", "ts"], default="st")
    output_folder = luigi.Parameter()  

    def run(self):

        # info's
        self.log_info(
            "Images from channel {a} are taken from folder {b}".format(
                a=self.image_file_pattern, b=self.image_folder))

        # get imagelist for the folder
        img_list = glob.glob(os.path.join(
            self.image_folder, self.image_file_pattern))
        if np.size(img_list) == 0:
            raise FileNotFoundError(
                "No input images provided! Check input directory ")

        # extract position and frame, and saves it in the dataframe filelist
        AllData = []
        for image in img_list:
            if self.data_format == 'st':
                pictureinfo = re.split('_s(\d+)_t(\d+)\..+', image)
                s_info = 1
                t_info = 2
            if self.data_format == 'ts':
                pictureinfo = re.split('t(\d+)_s(\d+)_', image)
                s_info = 2
                t_info = 1

            Data = {"File": image, 'Position': int(
                pictureinfo[s_info]), 'Frame': int(pictureinfo[t_info])}
            AllData.append(Data)

        filelist = pd.DataFrame(data=AllData)

        # sorts the filelist by position
        Unique_positions = filelist["Position"].unique()

        # print(Unique_positions)
        ordered_filelist = filelist.groupby(
            ['Position', 'Frame'])['File'].unique()

        # loops over the position to stack the images
        for position in Unique_positions:  # loops over the position

            self.log_info("running image position {a} from total of {b} positions".format(
                a=str(position), b=len(Unique_positions)))

            add_progress(self, position / len(Unique_positions)*100)
            #print("pos "+ str(position))

            # make a list which images to stack
            images_to_stack = ordered_filelist[position]

            # creates empty stacked image
            image_to_save = []

            # loops over the images in position
            for image_file in images_to_stack:
                # print(image_file)
                img = io.imread(image_file[0])
                image_to_save.append(img)

            # add extra dimention for C-channel

            image_to_save = np.stack(image_to_save, axis=0)
            image_to_save = image_to_save[:, :, np.newaxis]
            print(np.shape(image_to_save))

            # write to tiff
            os.makedirs(join(self.output_folder, "temp"), exist_ok=True)
            img_name = join(self.output_folder, "temp", "s"+str(position)+".tiff")
            imwrite(img_name, image_to_save, imagej=True)

        self.log_info("stacking files is completed")

    def output(self):
        return luigi.LocalTarget(join(self.output_folder, "temp"))
