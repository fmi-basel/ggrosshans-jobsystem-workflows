# %%
import pandas as pd
import numpy as np
import glob
import os
import re

import luigi
from ggjw.tasks.logging import LGRunnerLoggingMixin
from ggjw.tasks.lgrunner.stop import StoppableTaskMixin


class DeleteFilesTask(luigi.Task, LGRunnerLoggingMixin, StoppableTaskMixin):

    # inputs from luigi
    image_folder = luigi.Parameter()
    data_format = luigi.ChoiceParameter(choices=["st", "ts"], default="st")
    csv_document = luigi.Parameter()
    image_file_pattern = luigi.Parameter()

    def run(self):

        self.log_info(
            "Images will be deleted from folder {}".format(
                self.image_folder))

        # get the image list
        img_list = glob.glob(os.path.join(
            self.image_folder, self.image_file_pattern))

        # get annotation file
        if os.path.exists(self.csv_document):
            CSV_annotation = pd.read_csv(self.csv_document)

        else:
            raise FileNotFoundError(
                'CSV file in path {} is not there'
                .format(self.csv_document))

        if "Position" not in CSV_annotation:
            raise FileNotFoundError(
                'CSV document doesnt contain header Position')

        if "Quality" not in CSV_annotation:
            raise FileNotFoundError(
                'CSV document doesnt contain header Quality')

        if "Hatch" not in CSV_annotation:
            raise FileNotFoundError('CSV document doesnt contain header Hatch')
        if "Escape" not in CSV_annotation:
            raise FileNotFoundError(
                'CSV document doesnt contain header Escape')

        # determine position and frame of each image
        AllData = []
        for image in img_list:
            if self.data_format == 'st':
                pictureinfo = re.split('_s(\\d+)_t(\\d+)\\..+', image)
                s_info = 1
                t_info = 2
            if self.data_format == 'ts':
                pictureinfo = re.split('t(\\d+)_s(\\d+)_', image)
                s_info = 2
                t_info = 1

            Data = {"File": image, 'Position': int(
                pictureinfo[s_info]), 'Frame': int(pictureinfo[t_info])}
            AllData.append(Data)

        filelist = pd.DataFrame(data=AllData)

        # merges annotation file and image folder list
        filelist = filelist.merge(
            CSV_annotation, on=['Position'], how='inner')  # merges two datasets

        filelist.to_csv(os.path.join(self.image_folder, "original_files.csv"))

        # reduces the filelist to only the files that need to be deleted
        # selects only worms where quality is 0
        files_to_delete1 = filelist[filelist['Quality'] == 0]

        # select quality is 1, but where timepoints are out of Hatch/Escape
        files_to_delete2 = filelist[filelist['Quality'] == 1]
        files_to_delete2 = files_to_delete2[(files_to_delete2["Frame"] < files_to_delete2["Hatch"]) | (
            files_to_delete2["Frame"] > files_to_delete2["Escape"])]  # selects only worms where frame is smaller then hatch

        # merges the files to delete
        files_to_delete = pd.concat([files_to_delete1, files_to_delete2])

        files_to_delete.to_csv(os.path.join(
            self.image_folder, "deleted_files.csv"))

        # info's
        self.log_info(
            "{} Nr of images are going to be delted".format(
                np.size(files_to_delete, 0)))

        # delete files in folder
        for i in range(np.size(files_to_delete, 0)):
            # print(files_to_delete["File"].iloc[i])
            os.remove(files_to_delete["File"].iloc[i])

        self.log_info("Files are deleted")

    def output(self):
        return luigi.LocalTarget(
            os.path.join(self.image_folder, 'deleted_files.csv'))
