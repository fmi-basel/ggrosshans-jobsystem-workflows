#%%
import pandas as pd
import numpy as np
import glob
import os
import re

import luigi
from ggjw.tasks.logging import LGRunnerLoggingMixin



class DeleteFilesTask(luigi.Task, LGRunnerLoggingMixin):

    #inputs from luigi
    image_folder = luigi.Parameter()  # where the images are
    csv_document = luigi.Parameter() 
    image_file_pattern = luigi.Parameter()  #which channel is the channel for quantification?
    data_format = luigi.Parameter() #TODO Make dropdown menuw with "st" or "ts"

    def run(self):

        self.log_info(
            "Images will be deleted from folder {}".format(
                self.image_folder))
   
        #get the annotation and the image list
        CSV_annotation= pd.read_csv(self.csv_document)
        img_list=glob.glob(self.image_folder +"/"+self.image_file_pattern)

        #gets list of images in folder
        AllData=[]
        for image in img_list:
            if self.data_format == 'st':
                pictureinfo = re.split('_s(\d+)_t(\d+)\..+', image)
                s_info = 1
                t_info = 2
            if self.data_format == 'ts':
                pictureinfo = re.split('t(\d+)_s(\d+)_', image)
                s_info = 2
                t_info = 1

            Data= {"File":image, 'Position': int(pictureinfo[s_info]), 'Frame': int(pictureinfo[t_info])}
            AllData.append(Data)
            
        filelist=pd.DataFrame(data=AllData)

        #merges annotation file and image folder list
        filelist=filelist.merge(CSV_annotation,on=['Position'],how='inner') #merges two datasets
        filelist.to_csv(csv_dir+"/detected_files.csv")

        #reduces the filelist to only the files that need to be deleted
        files_to_delete1=filelist[filelist['Quality']==0] #selects only worms where quality is 0

        files_to_delete2=filelist[filelist['Quality']==1] #select quality is 1, but where timepoints are out of Hatch/Escape
        files_to_delete2=files_to_delete2[(files_to_delete2["Frame"]<files_to_delete2["Hatch"]) | (files_to_delete2["Frame"]>files_to_delete2["Escape"])] #selects only worms where frame is smaller then hatch

        #merges the files to delete
        files_to_delete=pd.concat([files_to_delete1,files_to_delete2])
        files_to_delete.to_csv(csv_dir+"/deleted_files.csv")

        # info's
        self.log_info(
            "{} Nr of images are going to be delted".format(
                np.size(files_to_delete,0)))
        #print("nr of files are going to be deleted = " + str(np.size(files_to_delete,0)))

        #delete files in folder
        for i in range(np.size(files_to_delete,0)):
            #print(files_to_delete["File"].iloc[i])
            os.remove(files_to_delete["File"].iloc[i])

        self.log_info("done")

    #   No def output(self):??





