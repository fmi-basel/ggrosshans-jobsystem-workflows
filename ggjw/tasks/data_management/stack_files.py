#testp
import pandas as pd
import numpy as np
import glob
import re
from skimage import io
import os

#pip install tifffile
from tifffile import imwrite
from skimage import img_as_ubyte

import luigi
from ggjw.tasks.logging import LGRunnerLoggingMixin, add_progress



class StackFilesTask(luigi.Task, LGRunnerLoggingMixin):
    '''should execute stacking the files.'''

    
    #inputs from luigi
    image_folder = luigi.Parameter()  # where the images are
    image_file_pattern = luigi.Parameter()  #which channel is the channel for quantification?
    data_format = luigi.Parameter() #TODO Make dropdown menuw with "st" or "ts"
    output_folder = luigi.Parameter()  #where the results need to be saved


    def run(self):
        
        # info's
        self.log_info(
            "Images from channel {a} are taken from folder {b}".format(
                a=self.image_file_pattern, b=self.image_folder))


        #get imagelist for the folder
        img_list=glob.glob(self.image_folder+"/"+self.image_file_pattern)


        #extract position and frame, and saves it in the dataframe filelist
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

        #sorts the filelist by position
        Unique_positions=filelist["Position"].unique()
        #print(Unique_positions)
        ordered_filelist= filelist.groupby(['Position','Frame'])['File'].unique()


        #loops over the position to stack the images
        for position in Unique_positions: #loops over the position

            self.log_info("running image position {a} from total of {b} positions".format(a=str(position),b=len(Unique_positions)))

            add_progress(self,position / len(Unique_positions)*100)
            #print("pos "+ str(position))

            #make a list which images to stack
            images_to_stack=ordered_filelist[position]

            #creates empty stacked image
            image_to_save=[]

            #loops over the images in position
            for image_file in images_to_stack: 
                #print(image_file)
                img = io.imread(image_file[0])
                image_to_save.append(img)

            image_to_save=np.stack(image_to_save,axis=0)
            print("done")
            
            # write to tiff
            os.makedirs(self.output_folder+'/temp', exist_ok=True)
            img_name=self.output_folder+'/temp/s'+str(position)+".tiff"
            imwrite(img_name, image_to_save,imagej=True)

            # with self.output().open('w') as fout:
            #     df.to_csv(fout, index=False)

            self.log_info("stacking files is completed")


    def output(self):
        return luigi.LocalTarget(self.output_folder+'/temp')


## added to run the code from here
if __name__ == '__main__':
    luigi.build([
        StackFilesTask(
            image_folder = '/Users/Marit/Documents/Test_folder',  # where the images are
            image_file_pattern = '*w1Marit-488-BF-Cam0*.stk',  #which channel is the channel for quantification?
            data_format = 'st', 
            output_folder = '/Users/Marit/Documents/output')
    ],
                local_scheduler=True)
