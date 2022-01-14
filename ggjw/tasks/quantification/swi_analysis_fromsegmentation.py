from numpy.core.fromnumeric import ndim
from functions_fromsegmentation import get_image_path_pairs,read_image_and_metadata,select_zslides, mask_postprocessing
from functions_fromsegmentation import calculate_worm_properties, make_mask_background


import numpy as np
import pandas as pd
import multiprocessing
from os import path
from os import mkdir
import re


# parameters
image_folder = '/tungstenfs/nobackup/ggrossha/grafmaik/eor1xnhr25_rep_compressed'
segmentation_folder= '/tungstenfs/scratch/ggrossha/svcgrossha/grafmaik/eor1xnhr25_rep_Segmentation'
channel_GFP = '*488-BF*.tif'
data_format = 'st'

outputpath ='/tungstenfs/scratch/ggrossha/grafmaik/eor1xnhr25_python_quantification'
output_filename= 'results_all.csv'


#save retults
results = []
current_res_df = []
counter = 0

#list for all channels the stk files in folder
list_GFP,list_seg=get_image_path_pairs(image_folder,channel_GFP,segmentation_folder)
print("size files" + str(np.size(list_GFP)))

#checks
if (path.exists(image_folder)==False):
    raise FileNotFoundError("Image path doesnt exist")
if (path.exists(segmentation_folder)==False):
    raise FileNotFoundError("Segmentation path doesnt exist")
if (path.exists(outputpath)==False):
    mkdir(outputpath)
    print("new output directory is made")
if (path.exists(path.join(outputpath,list_seg[0]))==False):
    raise FileNotFoundError("check name mCherry channel")
if (path.exists(path.join(outputpath,list_seg[0]))==False):
    raise FileNotFoundError("check name GFP channel")    
if (path.exists(path.join(outputpath,output_filename))):
    raise FileExistsError("Watch out, output file already exist. Rename outputfile")


# define the main function
def main_function(imglist_seg,imglist_gfp):
    
    # Reading the image and gets out the metadat
    files = (imglist_seg,imglist_gfp)
    (img_binary, img_gfp), current_res = read_image_and_metadata(files,data_format=data_format)
    
    #get maximum intensity projection GFP
    img_gfp = select_zslides(img_gfp)
    img_GFP_max = np.max(img_gfp, axis=0)

    #get background intensity
    background_binary= make_mask_background(img_binary)
    (background,n)= calculate_worm_properties(background_binary, img_GFP_max) 

    #postprocessing of mask: fill holes 
    img_binary=img_binary>127
    img_binary=mask_postprocessing(img_binary, krn_size = 10)

    #calculate properties of segmented worm with head and tail
    (intensity,area)=calculate_worm_properties(img_binary, img_GFP_max) 
    
    # Storing the properties in current results for curved worm
    current_res['intensity'] = intensity  #calculate volume
    current_res['area'] = area
    current_res['mean_background']=background
    
    current_res_df.append(current_res)

    #update how far the workflow is in the system
    global counter
    counter = counter + 1
    print(str(counter) + "out of "+ str(len(list_seg)/12) + "is done")

    return current_res


if __name__=='__main__':
    p = multiprocessing.Pool(12)
    results = p.starmap(main_function,zip(list_seg,list_GFP))
    p.close()


# Saving results
df = pd.DataFrame(results)
df.to_csv(path.join(outputpath,output_filename), index=False)
