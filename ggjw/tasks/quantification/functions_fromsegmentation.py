import numpy as np
import matplotlib.pyplot as plt
import glob
import os
import re
from fil_finder import FilFinder2D
import astropy.units as u
from skimage import io
import skimage.morphology as skimorph
from skimage.measure import regionprops
from scipy.ndimage.measurements import label 
import scipy.ndimage.morphology as scimorph


# read files from folder
def get_image_path_pairs(dir1, fname_pattern1, dir2, fname_pattern2='*'):
    """
    lists GFP images in dir1/channel1 and find corresponding GFP images in dir2
    Parameters
    ----------
    dir1 : directory where the GFP images are located 
    channel1 : name of channel1 images(GFP) 
    dir2 : directory where segmented images are located
    Returns
    -------
    list_1 : list of files in GFP 
    list_2 : list of files in BF
    """
    #gets all images in directory2 
    lhs_paths = sorted(glob.glob(os.path.join(dir2, fname_pattern2)))

    #checks if the channel pattern is present in the directory
    if not lhs_paths:
        raise FileNotFoundError(
            'No files in image_folder match the given image_file_pattern={}'.
            format(fname_pattern1))

    #get metadata for each file in the path
    metadata = {_get_meta_tuple(path): path for path in lhs_paths}

    #create emty list
    list1=[]
    list2=[]

    #goes over 
    for rhs_path in sorted(glob.glob(os.path.join(dir1, fname_pattern1))):
        lhs_match = metadata.get(_get_meta_tuple(rhs_path), None)
        list1.append(rhs_path)
        list2.append(lhs_match)
        if lhs_match is None:
            raise FileNotFoundError(
                'the segmented pair was not found for ={}'.format(rhs_path))
            
    
    return list1,list2

def _get_meta_tuple(path):
    return tuple(re.split('_s(\d+)_t(\d+)\..+', path)[1:])

def read_image_and_metadata(path, data_format = 'ts'):
    '''
    Reads a STK or TIFF file for a path (include filename) and return
    the (3D) image and the values for position and frame.
    
    This function allows for more than on path name to be read. This 
    allows for all channels to be read simultaneously. Following Marit's 
    convention for ordering. For instance:
    (mCherry_img, GFP_img, BF_imag), metadata_out = 
        read_image_and_metadata(mCherry_path, GFP_path, BF_path)

    Filenames are assume to be of the form:
        name_sx_tx_otherinfo.extension
    Other filenames would not yield correct results.

    Version note: This function replaces read_image, get_meta_info 
    and get_meta_info_temp from the Marit module.

    Parameters
    ----------
    path : Path of the file(s) to read.
    data_format: Declaring the filename structure. By default it is 
        assumed for time to preceed position ('st' format). 
        If position preceeds time, then define data_format = 'ts'.

    Returns
    -------
    img_output: (3D) array(s) of the selected image(s). For more than
        one image, each array is contained in a tuple.
    metadata_out: Dictionary containing the position (s) and the 
        frame (t).

    '''
    # Image
    if np.ndim(path) == 0:
        print("path==0")
        img = io.imread(path)
        img_output = img.squeeze()
        
        # Metadata
        if data_format == 'st':
            pictureinfo = re.split('_s(\d+)_t(\d+)\..+', path)
            s_info = 0
            t_info = 1
        if data_format == 'ts':
            pictureinfo = re.split('t(\d+)_s(\d+)_', path)
            s_info = 1
            t_info = 0

    else:
        img_output = []
        for k in enumerate(path):
            img = io.imread(path[k[0]])
            img_output.append(img.squeeze())
        
        # Metadata
        if data_format == 'st':
            pictureinfo = re.split('_s(\d+)_t(\d+)\..+', path[1])
            s_info = 1
            t_info = 2

        if data_format == 'ts':
            pictureinfo = re.split('t(\d+)_s(\d+)_', path[1])
            s_info = 2
            t_info = 1


    
    metadata_out = {'Position': pictureinfo[s_info], 'Frame': pictureinfo[t_info]}

    return img_output, metadata_out

#select z slides
def select_zslides(signal_image):
    """
    select z slides that are in focus, where the mean intensity of every plane is above the avarage
    """
    assert signal_image.ndim == 3
    mean_img = np.mean(
        signal_image, axis=tuple(ax for ax in range(
            1, signal_image.ndim)))  #calculates mean of every zslide
    threshold = (
        np.max(mean_img) + np.min(mean_img)
    ) / 2  #calulates the mean of mean, and determines the treshold

    start, stop = _find_best_interval(
        mean_img, threshold
    )  #finds the best interval where intensity is above avarage strashold

    selected_slides_signal = signal_image[
        start:stop]  # removes the slides that are out of focus

    return selected_slides_signal

def _find_best_interval(values, threshold):
    '''returns the indices of the interval over the given values that maximizes
        \sum_{i \in [start:stop)} sign(values[i] - threshold)
    Please note that the intervals stop is always exclusive. You can
    therefore use values[start:stop] to access all values of the
    interval.
    '''
    class Interval:
        def __init__(self, start, stop, score=0.0):
            self.start = start
            self.stop = stop
            self.score = score

        def reset(self, idx):
            self.start = idx
            self.stop = idx
            self.score = 0.0

    assert values.ndim == 1

    current = Interval(0, 0)
    best = Interval(0, 0)

    for idx, val in enumerate(np.sign(values - threshold)):
        current.score += val
        if current.score < 0.0:
            current.reset(idx + 1)

        elif current.score > best.score:
            best = Interval(current.start, idx + 1, current.score)

    return best.start, best.stop

# calculate background
def make_mask_background(binary_image):
    krn = skimorph.disk(3)
    surrounding_mask = np.zeros(binary_image.shape)
    
    if np.size(np.shape(binary_image))>2:
        for zslide in np.arange(0,binary_image.shape[0]):
            surrounding_mask[zslide,:,:] = skimorph.binary_dilation(binary_image[zslide,:,:], krn)
    else:
        surrounding_mask = skimorph.binary_dilation(binary_image, krn)

    surrounding_mask = (surrounding_mask*(np.invert(binary_image)))==1
    #plt.imshow(surroundings)
 
    return surrounding_mask

# calculate properties from image
def calculate_worm_properties(img_binary, img_signal):
    """calculate worm properties is a function that calculate the area of the segmented area
    in the binary image, and calculate the mean intensity of this segmented area in the image signal.
    This mean intensity is substracted by the minimum intensity as background substraction

    Args:
        img_binary (3D array): [description]
        img_signal (3D array): [description]

    Returns:
        mean_intensity: [description]
        volume        : 
    """

    #selects the biggest region
    ccs, num_ccs = label(img_binary) #set labels in binary image
    if num_ccs>0:
        properties=regionprops(ccs,img_signal,cache=False) #calculates the properties of the different areas
        best_region = max(properties, key=lambda region: region.area) #selects the biggest region
        median=np.median(best_region.intensity_image[best_region.image])
        return best_region.mean_intensity, best_region.area   #returns the mean of the biggest selected region
    
    else: 
        return 0,0

# mask processing
def mask_postprocessing(input_mask, krn_size , krn_type = 'Disk'):
    '''
    Processes a binary mask to:
        - Remove noisy pixels by eroding and then dilating.
        - Filling holes.
        - Bridging gaps by dilating and then eroding.

    Parameters
    ----------
    input_mask : 3D binary mask of the worm.

    krn_type: type of kernel. By default a disk kernel is used.

    krn_size: kernel size for the removal of noisy pixels. Default value 
        of 1 implies this step is not performed.

        
    Returns
    -------
    input_maks: Processed mask.

    '''
    if krn_size>1 :
        # Kernel selection
        if krn_type == 'Disk':
            krn = skimorph.disk(krn_size)
        elif krn_type == 'Square':
            krn = skimorph.square(krn_size)

        # Dilation
        input_mask = skimorph.binary_dilation(input_mask, krn)

        # Erosion
        input_mask = skimorph.binary_erosion(input_mask, krn)

        # Filling holes
        input_mask = scimorph.binary_fill_holes(input_mask)

            
    return input_mask
    

    """ function uses the package https://fil-finder.readthedocs.io/en/latest/ 
    to create skeleton and prune the she skeleton based on the colored image. 
    Images can be in 3D, but it creates a skeleton based on the maximum intensity projection of the 3D images.
    So the function generates a skeleton in X and Y coordinates. 
    For extra accuracy, the mask is also loaded. 
    

    Args:
        image ([3D image]): [Image with signal used for segmentation]
        mask ([3D image]): [generated mask for the image]
        verbose (bool, optional): [When true, skeletion will be plotted in the image ]. Defaults to False.

    Returns:
        X: 
        Y:
    
    """
    
 
    fil = FilFinder2D(image2D,beamwidth=0*u.pix, mask=mask2D) #creates a class, add beamwith?!
    # idea, put the fill finder on one plane where the worm is in focus 
    fil.preprocess_image()
    fil.create_mask(use_existing_mask=True)
    fil.medskel(verbose=False)
    fil.analyze_skeletons(branch_thresh=40* u.pix, skel_thresh=10 * u.pix, prune_criteria='length')

    skeletonized_image=fil.skeleton_longpath
    [X,Y]=np.where(skeletonized_image==1)

    if (verbose==True):
        plt.figure()
        plt.imshow(image2D, cmap='gray')
        plt.contour(skeletonized_image, colors='r')
        plt.axis('off')
        plt.title('check skeleton')
        plt.show()

    return X,Y