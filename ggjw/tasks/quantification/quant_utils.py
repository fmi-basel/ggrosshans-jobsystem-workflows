#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import glob
import os
import re
from skimage import io
from skimage import measure
from scipy.ndimage.measurements import label
from skimage.measure import regionprops

#-----------------------------------------------------------------------------
#read image and get information from image


def read_image(path):
    img = io.imread(path)
    return img.squeeze()


def _get_meta_tuple(path):
    return tuple(re.split('_s(\d+)_t(\d+)\..+', path)[1:])


def get_meta_info(path):
    pictureinfo = _get_meta_tuple(path)
    return {'Position': pictureinfo[0], 'Frame': pictureinfo[1]}


#-----------------------------------------------------------------------------
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
    #creating list1 with GFP images
    lhs_paths = sorted(glob.glob(os.path.join(dir2, fname_pattern2)))

    #checks if the channel pattern is present in the directory
    if not lhs_paths:
        raise FileNotFoundError(
            'No files in image_folder match the given image_file_pattern={}'.
            format(fname_pattern1))

    lhs_paths = {_get_meta_tuple(path): path for path in lhs_paths}

    for rhs_path in sorted(glob.glob(os.path.join(dir1, fname_pattern1))):
        lhs_match = lhs_paths.get(_get_meta_tuple(rhs_path), None)
        if lhs_match is None:
            raise FileNotFoundError(
                'the segmented pair was not found for ={}'.format(rhs_path))
        yield lhs_match, rhs_path

    # #creating list2 with segmented images
    # list_2=[]
    # for name in list_1:
    #     match=re.split('(\S*_)(\S*)(_s\S*)', os.path.basename(name)) #space=[0],exp_name=[1],channelname=[2],wrom&tp=[3]
    #     #finds the matched image pairs for exp_name and worm&tp in dir2
    #     matching_BF=glob.glob(os.path.join(dir2, match[1]+"*"+ match[3]))

    #     #checks if the segmented image pair is present
    #     if not matching_BF:
    #         raise FileNotFoundError(
    #             'the segmented pair was not found for ={}'
    #             .format(name))

    #     #if image pair is present, first entry of the mathing file [0] is added to list2
    #     list_2.append(matching_BF[0])
    # return list_1, list_2


#----------------------------------------------------------------------------
#selecting good chambers and  slides in focus


def calculate_worm_properties(img_binary, img_signal):
    '''
    worm_proprties_output is a function that  calculates different properties 
    (area, mean_intensity, min_intensity) of the signal images, based on the 
    biggest segmented area in the binary image

    Parameters
    ----------
    img_binary : binary image
    img_signal : signal image

    Returns
    -------
    binary_image : image that only contains the best area
    area         : area of the best area
    mean_intensity: mean intensity of the signal image in the best area
    min_intensity: minimum intensity of the signal image in the best area

    '''

    #select biggest area
    ccs, num_ccs = label(img_binary)  #set labels in binary image
    properties = regionprops(
        ccs, img_signal, ['area', 'mean_intensity', 'min_intensity'
                          ])  #calculates the properties of the different areas
    best_region = max(
        properties,
        key=lambda region: region.area)  #selects the biggest region

    binary_image = (ccs == best_region.label).astype(np.uint8)
    min_intensity = best_region.min_intensity
    mean_intensity = best_region.mean_intensity
    area = best_region.area

    return area, mean_intensity, min_intensity


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

    start, stop = find_best_interval(
        mean_img, threshold
    )  #finds the best interval where intensity is above avarage strashold

    selected_slides_signal = signal_image[
        start:stop]  # removes the slides that are out of focus
    #selected_slides_binary = binary_image[start:stop]

    # plt.figure()
    # plt.plot(mean_img)
    # plt.plot(threshold,'r')
    # plt.plot([start,start],[0,threshold],'--r')
    # plt.plot([stop,stop],[0,threshold],'--r')
    # plt.title("mean intensity of zlides per plane")
    # plt.xlabel("z-slide")
    # plt.ylabel("mean intensity")
    # plt.close()

    return selected_slides_signal


def find_best_interval(values, threshold):
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
