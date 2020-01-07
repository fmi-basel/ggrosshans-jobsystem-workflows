# Jobsystem workflows for the Grosshans lab

[WIP] Intended to be used with the ```minimal-job-system```.

## Installation notes


### Adjusting task parameters

Some tasks may require appropriate configuration to run correctly on
the host. For example, ```RunBinarySegmentationModelPredictionTask```
may need to be configured with a maximum patch size if the hosts GPU
doesnt have enough VRAM to process the entire input image as one.

This can be done in the ```luigi.cfg``` as follows:

```
[RunBinarySegmentationModelPredictionTask]
patch_size=576
patch_overlap=64

```

Make sure to choose the ```patch_overlap``` appropriately such that
there are no/minimal stitching artifacts.


## Troubleshooting

### Out of memory error (OOM) with segmentation models running on GPU

Make sure the ```patch_size``` is appropriately set (see Adjusting task parameters)

### Issues with CUDNN:

Should you encounter errors like this:

```
Could not create cudnn handle: CUDNN_STATUS_INTERNAL_ERROR
```

Allow Tensorflow to "grow" the model on the GPU:

```
export TF_FORCE_GPU_ALLOW_GROWTH=true
```




