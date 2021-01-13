# Jobsystem workflows for the Grosshans lab

[WIP] Intended to be used with the ```minimal-job-system```.

## Installation for development

First, install the dependencies ```faim-luigi``` and ```dl-utils```. E.g. with

```
pip install git+https://github.com/fmi-basel/dl-utils.git
pip install git+https://github.com/fmi-basel/faim-luigi.git
```

Then, 
```
git clone https://github.com/fmi-basel/ggrosshans-jobsystem-workflows.git
cd ggrosshans-jobsystem-workflows/
```

install the actual workflow package:
```
pip install -e .
./fetch_data_and_models.sh
python -m pytest tests/
```
Note that ```./fetch_data_and_models.sh``` needs rsync and a mount to tungsten. If that is not available, please copy them manually (source and destinations can be found in the script).

### Notes

- Install as editable to avoid having to copy/link models
- fetch_data_and_models.sh needs rsync and a mount to tungsten.


### Adjusting task parameters

Some tasks may require appropriate configuration to run correctly on
the host. For example, ```RunBinarySegmentationModelPredictionTask```
may need to be configured with a maximum patch size if the hosts GPU
doesnt have enough VRAM to process the entire input image as one.

This can be done in the ```luigi.cfg``` as follows:

```
[RunBinarySegmentationModelPredictionTaskV0]
patch_size=576
patch_overlap=64

```

Make sure to choose the ```patch_overlap``` appropriately such that
there are no/minimal stitching artifacts.

### Remarks on the Knime-based WormQuantificationWorkflow

Make sure the knime executable is found, either through ```knime``` or 
by setting the path in the config appropriately, e.g.:

```
[WormQuantificationTask]
knime_executable=/tungstenfs/nobackup/ggrossha/svcgrossha/knime_4.1.3/knime
```

To avoid potential JVM heap space errors (```Execute failed: Java heap space```),
consider increasing it through ```-Xmx32g``` (=> sets it to 32G) in ```knime.ini```. 


### How do I run a workflow/task from the command line?

You can run any task or workflow from the command line using luigi. 

For example, to run the ```StkToTifImageCompressionWorkflow```

```
luigi --module ggjw.workflows ggrosshans.StkToTifImageCompressionWorkflow \
  --input-folder /tungstenfs/scratch/ggrossha/Lucas/Live_Imaging/201030_Maike_phat4_scl3 \
  --output-folder /tungstenfs/nobackup/ggrossha/grafmaik/Phat4_Scl3_reporter_Test_Johnson/ \
  --file-pattern "*stk" \
  --local-scheduler
```
  
or as an example for running a task:

```
luigi  --module ggjw.tasks.segmentation.fcn_task RunBinarySegmentationModelPredictionTask \
  --input-folder /tungstenfs/scratch/ggrossha/Marit/HBL1-GFP/20201002_Test5_Johnson \
  --file-pattern "*w2Marit-BF-488-Cam1_*.stk" \
  --output-folder /tungstenfs/scratch/ggrossha/svcgrossha/tmp/segm-test-gpu/ \
  --model-folder ggjw/models/segmentation/worms_from_bf/v2-beta-gpu/ \
  --local-scheduler
```

(Note that the ```\``` are just to split the command over multiple lines. The whole command could also be passed as one line without the ```\```.)

See also:  [Running luigi](https://luigi.readthedocs.io/en/stable/running_luigi.html#running-from-the-command-line)

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





