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

### We have trained and validated a new model version for the ```WormSegmentationFromBrightFieldWorkflow```. How do we release it in the jobsystem?

1. Make sure you have deployed the model including the necessary preprocessing (e.g. ```ManualModelDeploymentTask``` or ```DeployAndTestModelTask``` from [faim-worm-segmentation](https://github.com/fmi-basel/faim-worm-segmentation).
2. Stage the model with the previous versions, giving it a subfolder of incremented value, e.g. ```v3```. (See ```fetch_data_and_models.sh``` for the local staging area).
3. In [ggjw.workflows.worm_segmentation](https://github.com/fmi-basel/ggrosshans-jobsystem-workflows/blob/master/ggjw/workflows/worm_segmentation.py):
   - Add the new version to ```MODEL_FOLDER_FOR_VERSION```
   - Add the new version to the ```WormSegmentationFromBrightFieldWorkflow```.```model_version``` choice parameter and consider making it default. Note that this has to be explicit (i.e. you **cannot** use something like ```list(MODEL_FOLDER_FOR_VERSION.keys())```). Otherwise, the parameter parsing from the minimal-job-system will not work.
   - Add description of new version to ```WormSegmentationFromBrightFieldWorkflow```.```model_version```'s docstring.
4. Install new version on the host machine and make sure all tests (including the test of the new model) pass.
5. In the admin portal of the minimal-job-system, synchronize jobs again to update the parameters of the workflow. After that, you should be able to select the new model for ```WormSegmentationFromBrightFieldWorkflow``` and run it.

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

## FAQ

### Is there anything I should do regarding code style/conventions before creating a pull request?

It's recommended to run ```pylint``` on your new code to see if there's any errors. Use common sense to judge whether the warning is important or not.

```
python -m pylint ggjw/something_new.py
```
(you might need to do ```pip install pylint``` if you're using it the first time).

Also, please reformat the new code with ```yapf```:

```
python -m yapf -i ggjw/something_new.py
```
(you might need to do ```pip install yapf``` if you're using it the first time). Under some circumstances the auto-format doesn't yield a great outcome (e.g. when there's a lot of raw np.ndarrays manually defined for a unit test). In that case, you can use the ```# yapf: disable``` and ```# yapf: enable``` to protect the block. Use this sparingly.

### Do I really have to write a test for my new workflow?

Yes. You can use the existing ```tests/``` as a guide.




