from setuptools import setup, find_packages

contrib = [
    'Markus Rempfler',
]

# setup.
setup(
    name='ggjw',
    version='0.0.1',
    description='grosshans-jobsystem-workflows',
    author=', '.join(contrib),
    packages=find_packages(exclude=[
        'tests',
        'examples',
    ]),
    include_package_data=False,
    install_requires=[
        'luigi',
        'pytest',
        'scikit-image',
        'numpy',
        'tqdm',
        'faim-luigi',
        'dl-utils'
    ])
