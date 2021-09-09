from setuptools import setup, find_packages

contrib = [
    'Markus Rempfler', 'Marit van der Does', 'Jan Egliner', 'Tim-Oliver Buchholz'
]

# setup.
setup(
    name='ggjw',
    version='0.5.1',
    description='grosshans-jobsystem-workflows',
    author=', '.join(contrib),
    packages=find_packages(exclude=[
        'tests',
        'examples',
    ]),
    include_package_data=True,
    install_requires=[
        'luigi>=2.8',
        'pytest',
        'pandas',
        'scikit-image>=0.16,<0.17',
        'numpy',
        'tqdm',
        'faim-luigi>=0.1.1',
        'dl-utils',
        'tifffile'
    ])
