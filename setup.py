from setuptools import setup, find_packages

contrib = [
    'Markus Rempfler',
]

# setup.
setup(
    name='ggjw',
    version='0.1.0',
    description='grosshans-jobsystem-workflows',
    author=', '.join(contrib),
    packages=find_packages(exclude=[
        'tests',
        'examples',
    ]),
    include_package_data=True,
    install_requires=[
        'luigi',
        'pytest',
        'scikit-image>=0.16,<0.17',
        'numpy',
        'tqdm',
        'faim-luigi',
        'dl-utils'
    ])
