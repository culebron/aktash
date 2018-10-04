#!/usr/bin/env python3

from setuptools import setup

install_requires = [
    'argh',
    'decorator',
    'geojson',
    'geopandas',
    'pyproj',
    'rtree',
    'tqdm',
]

setup(
    name='aktash',
    version='0.1.0',
    description='Nomadic GIS toolset. GeoPandas + advanced and typical operations. Supports GeoPackage, GeoJSON, CSV formats.',
    #long_description=open('README.md').read(),
    author='Dmitri Lebedev',
    author_email='dl@peshemove.org',
    classifiers=[
        'Topic :: Utilities'
    ],
    packages=[
        'aktash',
    ],
    entry_points={
        'console_scripts': [
            'aktash = aktash:main',
        ]
    },
    install_requires=install_requires,
    tests_require=['pytest']
)
