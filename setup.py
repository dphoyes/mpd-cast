from setuptools import setup

setup(
    name='mpd-cast',
    version='0.1',
    packages=['mpd_cast'],
    install_requires=[
        'python_mpd_server',
        'anyio',
        'trio',
        'pychromecast',
        'flask',
    ],
    license='GPLv3',
    author='dphoyes',
    author_email='',
    description='MPD server for Chromecasts'
)
