from setuptools import setup, find_packages

try:
    from os import path
    with open(path.join(path.dirname(__file__), 'README.rst'), 'rb') as f:
        long_description = f.read()
except:
    long_description = ""

setup(
    name="MagicFolder",
    description="Synchronize local folder with a central repository",
    long_description=long_description,
    version="0.2",
    license="MIT License",
    author="Alex Morega",
    author_email="public@grep.ro",
    packages=find_packages(),
    install_requires=['argparse'],
    url="http://github.com/alex-morega/MagicFolder",
    entry_points={'console_scripts': ['mf = magicfolder.client:main']},
)
