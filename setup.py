from setuptools import setup, find_packages

setup(
    name="MagicFolder",
    description="Synchronize local folder with a central repository",
    version="0.2",
    license="MIT License",
    author="Alex Morega",
    author_email="public@grep.ro",
    packages=find_packages(),
    install_requires=['argparse'],
    url="http://github.com/alex-morega/MagicFolder",
    entry_points={'console_scripts': ['mf = magicfolder.client:main']},
)
