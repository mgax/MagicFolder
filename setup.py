from setuptools import setup, find_packages

setup(
    name="MagicFolder",
    description="Synchronize local folder with a central repository",
    version="0.1",
    license="MIT License",
    author="Alex Morega",
    author_email="public@grep.ro",
    packages=find_packages(),
    install_requires=['Probity', 'argparse'],
    entry_points={'console_scripts': ['mf = magicfolder.client:main',
                                      'mf-server = magicfolder.server:main']},
)
