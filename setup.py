from setuptools import setup, find_packages

setup(
    name="SyncIt",
    description="Synchronize local folder with a central repository",
    version="0.1",
    license="MIT License",
    author="Alex Morega",
    author_email="public@grep.ro",
    packages=find_packages(),
    install_requires=['Probity'],
    entry_points={'console_scripts': ['syncit = syncit.client:main',
                                      'syncit-server = syncit.server:main']},
)
