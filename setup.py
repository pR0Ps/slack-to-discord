#!/usr/bin/env python

from setuptools import setup
import os.path


try:
    DIR = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(DIR, "README.md"), encoding="utf-8") as f:
        long_description = f.read()
except Exception:
    long_description=None


setup(
    name="slack-to-discord",
    version="1.0.1",
    description="Extract data from a Slack export and import it into Discord",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pR0Ps/slack-to-discord",
    license="GPLv3",
    python_requires=">=3.6.0",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Operating System :: OS Independent",
        "Topic :: Communications :: Chat",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)"
    ],
    install_requires = [
        "discord.py>=2.0.0,<3.0.0"
    ],
    py_modules=["slack_to_discord"],
    entry_points={
        "console_scripts": [
            "slack-to-discord=slack_to_discord:main"
        ]
    },
)
