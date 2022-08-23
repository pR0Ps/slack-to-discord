#!/usr/bin/env python

from setuptools import setup

setup(
    name="slack-to-discord",
    version="0.0.1",
    description="Extract data from a Slack export and import it into Discord",
    url="https://github.com/pR0Ps/slack-to-discord",
    license="GPLv3",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    install_requires = [
        "discord.py>=2.0.0,<3.0.0"
    ],
    py_modules=["slack_to_discord"],
    entry_points={"console_scripts": ["slack-to-discord=slack_to_discord:main"]},
)
