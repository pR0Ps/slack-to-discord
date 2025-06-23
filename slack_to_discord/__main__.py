#!/usr/bin/env python

import argparse
import logging
from discord.utils import setup_logging

from slack_to_discord import run_import


def main():
    parser = argparse.ArgumentParser(
        description="Import Slack chat history into Discord"
    )
    parser.add_argument("-z", "--zipfile", help="The Slack export zip file", required=True)
    parser.add_argument("-t", "--token", help="The Discord bot token", required=True)
    parser.add_argument("-g", "--guild", help="The Discord Guild (ie. server name) to import history into", required=True)
    parser.add_argument("-c", "--channels", help="When specified, will only import the provided channels. Do not include the '#'s (ex: 'general', not '#general')", nargs="*")
    parser.add_argument("-s", "--start", help="The date to start importing from (YYYY-MM-DD)", required=False, default=None)
    parser.add_argument("-e", "--end", help="The date to end importing at (YYYY-MM-DD)", required=False, default=None)
    parser.add_argument("-p", "--all-private", help="Import all channels as private channels in Discord", action="store_true", default=False)
    parser.add_argument("-r", "--real-names", help="Use real names from Slack instead of usernames", action="store_true", default=False)
    parser.add_argument("-i", "--inline-dates", help="Include dates inline in messages rather than posting date-separator messages", action="store_true", default=False)
    parser.add_argument("-df", "--date-format", help="Date format string for Discord messages (default: %%Y-%%m-%%d)", required=False, default="%Y-%m-%d")
    parser.add_argument("-tf", "--time-format", help="Time format string for Discord messages (default: %%H:%%M)", required=False, default="%H:%M")
    parser.add_argument("-v", "--verbose", help="Show more verbose logs", action="store_true")
    args = parser.parse_args()

    setup_logging(level=logging.DEBUG if args.verbose else logging.INFO)

    run_import(
        zipfile=args.zipfile,
        token=args.token,
        guild_name=args.guild,
        channels=args.channels,
        all_private=args.all_private,
        real_names=args.real_names,
        inline_dates=args.inline_dates,
        start=args.start,
        end=args.end,
        date_format=args.date_format,
        time_format=args.time_format
    )

if __name__ == "__main__":
    main()
