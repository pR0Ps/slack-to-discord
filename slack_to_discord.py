import argparse
import contextlib
import glob
import html
import io
import json
import os
import re
import tempfile
import urllib
import zipfile
from datetime import datetime

import discord


# Date and time formats
DATE_FORMAT = "%Y-%m-%d"
TIME_FORMAT = "%H:%M"

# Formatting options for messages
THREAD_FORMAT = ">>>> {date} {time} <**{username}**> {text}"
MSG_FORMAT = "{time} <**{username}**> {text}"

# Create a separator between dates? (None for no)
DATE_SEPARATOR = "{:-^50}"

MENTION_RE = re.compile(r"<([@!#])([^>]*?)(?:\|([^>]*?))?>")
LINK_RE = re.compile(r"<((?:https?|mailto|tel):[A-Za-z0-9_\+\.\-\/\?\,\=\#\:\@\(\)]+)\|([^>]+)>")
EMOJI_RE = re.compile(r":([^ /<>:])([^ /<>:]+):")


def emoji_replace(s):
    # Convert -'s to "_"s except when in the 1st char like :-1:
    # This fixes things like ":woman_shrugging:" being ":woman-shrugging:" on Slack
    s = EMOJI_RE.sub(lambda x: ":{}{}:".format(x.group(1), x.group(2).replace("-", "_")) , s)

    # Custom substitutions (generalize these if possible)
    s = (s
            .replace(":thumbsup_all:", ":thumbsup:")
            .replace(":facepunch:", ":punch:")
            .replace(":the_horns:", ":sign_of_the_horns:")
            .replace(":simple_smile:", ":slightly_smiling_face:")
            .replace(":clinking_glasses:", ":champagne_glass:")
            .replace(":tornado:", ":cloud_with_tornado:")
            .replace(":car:", ":red_car:")
            .replace(":us:", ":flag_us:")
            .replace(":snow_cloud:", ":cloud_with_snow:")
            .replace(":snowman:", ":snowman2:")
            .replace(":snowman_without_snow:", ":snowman:")
            .replace(":crossed_fingers:", ":fingers_crossed:")
            .replace(":hocho:", ":knife:")
            .replace(":waving_black_flag:", ":flag_black:")
            .replace(":waving_white_flag:", ":flag_white:")
            .replace(":woman_heart_man:", ":couple_with_heart_woman_man:")
            .replace(":man_heart_man:", ":couple_with_heart_mm:")
            .replace(":woman_heart_woman:", ":couple_with_heart_ww:")
            .replace(":man_kiss_man:", ":couplekiss_mm:")
            .replace(":woman_kiss_woman:", ":couplekiss_ww:")
        )
    return s


def slack_usermap(d):
    with open(os.path.join(d, "users.json"), 'rb') as fp:
        data = json.load(fp)
    return {x["id"]: x["name"] for x in data}


def slack_channels(d):
    with open(os.path.join(d, "channels.json"), 'rb') as fp:
        data = json.load(fp)
    return [x["name"] for x in data]


def slack_channel_messages(d, channel_name):
    users = slack_usermap(d)

    def mention_repl(m):
        type_ = m.group(1)
        target = m.group(2)
        channel_name = m.group(3)

        if type_ == "#":
            return "`#{}`".format(channel_name)
        elif channel_name is not None:
            return m.group(0)

        if type_ == "@":
            return "`@{}`".format(users.get(target, "[unknown]"))
        elif type_ == "!":
            return "`@{}`".format(target)
        return m.group(0)

    messages = {}
    file_ts_map = {}
    for file in sorted(glob.glob(os.path.join(d, channel_name, "*.json"))):
        with open(file, 'rb',) as fp:
            data = json.load(fp)
        for d in sorted(data, key=lambda x: x["ts"]):
            text = d["text"]
            text = MENTION_RE.sub(mention_repl, text)
            text = LINK_RE.sub(lambda x: x.group(1), text)
            text = emoji_replace(text)
            text = html.unescape(text)

            ts = d["ts"]

            user_id = d.get("user")
            subtype = d.get("subtype", "")
            files = d.get("files", [])
            thread_ts = d.get("thread_ts", ts)

            # add bots to user map as they're discovered
            if subtype.startswith("bot_") and "bot_id" in d and d["bot_id"] not in users:
                users[d["bot_id"]] = d.get("username", "[unknown bot]")
                user_id = d["bot_id"]

            # Treat file comments as threads started on the message that posted the file
            if subtype == "file_comment":
                text = d["comment"]["comment"]
                user_id = d["comment"]["user"]
                file_id = d["file"]["id"]
                thread_ts = file_ts_map.get(file_id, ts)
                # remove the commented file from this messages's files
                files = [x for x in files if x["id"] != file_id]

            # Store a map of fileid to ts so file comments can be treated as replies
            for f in files:
                file_ts_map[f["id"]] = ts

            msg = {
                "username": users.get(user_id, "[unknown]"),
                "datetime": datetime.fromtimestamp(float(ts)),
                "text": text,
                "replies": {},
                "reactions": {
                    emoji_replace(":{}:".format(x["name"])): [
                        users.get(u, "[unknown]") for u in x["users"]
                    ]
                    for x in d.get("reactions", [])
                },
                "files": [
                    {
                        "name": x["name"],
                        "url": x["url_private"]
                    }
                    for x in files
                ],
            }

            # If this is a reply, add it to the parent message's replies
            # Replies have a "thread_ts" that differs from their "ts"
            if thread_ts != ts:
                if thread_ts not in messages:
                    # Orphan thread message - skip it
                    continue
                messages[thread_ts]["replies"][ts] = msg
            else:
                messages[ts] = msg

    # Sort the dicts by timestamp and yield the messages
    for msg in (messages[x] for x in sorted(messages.keys())):
        msg["replies"] = [msg["replies"][x] for x in sorted(msg["replies"].keys())]
        yield msg


def make_files(msg):
    for f in msg["files"]:
        try:
            data = urllib.request.urlopen(f["url"]).read()
        except Exception:
            continue
        yield discord.File(fp=io.BytesIO(data), filename=f["name"])


def make_discord_msg(msg, is_reply):
    # Show reactions listed in an embed
    embed = None
    if msg["reactions"]:
        embed = discord.Embed(
            description="\n".join(
                "{} {}".format(k, ", ".join(v)) for k, v in msg["reactions"].items()
            )
        )

    # Format the date
    msg["time"] = msg["datetime"].strftime(TIME_FORMAT)
    msg["date"] = msg["datetime"].strftime(DATE_FORMAT)

    return {
        "content": (THREAD_FORMAT if is_reply else MSG_FORMAT).format(**msg),
        "files": list(make_files(msg)),
        "embed": embed,
    }


class MyClient(discord.Client):

    def __init__(self, *args, data_dir, guild_name, start, end, **kwargs):
        self._data_dir = data_dir
        self._guild_name = guild_name
        self._prev_msg = None
        self._start, self._end = [datetime.strptime(x, DATE_FORMAT) if x else None for x in (start, end)]

        self._started = False # TODO: async equiv of a threading.event
        super().__init__(*args, **kwargs)

    async def on_ready(self):
        if self._started:
            return

        print("Done!")
        try:
            g = discord.utils.get(self.guilds, name=self._guild_name)
            if g is None:
                print("Guild {} not accessible to bot".format(self._guild_name))
                return

            await self._run_import(g)
        finally:
            print("Bot logging out")
            await self.logout()


    async def _send_slack_msg(self, channel, msg, is_reply=False):

        if not is_reply and DATE_SEPARATOR:
            msg_date = msg["datetime"].date()
            if (
                not self._prev_msg or
                self._prev_msg["datetime"].date() != msg_date
            ):
                await channel.send(
                    content=DATE_SEPARATOR.format(
                        msg_date.strftime(DATE_FORMAT)
                    )
                )
            self._prev_msg = msg

        data = make_discord_msg(msg, is_reply)

        with contextlib.suppress(discord.errors.HTTPException):
            return await channel.send(**data)

        if data["files"]:
            # Files that are too big could cause errors
            # Try again just linking them instead of uploading
            data.pop("files")
            data["content"] += "\n\nFiles:\n" + "\n".join(f["url"] for f in msg["files"])

            with contextlib.suppress(discord.errors.HTTPException):
                return await channel.send(**data)

        print("Failed to post message: <{username}> {text}".format(**msg))
        return None

    async def _run_import(self, g):
        self._started = True
        # TODO: integrate into emoji_replace (emoji_replace can be in the Client to check self.emoji. slack_channel_messages too)
        print(self.emojis)
        print([str(x) for x in self.emojis])
        print("Importing messages...")

        existing_channels = {x.name: x for x in g.text_channels}

        for c in slack_channels(self._data_dir):

            print("Processing channel {}...".format(c))
            if c not in existing_channels:
                if False: # TODO: What does a private channel look like in Slack's export?
                    print("Creating private channel")
                    overwrites = {
                        g.default_role: discord.PermissionOverwrite(read_messages=False),
                        g.me: discord.PermissionOverwrite(read_messages=True),
                    }
                    ch = await g.create_text_channel(c, overwrites=overwrites)
                else:
                    print("Creating public channel")
                    ch = await g.create_text_channel(c)
            else:
                ch = existing_channels[c]

            print("Sending messages...", end="", flush=True)
            for msg in slack_channel_messages(self._data_dir, c):
                # skip messages that are too early, stop when messages are too late
                if self._end and msg["datetime"] > self._end:
                    break
                elif self._start and  msg["datetime"] < self._start:
                    continue

                await self._send_slack_msg(ch, msg)
                for rmsg in msg["replies"]:
                    await self._send_slack_msg(ch, rmsg, is_reply=True)
            print("Done!")

def main():
    parser = argparse.ArgumentParser(
        description="Import Slack chat history into Discord"
    )
    parser.add_argument("-z", "--zipfile", help="The Slack export zip file", required=True)
    parser.add_argument("-g", "--guild", help="The Discord Guild to import history into", required=True)
    parser.add_argument("-t", "--token", help="The Discord bot token", required=True)
    parser.add_argument("-s", "--start", help="The date to start importing from", required=False, default=None)
    parser.add_argument("-e", "--end", help="The date to end importing at", required=False, default=None)
    args = parser.parse_args()

    print("Extracting zipfile...", end="", flush=True)
    with tempfile.TemporaryDirectory() as t:
        with zipfile.ZipFile(args.zipfile, 'r') as z:
            z.extractall(t)
        print("Done!")

        print("Logging the bot into Discord...", end="", flush=True)
        client = MyClient(data_dir=t, guild_name=args.guild, start=args.start, end=args.end)
        client.run(args.token)

if __name__ == "__main__":
    main()
