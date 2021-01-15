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
ATTACHMENT_TITLE_TEXT = "<*uploaded a file*> {title}"
ATTACHMENT_ERROR_APPEND = "\n<file downsized/omitted due to size restrictions. See original at <{url}>>"

# Create a separator between dates? (None for no)
DATE_SEPARATOR = "{:-^50}"

MENTION_RE = re.compile(r"<([@!#])([^>]*?)(?:\|([^>]*?))?>")
LINK_RE = re.compile(r"<((?:https?|mailto|tel):[A-Za-z0-9_\+\.\-\/\?\,\=\#\:\@\(\)]+)\|([^>]+)>")
EMOJI_RE = re.compile(r":([^ /<>:]+):(?::skin-tone-(\d):)?")


# Map Slack emojis to Discord's versions
# Note that dashes will have been converted to underscores before this is processed
GLOBAL_EMOJI_MAP = {
    "thumbsup_all": "thumbsup",
    "facepunch": "punch",
    "the_horns": "sign_of_the_horns",
    "simple_smile": "slightly_smiling_face",
    "clinking_glasses": "champagne_glass",
    "tornado": "cloud_with_tornado",
    "car": "red_car",
    "us": "flag_us",
    "snow_cloud": "cloud_with_snow",
    "snowman": "snowman2",
    "snowman_without_snow": "snowman",
    "crossed_fingers": "fingers_crossed",
    "hocho": "knife",
    "waving_black_flag": "flag_black",
    "waving_white_flag": "flag_white",
    "woman_heart_man": "couple_with_heart_woman_man",
    "man_heart_man": "couple_with_heart_mm",
    "woman_heart_woman": "couple_with_heart_ww",
    "man_kiss_man": "couplekiss_mm",
    "woman_kiss_woman": "couplekiss_ww",
}

def emoji_replace(s, emoji_map):
    def replace(match):
        e, t = match.groups()

        # Emojis in the emoji_map already have bounding :'s and can't have skin
        # tones applied to them so just directly return them.
        if e in emoji_map:
            return emoji_map[e]

        # Convert -'s to "_"s except the 1st char (ex. :-1:)
        # On Slack some emojis use underscores and some use dashes
        # On Discord everything uses underscores
        if len(e) > 1 and "-" in e[1:]:
            e = e[0] + e[1:].replace("-", "_")

        if e in GLOBAL_EMOJI_MAP:
            e = GLOBAL_EMOJI_MAP[e]

        # Convert Slack's skin tone system to Discord's
        if t is not None:
            return ":{}_tone{}:".format(e, int(t)-1)
        else:
            return ":{}:".format(e)

    return EMOJI_RE.sub(replace, s)


def slack_usermap(d):
    with open(os.path.join(d, "users.json"), 'rb') as fp:
        data = json.load(fp)
    return {x["id"]: x["name"] for x in data}


def slack_channels(d):
    with open(os.path.join(d, "channels.json"), 'rb') as fp:
        data = json.load(fp)
    return [x["name"] for x in data]


def slack_channel_messages(d, channel_name, emoji_map):
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
            text = emoji_replace(text, emoji_map)
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

            dt = datetime.fromtimestamp(float(ts))
            msg = {
                "username": users.get(user_id, "[unknown]"),
                "datetime": dt,
                "time": dt.strftime(TIME_FORMAT),
                "date": dt.strftime(DATE_FORMAT),
                "text": text,
                "replies": {},
                "reactions": {
                    emoji_replace(":{}:".format(x["name"]), emoji_map): [
                        users.get(u, "[unknown]") for u in x["users"]
                    ]
                    for x in d.get("reactions", [])
                },
                "files": [
                    {
                        # Make sure names have the correct extension (can cause pictures to not be shown)
                        "name": ("{name}" if x["name"].lower().endswith(".{filetype}".format(**x).lower()) else "{name}.{filetype}").format(**x),
                        "title": x["title"],
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


def make_discord_msgs(msg, is_reply):

    msg_fmt = (THREAD_FORMAT if is_reply else MSG_FORMAT)

    # Show reactions listed in an embed
    embed = None
    if msg["reactions"]:
        embed = discord.Embed(
            description="\n".join(
                "{} {}".format(k, ", ".join(v)) for k, v in msg["reactions"].items()
            )
        )

    # Send the original message without any files (only if there is any content in it)
    if msg.get("text") or embed:
        yield {
            "content": msg_fmt.format(**msg),
            "embed": embed,
        }

    # Send one messge per image that was posted (using the picture title as the message)
    for f in msg["files"]:
        content = msg_fmt.format(**{**msg, "text": ATTACHMENT_TITLE_TEXT.format(**f)})

        # Attempt to download the file from slack and re-upload it to Discord
        # Fall back to adding the URL to the message
        fileobj = None
        try:
            fileobj = discord.File(fp=io.BytesIO(urllib.request.urlopen(f["url"]).read()), filename=f["name"])
        except Exception:
            content += ATTACHMENT_ERROR_APPEND.format(*f)

        yield {
            "content": content,
            "file": fileobj
        }


class MyClient(discord.Client):

    def __init__(self, *args, data_dir, guild_name, start, end, **kwargs):
        self._data_dir = data_dir
        self._guild_name = guild_name
        self._prev_msg = None
        self._start, self._end = [datetime.strptime(x, DATE_FORMAT).date() if x else None for x in (start, end)]

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
            msg_date = msg["date"]
            if (
                not self._prev_msg or
                self._prev_msg["date"] != msg_date
            ):
                await channel.send(content=DATE_SEPARATOR.format(msg_date))
            self._prev_msg = msg

        for data in make_discord_msgs(msg, is_reply):
            with contextlib.suppress(discord.errors.HTTPException):
                await channel.send(**data)
                continue

            if data["file"]:
                # Files that are too big could cause errors
                # Try again just linking them instead
                f = data.pop("file")
                data["content"] += ATTACHMENT_ERROR_APPEND.format(*f)

                with contextlib.suppress(discord.errors.HTTPException):
                    await channel.send(**data)
                    continue

            print("Failed to post message: <{username}> {text}".format(**msg))

    async def _run_import(self, g):
        self._started = True
        emoji_map = {x.name: str(x) for x in self.emojis}

        print("Importing messages...")

        existing_channels = {x.name: x for x in g.text_channels}

        for c in slack_channels(self._data_dir):
            ch = None

            print("Processing channel {}...".format(c))
            print("Sending messages...", end="", flush=True)

            for msg in slack_channel_messages(self._data_dir, c, emoji_map):
                # skip messages that are too early, stop when messages are too late
                if self._end and msg["datetime"].date() > self._end:
                    break
                elif self._start and  msg["datetime"].date() < self._start:
                    continue

                # Now that we have a message to send, get/create the channel to send it to
                if ch is None:
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

                # Send message and threaded replies
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
