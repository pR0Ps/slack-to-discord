#1/usr/bin/env python

import argparse
import contextlib
import functools
import glob
import html
import io
import json
import logging
import os
import re
import tempfile
import textwrap
import urllib
from zipfile import ZipFile
from datetime import datetime
from urllib.parse import urlparse

import discord
from discord.errors import Forbidden


# Discord size limits
MAX_MESSAGE_SIZE = 2000
MAX_THREADNAME_SIZE = 100

# Date and time formats
DATE_FORMAT = "%Y-%m-%d"
TIME_FORMAT = "%H:%M"

# Formatting options for messages
MSG_FORMAT = "`{time}` {text}"
BACKUP_THREAD_NAME = "{date} {time}"  # used when the message to create the thread from has no text
ATTACHMENT_TITLE_TEXT = "<*uploaded a file*> {title}"
ATTACHMENT_ERROR_APPEND = "\n<file thumbnail used due to size restrictions. See original at <{url}>>"

# Create a separator between dates? (None for no)
DATE_SEPARATOR = "`{:-^30}`"

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


__log__ = logging.getLogger(__name__)


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


def slack_usermap(d, real_names=False):
    with open(os.path.join(d, "users.json"), "rb") as fp:
        data = json.load(fp)

    def get_userinfo(userdata):
        profile = userdata["profile"]
        if real_names:
            name = profile["real_name_normalized"]
        else:
            # bots sometimes don't set a display name - fall back to the internal username
            name = profile["display_name_normalized"] or userdata["name"]
        return (name, profile.get("image_original"))


    r = {x["id"]: get_userinfo(x) for x in data}
    r["USLACKBOT"] = ("Slackbot", None)
    r["B01"] = ("Slackbot", None)
    return r


def slack_channels(d):
    topic = lambda x: "\n\n".join([x[k]["value"] for k in ("purpose", "topic") if x[k]["value"]])
    pins = lambda x: set(p["id"] for p in x.get("pins", []))

    for is_private, file in ((False, "channels.json"), (True, "groups.json")):
        with contextlib.suppress(FileNotFoundError):
            with open(os.path.join(d, file), "rb") as fp:
                for x in json.load(fp):
                    yield x["name"], topic(x), pins(x), is_private


def slack_filedata(f):
    # Make sure the filename has the correct extension
    # Not fixing these issues can cause pictures to not be shown
    name, *ext = (f.get("name") or "unnamed").rsplit(".", 1)

    ext = ext[0] if ext else ""
    ft = f.get("filetype") or ""
    if ext.lower() == ft.lower():
        # extension is already correct, don't fix it
        ft = None

    newname = ".".join(x for x in (name or "unknown", ext, ft) if x)

    # Make a list of thumbnails for this file in case the original can't be posted
    thumbs = [f[t] for t in sorted((k for k in f if re.fullmatch("thumb_(\d+)", k)), key=lambda x: int(x.split("_")[-1]), reverse=True)]
    if "thumb_video" in f:
        thumbs.append(f["thumb_video"])

    return {
        "name": newname,
        "title": f.get("title") or newname,
        "url": f["url_private"],
        "thumbs": thumbs
    }


def slack_channel_messages(d, channel_name, users, emoji_map, pins):
    def mention_repl(m):
        type_ = m.group(1)
        target = m.group(2)
        channel_name = m.group(3)

        if type_ == "#":
            return "`#{}`".format(channel_name)
        elif channel_name is not None:
            return m.group(0)

        if type_ == "@":
            return "`@{}`".format(users[target][0] if target in users else "[unknown]")
        elif type_ == "!":
            return "`@{}`".format(target)
        return m.group(0)

    channel_dir = os.path.join(d, channel_name)
    if not os.path.isdir(channel_dir):
        __log__.error("Data for channel '#%s' not found in export", channel_name)

    messages = {}
    file_ts_map = {}
    for file in sorted(glob.glob(os.path.join(channel_dir, "*.json"))):
        with open(file, "rb") as fp:
            data = json.load(fp)
        for d in sorted(data, key=lambda x: x["ts"]):
            text = d["text"]
            text = MENTION_RE.sub(mention_repl, text)
            text = LINK_RE.sub(lambda x: x.group(1), text)
            text = emoji_replace(text, emoji_map)
            text = html.unescape(text)
            text = text.rstrip()

            ts = d["ts"]

            user_id = d.get("user")
            subtype = d.get("subtype", "")
            files = d.get("files", [])
            thread_ts = d.get("thread_ts", ts)
            events = {}

            # add bots to user map as they're discovered
            if subtype.startswith("bot_") and "bot_id" in d and d["bot_id"] not in users:
                users[d["bot_id"]] = (d.get("username", "[unknown bot]"), None)
                user_id = d["bot_id"]

            # Treat file comments as threads started on the message that posted the file
            elif subtype == "file_comment":
                text = d["comment"]["comment"]
                user_id = d["comment"]["user"]
                file_id = d["file"]["id"]
                thread_ts = file_ts_map.get(file_id, ts)
                # remove the commented file from this messages's files
                files = [x for x in files if x["id"] != file_id]

            # Handle "/me <text>" commands (italicize)
            elif subtype == "me_message":
                text = "*{}*".format(text)

            elif subtype == "reminder_add":
                text = "<*{}*>".format(text.strip())

            # Handle channel operations
            elif subtype == "channel_join":
                text = "<*joined the channel*>"
            elif subtype == "channel_leave":
                text = "<*left the channel*>"
            elif subtype == "channel_archive":
                text = "<*archived the channel*>"

            # Handle setting channel topic/purpose
            elif subtype == "channel_topic" or subtype == "channel_purpose":
                events["topic"] = d.get("topic", d.get("purpose"))
                if events["topic"]:
                    text = "<*set the channel topic*>: {}".format(events["topic"])
                else:
                    text = "<*cleared the channel topic*>"

            if ts in pins:
                events["pin"] = True

            # Store a map of fileid to ts so file comments can be treated as replies
            for f in files:
                file_ts_map[f["id"]] = ts

            # Ignore tombstoned (removed) files and ones that don't have a URL
            files = [x for x in files if x["mode"] != "tombstone" and x.get("url_private")]

            dt = datetime.fromtimestamp(float(ts))
            msg = {
                "userinfo": users.get(user_id, ("[unknown]", None)),
                "datetime": dt,
                "time": dt.strftime(TIME_FORMAT),
                "date": dt.strftime(DATE_FORMAT),
                "text": text,
                "replies": {},
                "reactions": {
                    emoji_replace(":{}:".format(x["name"]), emoji_map): [
                        users[u][0].replace("_", "\\_") if u in users else "[unknown]"
                        for u in x["users"]
                    ]
                    for x in d.get("reactions", [])
                },
                "files": [slack_filedata(f) for f in files],
                "events": events
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


def mark_end(iterable):
    # yield (is_last, x) for x in iterable
    it = iter(iterable)
    try:
        b = next(it)
    except StopIteration:
        return

    try:
        while True:
            a = b
            b = next(it)
            yield False, a
    except StopIteration:
        yield True, a


def make_discord_msgs(msg):

    # Show reactions listed in an embed
    embed = None
    if msg["reactions"]:
        embed = discord.Embed(
            description="\n".join(
                "{} {}".format(k, ", ".join(v)) for k, v in msg["reactions"].items()
            )
        )

    # Split the text into chunks to keep it under MAX_MESSAGE_SIZE
    # Send everything except the last chunk
    content = None
    prefix_len = len(MSG_FORMAT.format(**{**msg, "text": ""}))
    for is_last, chunk in mark_end(textwrap.wrap(
        text=msg.get("text") or "",
        width=MAX_MESSAGE_SIZE - prefix_len,
        drop_whitespace=False,
        replace_whitespace=False
    )):
        content = MSG_FORMAT.format(**{**msg, "text": chunk.strip()})
        if not is_last:
            yield {
                "content": content
            }

    # Send the original message without any files
    if len(msg["files"]) == 1:
        # if there is a single file attached, put reactions on the the file
        if content:
            yield {
                "content": content,
            }
    elif content or embed:
        # for no/multiple files, put reactions on the message (even if blank)
        yield {
            "content": content,
            "embed": embed,
        }
        embed = None

    # Send one messge per image that was posted (using the picture title as the message)
    for f in msg["files"]:
        yield {
            "content": MSG_FORMAT.format(**{**msg, "text": ATTACHMENT_TITLE_TEXT.format(**f)}),
            "file_data": f,
            "embed": embed
        }
        embed = None


def file_upload_attempts(data):
    # Files that are too big cause issues
    # yield data to try to send (original, then thumbnails)
    fd = data.pop("file_data", None)
    if not fd:
        yield data
        return

    for i, url in enumerate([fd["url"]] + fd.get("thumbs", [])):
        if i > 0:
            # Trying thumbnails - get the filename from Slack (it has the correct extension)
            filename = urlparse(url).path.rsplit("/", 1)[-1]
        else:
            filename = fd["name"]

        try:
            f = discord.File(
                fp=io.BytesIO(urllib.request.urlopen(url).read()),
                filename=filename
            )
        except Exception:
            pass
        else:
            yield {
                **data,
                "file": f
            }

        # The original URL failed - trying thumbnails
        if i < 1:
            data["content"] += ATTACHMENT_ERROR_APPEND.format(**fd)

    __log__.error("Failed to upload file for message '%s'", data["content"])

    # Just post the message without the attachment
    yield data


class SlackImportClient(discord.Client):

    def __init__(self, *args, data_dir, guild_name, all_private, real_names, start, end, **kwargs):
        self._data_dir = data_dir
        self._guild_name = guild_name
        self._prev_msg = None
        self._all_private = all_private
        self._start, self._end = [datetime.strptime(x, DATE_FORMAT).date() if x else None for x in (start, end)]

        self._users = slack_usermap(data_dir, real_names=real_names)

        self._exception = None

        super().__init__(
            *args,
            intents=discord.Intents(guilds=True, emojis_and_stickers=True),
            **kwargs
        )

    async def on_ready(self):
        __log__.info("The bot has logged in!")
        try:
            g = discord.utils.get(self.guilds, name=self._guild_name)
            if g is None:
                raise Exception(
                    "Guild '{}' not accessible to the bot. Available guild(s): {}".format(
                        self._guild_name,
                        ", ".join("'{}'".format(g.name) for g in self.guilds)
                    )
                )

            await self._run_import(g)
        except Exception as e:
            __log__.critical("Failed to finish import!", exc_info=True)
            self._exception = e
        finally:
            __log__.info("Bot logging out")
            await self.close()

    async def _handle_date_sep(self, target, msg):
        if DATE_SEPARATOR:
            msg_date = msg["date"]
            if (
                not self._prev_msg or
                self._prev_msg["date"] != msg_date
            ):
                await target.send(content=DATE_SEPARATOR.format(msg_date))
            self._prev_msg = msg

    async def _send_slack_msg(self, send, msg):
        sent = None
        pin = msg["events"].pop("pin", False)
        for data in make_discord_msgs(msg):
            for attempt in file_upload_attempts(data):
                with contextlib.suppress(Exception):
                    sent = await send(
                        username=msg["userinfo"][0],
                        avatar_url=msg["userinfo"][1],
                        **attempt
                    )
                    if pin:
                        pin = False
                        # Requires the "manage messages" optional permission
                        with contextlib.suppress(Forbidden):
                            await sent.pin()
                    break
            else:
                __log__.error("Failed to post message: '%s'", data["content"])

        return sent

    async def _run_import(self, g):
        emoji_map = {x.name: str(x) for x in self.emojis}

        __log__.info("Starting to import messages")
        c_chan, c_msg, start_time = 0, 0, datetime.now()

        existing_channels = {x.name: x for x in g.text_channels}

        for webhook in await g.webhooks():
            if webhook.user == self.user and webhook.name == "s2d-importer":
                __log__.info("Cleaning up previous webhook %s", webhook)
                await webhook.delete()

        for chan_name, init_topic, pins, is_private in slack_channels(self._data_dir):
            ch = None
            ch_webhook, ch_send = None, None
            c_msg_start = c_msg

            self._prev_msg = None  # always start with the date in a new channel

            init_topic = emoji_replace(init_topic, emoji_map)

            __log__.info("Processing channel '#%s'...", chan_name)

            for msg in slack_channel_messages(self._data_dir, chan_name, self._users, emoji_map, pins):
                # skip messages that are too early, stop when messages are too late
                if self._end and msg["datetime"].date() > self._end:
                    break
                elif self._start and  msg["datetime"].date() < self._start:
                    continue

                # Now that we have a message to send, get/create the channel to send it to
                if ch is None:
                    if chan_name not in existing_channels:
                        if self._all_private or is_private:
                            __log__.info("Creating '#%s' as a private channel", chan_name)
                            overwrites = {
                                g.default_role: discord.PermissionOverwrite(read_messages=False),
                                g.me: discord.PermissionOverwrite(read_messages=True),
                            }
                            ch = await g.create_text_channel(chan_name, topic=init_topic, overwrites=overwrites)
                        else:
                            __log__.info("Creating '#%s' as a public channel", chan_name)
                            ch = await g.create_text_channel(chan_name, topic=init_topic)
                    else:
                        ch = existing_channels[chan_name]
                    c_chan += 1

                    ch_webhook = await ch.create_webhook(
                        name="s2d-importer",
                        reason="For importing messages into '#{}'".format(chan_name)
                    )
                    ch_send = functools.partial(ch_webhook.send, wait=True)

                topic = msg["events"].get("topic", None)
                if topic is not None and topic != ch.topic:
                    # Note that the ratelimit is pretty extreme for this
                    # (2 edits per 10 minutes) so it may take a while if there
                    # a lot of topic changes
                    await ch.edit(topic=topic)

                # Send message and threaded replies
                await self._handle_date_sep(ch, msg)
                sent = await self._send_slack_msg(ch_send, msg)
                c_msg += 1
                if sent and msg["replies"]:
                    thread_name = (
                        textwrap.wrap(msg.get("text") or "", max_lines=1, width=MAX_THREADNAME_SIZE, placeholder="…") or
                        [BACKUP_THREAD_NAME.format(**msg).replace(":", "-")]  # ':' is not allowed in thread names
                    )[0]
                    thread = await sent.create_thread(name=thread_name)
                    thread_send = functools.partial(ch_send, thread=thread)
                    for rmsg in msg["replies"]:
                        await self._handle_date_sep(thread, rmsg)
                        await self._send_slack_msg(thread_send, rmsg)
                        c_msg += 1

                    # calculate next date separator based on the last message sent to the main channel
                    self._prev_msg = msg

            if ch_webhook:
                await ch_webhook.delete()

            __log__.info("Imported %s messages into '#%s'", c_msg - c_msg_start, chan_name)
        __log__.info(
            "Finished importing %d messages into %d channel(s) in %s",
            c_msg,
            c_chan,
            datetime.now()-start_time
        )


def run_import(*, zipfile, token, **kwargs):
    __log__.info("Extracting Slack export zip")
    with tempfile.TemporaryDirectory() as t:
        with ZipFile(zipfile, "r") as z:
            # Non-ASCII filenames in the zip seem to be encoded using UTF-8, but don't set the flag
            # that signals this. This means Python will use cp437 to decode them, resulting in
            # mangled filenames. Fix this by undoing the cp437 decode and using UTF-8 instead
            for zipinfo in z.infolist():
                if not zipinfo.flag_bits & (1 << 11):
                    # UTF-8 flag not set, cp437 was used to decode the filename
                    with contextlib.suppress(UnicodeEncodeError, UnicodeDecodeError):
                        zipinfo.filename = zipinfo.filename.encode("cp437").decode("utf-8")
                z.extract(zipinfo, path=t)

        __log__.info("Logging the bot into Discord")
        client = SlackImportClient(data_dir=t, **kwargs)
        client.run(token, reconnect=False, log_handler=None)
        if client._exception:
            raise client._exception


def main():
    parser = argparse.ArgumentParser(
        description="Import Slack chat history into Discord"
    )
    parser.add_argument("-z", "--zipfile", help="The Slack export zip file", required=True)
    parser.add_argument("-g", "--guild", help="The Discord Guild to import history into", required=True)
    parser.add_argument("-t", "--token", help="The Discord bot token", required=True)
    parser.add_argument("-s", "--start", help="The date to start importing from", required=False, default=None)
    parser.add_argument("-e", "--end", help="The date to end importing at", required=False, default=None)
    parser.add_argument("-p", "--all-private", help="Import all channels as private channels in Discord", action="store_true", default=False)
    parser.add_argument("-r", "--real-names", help="Use real names from Slack instead of usernames", action="store_true", default=False)
    parser.add_argument("-v", "--verbose", help="Show more verbose logs", action="store_true")
    args = parser.parse_args()

    discord.utils.setup_logging(level=logging.DEBUG if args.verbose else logging.INFO)

    run_import(
        zipfile=args.zipfile,
        token=args.token,
        guild_name=args.guild,
        all_private=args.all_private,
        real_names=args.real_names,
        start=args.start,
        end=args.end
    )

if __name__ == "__main__":
    main()
