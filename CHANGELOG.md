slack-to-discord changelog
==========================

### v1.1.7
 - Add more debug logging

### v1.1.6
 - Fix issue where Slack canvases would cause imports to fail
 - Add support for channel unarchiving messages

### v1.1.5
 - Require Python 3.8+

### v1.1.4
 - Fix issue where some files would fail to upload and show the "See
   original at ..." message instead.
 - Fix names of some non-image files having extra extensions appended.
   Ex: `script.sh` --> `script.sh.shell`

### v1.1.3
 - Properly handle the case where file data doesn't include the "mode" key

### v1.1.2
 - Add more emoji shortcode conversions (ex: ":spock_hand:" --> ":vulcan:")

### v1.1.1
 - Fix issue where custom emojis from Slack with `-`'s in their names
   could not be imported.

### v1.1.0
 - Added the CLI flag `--channels` to control which channels are
   imported
 - Fixed timeouts and reduced memory usage when downloading large files
   from Slack. The bot will now stream the data down from Slack as it's
   being uploaded to Discord instead of waiting for the entire file to
   be downloaded into RAM first.
 - The threads the bot creates will now be automatically be archived
   once the messages are imported into them. This fixes issues with
   hitting the active thread limit.

### v1.0.1
 - Fix an issue where exports with files that were missing names and
   titles would cause the import to fail.
 - Clean up any previous webhooks the bot created and failed to clean up
   before starting the import. This works around an issue where repeated
   failures to import would cause the bot to hit the maximum number of
   allowed webhooks.

### v1.0.0
 - Initial release
