source ~/.bashrc
source ~/.bash_profile

# 最外层10次重试
for a in {1..10}
do
    # /opt/homebrew/bin/python3 -u -m slack_to_discord --zipfile <slack export zip> --guild <server name> --token <bot token>
    if [ $? -ne 0 ]; then
        echo "fail\n\n"
    else
        echo "success\n\n"
        break
    fi
done
