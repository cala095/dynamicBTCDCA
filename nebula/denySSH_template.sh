#!/bin/bash
# Get the IP address of the incoming connection
# Note: This method of getting the IP address might vary based on your setup
IP_ADDRESS=$(echo $SSH_CLIENT | awk '{ print $1 }')
#echo "ip identified: $IP_ADDRESS"
echo "SSH_ORIGINAL_COMMAND: '$SSH_ORIGINAL_COMMAND'" >> /tmp/debug_ssh.txt
if [[ ($IP_ADDRESS == 10.0.0.3 || $IP_ADDRESS == 10.182.0.4) && $SSH_ORIGINAL_COMMAND != '/usr/lib/openssh/sftp-server' ]]; then
    echo "SSH access is denied for this IP."
    exit 3
elif [[ $SSH_ORIGINAL_COMMAND == '/usr/lib/openssh/sftp-server' ]]; then
    # Normal command
    exec $SSH_ORIGINAL_COMMAND
else
    # If it's an interactive session or no specific command, start a shell
    /bin/bash
fi
