set projectDir to "/Users/momu/douyin_compass_publish"
set pythonBin to "/usr/local/bin/python3"
set launchCommand to "cd " & quoted form of projectDir & " && " & quoted form of pythonBin & " main.py"

try
	do shell script "pgrep -f 'main.py' >/dev/null"
	tell application "Terminal" to activate
on error
	tell application "Terminal"
		activate
		do script launchCommand
	end tell
end try
